"""ETL pipeline for CPGF (Cartao de Pagamento do Governo Federal) data.

Ingests government credit card expense data from Portal da Transparencia.
Creates GovCardExpense nodes linked to Person (cardholder) via GASTOU_CARTAO.
"""

from __future__ import annotations

import hashlib
import logging
import zipfile
from datetime import date as _date_cls
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    parse_brl_amount,
    parse_date,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Portal da Transparencia "Cartão de Pagamento do Governo Federal"
# bulk endpoint. Each ``/download-de-dados/cpgf/<YYYYMM>`` request
# 302s to a monthly ``<YYYYMM>_CPGF.zip`` on the CGU dadosabertos
# bucket. Widget mode is ``MES``; the most recent month usually lags
# 1-2 months behind the calendar.
_CPGF_BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/cpgf"
_CPGF_USER_AGENT = "br-acc/bracc-etl download_cpgf (httpx)"
_CPGF_HTTP_TIMEOUT = 300.0
_CPGF_DEFAULT_WALKBACK_MONTHS = 6


def _yyyymm(d: _date_cls) -> str:
    return f"{d.year:04d}{d.month:02d}"


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    """Return (year, month) shifted by ``delta`` calendar months."""
    idx = (year * 12 + (month - 1)) + delta
    return (idx // 12, idx % 12 + 1)


def _expand_month_range(start: str, end: str) -> list[str]:
    """Inclusive YYYYMM range expansion (start <= end)."""
    sy, sm = int(start[:4]), int(start[4:])
    ey, em = int(end[:4]), int(end[4:])
    if (sy, sm) > (ey, em):
        sy, sm, ey, em = ey, em, sy, sm
    out: list[str] = []
    cy, cm = sy, sm
    while (cy, cm) <= (ey, em):
        out.append(f"{cy:04d}{cm:02d}")
        cy, cm = _shift_month(cy, cm, 1)
    return out


def _default_cpgf_months(walkback: int = _CPGF_DEFAULT_WALKBACK_MONTHS) -> list[str]:
    """Walk back from current UTC month for ``walkback`` months."""
    today = _date_cls.today()
    months: list[str] = []
    cy, cm = today.year, today.month
    for _ in range(walkback):
        months.append(f"{cy:04d}{cm:02d}")
        cy, cm = _shift_month(cy, cm, -1)
    return months


def fetch_to_disk(
    output_dir: Path | str,
    *,
    months: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    walkback: int = _CPGF_DEFAULT_WALKBACK_MONTHS,
    skip_existing: bool = True,
    timeout: float = _CPGF_HTTP_TIMEOUT,
) -> list[Path]:
    """Download Portal da Transparencia ``cpgf`` monthly ZIPs to disk.

    Resolution order for which YYYYMM tags to fetch:

    1. ``months`` if provided (explicit list).
    2. ``start``/``end`` if both provided (inclusive range).
    3. Walk back ``walkback`` months from today; stop at the first
       month that returns a usable ZIP. (Default behavior, since the
       Portal lags 1-2 months and unpublished months 404 / return HTML
       error pages.)

    Each ZIP contains a single ``<YYYYMM>_CPGF.csv`` which we extract
    verbatim into ``output_dir``. :class:`CpgfPipeline` globs ``*.csv``
    under ``data/cpgf/`` and reads the upstream ``;``-delim latin-1
    layout directly via its built-in column normalizer.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    explicit_request = bool(months or (start and end))
    if months:
        targets = sorted({m for m in months})
    elif start and end:
        targets = _expand_month_range(start, end)
    else:
        targets = _default_cpgf_months(walkback)

    written: list[Path] = []
    headers = {"User-Agent": _CPGF_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=False,
    ) as client:
        for tag in targets:
            existing = sorted(output_dir.glob(f"{tag}_*CPGF*.csv"))
            if skip_existing and existing:
                logger.info(
                    "[cpgf] skipping %s (found %d CSV(s))",
                    tag, len(existing),
                )
                written.extend(existing)
                if not explicit_request:
                    return sorted(set(written))
                continue

            url = f"{_CPGF_BASE_URL}/{tag}"
            zip_path = raw_dir / f"cpgf_{tag}.zip"

            if not (skip_existing and zip_path.exists() and zip_path.stat().st_size > 0):
                logger.info("[cpgf] downloading %s -> %s", url, zip_path.name)
                try:
                    with client.stream("GET", url) as resp:
                        resp.raise_for_status()
                        ctype = resp.headers.get("content-type", "")
                        if "zip" not in ctype and "octet-stream" not in ctype:
                            logger.warning(
                                "[cpgf] %s returned content-type %s "
                                "(month not yet published?)",
                                tag, ctype,
                            )
                            continue
                        with zip_path.open("wb") as fh:
                            for chunk in resp.iter_bytes(chunk_size=1 << 16):
                                if chunk:
                                    fh.write(chunk)
                except httpx.HTTPError as exc:
                    logger.warning(
                        "[cpgf] download failed for %s: %s", tag, exc,
                    )
                    continue
            else:
                logger.info("[cpgf] reusing cached zip %s", zip_path.name)

            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    extracted_for_tag: list[Path] = []
                    for info in zf.infolist():
                        name = Path(info.filename).name
                        if not name.lower().endswith(".csv"):
                            continue
                        dst_path = output_dir / name
                        with zf.open(info) as src, dst_path.open("wb") as dst:
                            while True:
                                block = src.read(1 << 20)
                                if not block:
                                    break
                                dst.write(block)
                        logger.info(
                            "[cpgf] extracted %s (%.2f MB)",
                            dst_path.name,
                            dst_path.stat().st_size / 1024 / 1024,
                        )
                        extracted_for_tag.append(dst_path)
                        written.append(dst_path)
            except zipfile.BadZipFile:
                logger.warning(
                    "[cpgf] bad zip %s -- deleting", zip_path.name,
                )
                zip_path.unlink(missing_ok=True)
                continue

            if not explicit_request and extracted_for_tag:
                # Default mode: stop at the first published month.
                return sorted(set(written))

    return sorted(set(written))

# Portal da Transparencia CPGF columns may have accented or unaccented names.
# Normalize to unaccented forms for reliable field access.
_COLUMN_ALIASES: dict[str, str] = {
    "CÓDIGO ÓRGÃO SUPERIOR": "CODIGO ORGAO SUPERIOR",
    "NOME ÓRGÃO SUPERIOR": "NOME ORGAO SUPERIOR",
    "CÓDIGO ÓRGÃO": "CODIGO ORGAO",
    "NOME ÓRGÃO": "NOME ORGAO",
    "CÓDIGO UNIDADE GESTORA": "CODIGO UNIDADE GESTORA",
    "MÊS EXTRATO": "MES EXTRATO",
    "TRANSAÇÃO": "TRANSACAO",
    "DATA TRANSAÇÃO": "DATA TRANSACAO",
    "VALOR TRANSAÇÃO": "VALOR TRANSACAO",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize accented column names to unaccented equivalents."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        stripped = col.strip()
        if stripped in _COLUMN_ALIASES:
            rename_map[col] = _COLUMN_ALIASES[stripped]
        elif stripped != col:
            rename_map[col] = stripped
    return df.rename(columns=rename_map) if rename_map else df


def _make_expense_id(cpf: str, date: str, amount: str, description: str) -> str:
    """Generate a stable expense ID from key fields."""
    raw = f"cpgf_{cpf}_{date}_{amount}_{description}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class CpgfPipeline(Pipeline):
    """ETL pipeline for CPGF (government credit card expenses)."""

    name = "cpgf"
    source_id = "cpgf"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.expenses: list[dict[str, Any]] = []
        self.cardholders: list[dict[str, Any]] = []
        self.gastou_cartao_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        cpgf_dir = Path(self.data_dir) / "cpgf"
        csv_files = sorted(cpgf_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", cpgf_dir)
            return

        frames: list[pd.DataFrame] = []
        for f in csv_files:
            df = pd.read_csv(
                f,
                sep=";",
                dtype=str,
                encoding="latin-1",
                keep_default_na=False,
            )
            frames.append(df)
            logger.info("  Loaded %d rows from %s", len(df), f.name)

        self._raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._raw = _normalize_columns(self._raw)
        logger.info("Total raw rows: %d", len(self._raw))

    def transform(self) -> None:
        if self._raw.empty:
            return

        expenses: list[dict[str, Any]] = []
        cardholders_map: dict[str, dict[str, Any]] = {}
        gastou_cartao: list[dict[str, Any]] = []
        skipped = 0

        for _, row in self._raw.iterrows():
            cpf_raw = str(row.get("CPF PORTADOR", "")).strip()
            digits = strip_document(cpf_raw)

            cardholder_name = normalize_name(
                str(row.get("NOME PORTADOR", ""))
            )
            if not cardholder_name:
                skipped += 1
                continue

            # Use full CPF when available, otherwise keep masked format
            cpf_formatted = format_cpf(cpf_raw) if len(digits) == 11 else cpf_raw

            amount = parse_brl_amount(row.get("VALOR TRANSACAO", ""))
            if amount == 0.0:
                skipped += 1
                continue

            agency = str(row.get("NOME ORGAO SUPERIOR", "")).strip()
            date = parse_date(str(row.get("DATA TRANSACAO", "")))
            description = str(row.get("NOME FAVORECIDO", "")).strip()
            transaction_type = str(row.get("TRANSACAO", "")).strip()

            expense_id = _make_expense_id(
                cpf_formatted, date, str(amount), description
            )

            expenses.append({
                "expense_id": expense_id,
                "cardholder_name": cardholder_name,
                "cardholder_cpf": cpf_formatted,
                "agency": agency,
                "amount": amount,
                "date": date,
                "description": description,
                "transaction_type": transaction_type,
                "source": "cpgf",
            })

            # Only link to Person nodes when we have a full CPF
            if len(digits) == 11:
                cardholders_map[cpf_formatted] = {
                    "cpf": cpf_formatted,
                    "name": cardholder_name,
                }

                gastou_cartao.append({
                    "source_key": cpf_formatted,
                    "target_key": expense_id,
                })

            if self.limit and len(expenses) >= self.limit:
                break

        self.expenses = deduplicate_rows(expenses, ["expense_id"])
        self.cardholders = list(cardholders_map.values())
        self.gastou_cartao_rels = gastou_cartao

        logger.info(
            "Transformed: %d expenses, %d cardholders (skipped %d)",
            len(self.expenses),
            len(self.cardholders),
            skipped,
        )

    def load(self) -> None:
        if not self.expenses:
            logger.warning("No expenses to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # Load GovCardExpense nodes
        count = loader.load_nodes(
            "GovCardExpense", self.expenses, key_field="expense_id"
        )
        logger.info("Loaded %d GovCardExpense nodes", count)

        # Merge Person nodes for cardholders
        if self.cardholders:
            count = loader.load_nodes("Person", self.cardholders, key_field="cpf")
            logger.info("Merged %d cardholder Person nodes", count)

        # GASTOU_CARTAO: Person -> GovCardExpense
        if self.gastou_cartao_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (e:GovCardExpense {expense_id: row.target_key}) "
                "MERGE (p)-[:GASTOU_CARTAO]->(e)"
            )
            count = loader.run_query_with_retry(query, self.gastou_cartao_rels)
            logger.info("Created %d GASTOU_CARTAO relationships", count)

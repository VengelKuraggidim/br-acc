from __future__ import annotations

import hashlib
import logging
import zipfile
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

_VIAGENS_BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/viagens"
_VIAGENS_USER_AGENT = "br-acc/bracc-etl download_viagens (httpx)"
_VIAGENS_HTTP_TIMEOUT = 600.0

# Portal da Transparencia Viagens CSVs use semicolon separators and
# mixed-case Portuguese column headers. Map to canonical names.
_COLUMN_ALIASES: dict[str, str] = {
    # Actual Portal da Transparencia *_Viagem.csv headers (with accents)
    "CÓDIGO DO ÓRGÃO SUPERIOR": "cod_orgao_superior",
    "NOME DO ÓRGÃO SUPERIOR": "nome_orgao_superior",
    "CÓDIGO ÓRGÃO SOLICITANTE": "cod_orgao",
    "NOME ÓRGÃO SOLICITANTE": "nome_orgao",
    "CPF VIAJANTE": "cpf",
    "NOME": "nome",
    "CARGO": "cargo",
    "FUNÇÃO": "funcao",
    "DESCRIÇÃO FUNÇÃO": "descricao_funcao",
    "PERÍODO - DATA DE INÍCIO": "data_inicio",
    "PERÍODO - DATA DE FIM": "data_fim",
    "DESTINOS": "destinos",
    "MOTIVO": "motivo",
    "VALOR DIÁRIAS": "valor_diarias",
    "VALOR PASSAGENS": "valor_passagens",
    "VALOR DEVOLUÇÃO": "valor_devolucao",
    "VALOR OUTROS GASTOS": "valor_outros",
    # Variants without accents
    "CODIGO DO ORGAO SUPERIOR": "cod_orgao_superior",
    "NOME DO ORGAO SUPERIOR": "nome_orgao_superior",
    "CODIGO ORGAO SOLICITANTE": "cod_orgao",
    "NOME ORGAO SOLICITANTE": "nome_orgao",
    "FUNCAO": "funcao",
    "DESCRICAO FUNCAO": "descricao_funcao",
    "PERIODO - DATA DE INICIO": "data_inicio",
    "PERIODO - DATA DE FIM": "data_fim",
    "VALOR DEVOLUCAO": "valor_devolucao",
    # Legacy aliases (download script pre-mapped names, kept for compatibility)
    "CÓDIGO ÓRGÃO": "cod_orgao",
    "NOME ÓRGÃO": "nome_orgao",
    "CPF SERVIDOR": "cpf",
    "NOME SERVIDOR": "nome",
    "CODIGO ORGAO": "cod_orgao",
    "NOME ORGAO": "nome_orgao",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names using alias lookup (case-insensitive)."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        col_upper = col.strip().upper()
        if col_upper in _COLUMN_ALIASES:
            rename_map[col] = _COLUMN_ALIASES[col_upper]
        elif col.strip() in _COLUMN_ALIASES.values():
            rename_map[col] = col.strip()
    return df.rename(columns=rename_map)


def _parse_money(value: str) -> float:
    """Parse Brazilian money format (1.234,56) to float."""
    if not value or not value.strip():
        return 0.0
    cleaned = value.strip().replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _make_travel_id(cpf: str, destination: str, start_date: str, amount: float) -> str:
    """Generate deterministic travel_id from key fields."""
    raw = f"{cpf}|{destination}|{start_date}|{amount:.2f}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _default_viagens_years() -> list[int]:
    """Return the last 3 completed calendar years (inclusive of current)."""
    current = date.today().year
    return list(range(current - 2, current + 1))


def fetch_to_disk(
    output_dir: Path | str,
    years: list[int] | None = None,
    *,
    skip_existing: bool = True,
    timeout: float = _VIAGENS_HTTP_TIMEOUT,
) -> list[Path]:
    """Download Portal da Transparencia ``viagens`` yearly ZIPs to disk.

    The upstream bulk endpoint ``/download-de-dados/viagens/<YYYY>``
    302s to a yearly ZIP on ``dadosabertos-download.cgu.gov.br``
    containing four CSVs (Viagem, Passagem, Pagamento, Trecho);
    ``ViagensPipeline.extract`` only consumes the ``*_Viagem.csv``
    grain, so this wrapper extracts only that file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    requested_years = sorted({int(y) for y in (years or _default_viagens_years())})
    written: list[Path] = []

    headers = {"User-Agent": _VIAGENS_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=False,
    ) as client:
        for year in requested_years:
            csv_path = output_dir / f"{year}_Viagem.csv"
            if skip_existing and csv_path.exists() and csv_path.stat().st_size > 0:
                logger.info("[viagens] skipping existing %s", csv_path.name)
                written.append(csv_path)
                continue

            url = f"{_VIAGENS_BASE_URL}/{year}"
            zip_path = raw_dir / f"viagens_{year}.zip"

            if not (skip_existing and zip_path.exists() and zip_path.stat().st_size > 0):
                logger.info("[viagens] downloading %s -> %s", url, zip_path.name)
                try:
                    with client.stream("GET", url) as resp:
                        resp.raise_for_status()
                        total = resp.headers.get("content-length")
                        logger.info(
                            "[viagens] %s size: %s bytes",
                            zip_path.name, total or "unknown",
                        )
                        with zip_path.open("wb") as fh:
                            for chunk in resp.iter_bytes(chunk_size=1 << 16):
                                if chunk:
                                    fh.write(chunk)
                except httpx.HTTPError as exc:
                    logger.warning(
                        "[viagens] download failed for %s: %s", year, exc,
                    )
                    continue
            else:
                logger.info("[viagens] reusing cached zip %s", zip_path.name)

            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    viagem_member = next(
                        (
                            n for n in zf.namelist()
                            if n.lower().endswith(".csv")
                            and "viagem" in n.lower()
                            and "passagem" not in n.lower()
                            and "pagamento" not in n.lower()
                            and "trecho" not in n.lower()
                        ),
                        None,
                    )
                    if viagem_member is None:
                        logger.warning(
                            "[viagens] no *_Viagem.csv found in %s",
                            zip_path.name,
                        )
                        continue
                    with zf.open(viagem_member) as src, csv_path.open("wb") as dst:
                        while True:
                            block = src.read(1 << 20)
                            if not block:
                                break
                            dst.write(block)
            except zipfile.BadZipFile:
                logger.warning(
                    "[viagens] bad zip %s -- deleting for re-download",
                    zip_path.name,
                )
                zip_path.unlink(missing_ok=True)
                continue

            logger.info(
                "[viagens] extracted %s (%.2f MB)",
                csv_path.name, csv_path.stat().st_size / 1024 / 1024,
            )
            written.append(csv_path)

    return sorted(written)


class ViagensPipeline(Pipeline):
    """ETL pipeline for Government Travel (Viagens a Servico) data."""

    name = "viagens"
    source_id = "portal_transparencia_viagens"

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
        self.travels: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        viagens_dir = Path(self.data_dir) / "viagens"
        csv_files = sorted(viagens_dir.glob("*.csv"))
        if not csv_files:
            msg = f"No CSV files found in {viagens_dir}"
            raise FileNotFoundError(msg)

        frames: list[pd.DataFrame] = []
        for csv_path in csv_files:
            try:
                df = pd.read_csv(
                    csv_path,
                    dtype=str,
                    delimiter=";",
                    encoding="latin-1",
                    keep_default_na=False,
                )
                df = _normalize_columns(df)
                frames.append(df)
                logger.info("[viagens] Read %d rows from %s", len(df), csv_path.name)
            except Exception:
                logger.warning("[viagens] Failed to read %s", csv_path.name)

        if frames:
            self._raw = pd.concat(frames, ignore_index=True)
        logger.info("[viagens] Extracted %d total rows", len(self._raw))

    def transform(self) -> None:
        travels: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            cpf_raw = str(row.get("cpf", "")).strip()
            digits = strip_document(cpf_raw)

            nome = normalize_name(str(row.get("nome", "")))
            if not nome:
                continue

            # Use full CPF when available, otherwise keep masked format
            cpf_formatted = format_cpf(cpf_raw) if len(digits) == 11 else cpf_raw

            agency = str(row.get("nome_orgao", "")).strip()
            destination = str(row.get("destinos", "")).strip()
            start_date = parse_date(str(row.get("data_inicio", "")))
            end_date = parse_date(str(row.get("data_fim", "")))
            justification = str(row.get("motivo", "")).strip()

            valor_diarias = _parse_money(str(row.get("valor_diarias", "")))
            valor_passagens = _parse_money(str(row.get("valor_passagens", "")))
            valor_outros = _parse_money(str(row.get("valor_outros", "")))
            amount = round(valor_diarias + valor_passagens + valor_outros, 2)

            travel_id = _make_travel_id(cpf_formatted, destination, start_date, amount)

            travels.append({
                "travel_id": travel_id,
                "traveler_name": nome,
                "traveler_cpf": cpf_formatted,
                "agency": agency,
                "destination": destination,
                "start_date": start_date,
                "end_date": end_date,
                "amount": amount,
                "justification": justification,
                "source": "portal_transparencia_viagens",
            })

            # Only link to Person nodes when we have a full CPF
            if len(digits) == 11:
                person_rels.append({
                    "source_key": cpf_formatted,
                    "target_key": travel_id,
                    "person_name": nome,
                })

            if self.limit and len(travels) >= self.limit:
                break

        self.travels = deduplicate_rows(travels, ["travel_id"])
        self.person_rels = person_rels
        logger.info(
            "[viagens] Transformed %d travel records, %d person links",
            len(self.travels),
            len(self.person_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.travels:
            loaded = loader.load_nodes("GovTravel", self.travels, key_field="travel_id")
            logger.info("[viagens] Loaded %d GovTravel nodes", loaded)

        if self.person_rels:
            query = (
                "UNWIND $rows AS row "
                "MERGE (p:Person {cpf: row.source_key}) "
                "ON CREATE SET p.name = row.person_name "
                "WITH p, row "
                "MATCH (t:GovTravel {travel_id: row.target_key}) "
                "MERGE (p)-[:VIAJOU]->(t)"
            )
            loaded = loader.run_query_with_retry(query, self.person_rels)
            logger.info("[viagens] Loaded %d VIAJOU relationships", loaded)

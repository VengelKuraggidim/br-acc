from __future__ import annotations

import logging
import re
import unicodedata
import zipfile
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
    format_cnpj,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# Portal da Transparencia "Acordos de Leniência" widget publishes a
# single consolidated ZIP per day (mode "DIA"). The landing page embeds
# the current snapshot date in an inline ``arquivos.push({...})`` block;
# the download URL is
# ``/download-de-dados/acordos-leniencia/<YYYYMMDD>`` which 302s to a
# dated ZIP on ``dadosabertos-download.cgu.gov.br``. Guessing a date
# that is not the officially-published snapshot returns 403 from S3.
_LENIENCY_LANDING_URL = (
    "https://portaldatransparencia.gov.br/download-de-dados/acordos-leniencia"
)
_LENIENCY_DOWNLOAD_BASE = (
    "https://portaldatransparencia.gov.br/download-de-dados/acordos-leniencia"
)
_LENIENCY_USER_AGENT = "br-acc/bracc-etl download_leniency (httpx)"
_LENIENCY_HTTP_TIMEOUT = 120.0

# ZIP CSV columns -> pipeline-expected lowercase ASCII keys the
# LeniencyPipeline.transform() method looks up. The upstream header uses
# Windows-1252 en-dashes (``\x96``) between tokens (read back as latin-1),
# so we normalize each column to ASCII-uppercase with single-space
# separators before matching.
_LENIENCY_COL_RENAME: dict[str, str] = {
    "CNPJ DO SANCIONADO": "cnpj",
    "RAZAO SOCIAL CADASTRO RECEITA": "razao_social",
    "DATA DE INICIO DO ACORDO": "data_inicio",
    "DATA DE FIM DO ACORDO": "data_fim",
    "SITUACAO DO ACORDO DE LENIENICA": "situacao",
    "SITUACAO DO ACORDO DE LENIENCIA": "situacao",
    "ORGAO SANCIONADOR": "orgao_responsavel",
    "NUMERO DO PROCESSO": "numero_processo",
    "ID DO ACORDO": "id_acordo",
}

_LENIENCY_PUSH_RE = re.compile(
    r'arquivos\.push\(\s*\{\s*"ano"\s*:\s*"(\d{4})"\s*,\s*'
    r'"mes"\s*:\s*"(\d{2})"\s*,\s*"dia"\s*:\s*"(\d{2})"',
)


def _normalize_leniency_col(name: str) -> str:
    """Collapse accents/punctuation so column headers match lookups."""
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(
        c if c.isalnum() or c.isspace() else " "
        for c in decomposed
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(ascii_only.upper().split())


def _discover_snapshot_date(
    client: httpx.Client,
    landing_url: str = _LENIENCY_LANDING_URL,
) -> str | None:
    """Scrape the YYYYMMDD snapshot date off the leniency landing page."""
    try:
        resp = client.get(landing_url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[leniency] cannot fetch landing page: %s", exc)
        return None
    dates = [
        f"{y}{m}{d}" for (y, m, d) in _LENIENCY_PUSH_RE.findall(resp.text)
    ]
    if not dates:
        logger.warning(
            "[leniency] no arquivos.push entries on landing page",
        )
        return None
    return max(dates)


def fetch_to_disk(
    output_dir: Path | str,
    snapshot: str | None = None,
    *,
    skip_existing: bool = True,
    timeout: float = _LENIENCY_HTTP_TIMEOUT,
) -> list[Path]:
    """Download the CGU "Acordos de Leniência" snapshot CSV to disk.

    Scrapes the current snapshot date off the landing page (unless
    ``snapshot`` YYYYMMDD is passed), downloads the dated ZIP, extracts
    the main ``*_Acordos.csv``, remaps its columns from the accented
    Portuguese headers to the lowercase ASCII schema
    ``LeniencyPipeline.extract`` expects, and writes
    ``leniencia.csv`` into ``output_dir``.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    out_csv = output_dir / "leniencia.csv"
    if skip_existing and out_csv.exists() and out_csv.stat().st_size > 0:
        logger.info("[leniency] skipping existing %s", out_csv.name)
        return [out_csv]

    headers = {"User-Agent": _LENIENCY_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=False,
    ) as client:
        date_tag = snapshot or _discover_snapshot_date(client)
        if not date_tag:
            logger.error(
                "[leniency] could not determine snapshot date; aborting",
            )
            return []

        url = f"{_LENIENCY_DOWNLOAD_BASE}/{date_tag}"
        zip_path = raw_dir / f"acordos_leniencia_{date_tag}.zip"

        if not (skip_existing and zip_path.exists() and zip_path.stat().st_size > 0):
            logger.info("[leniency] downloading %s -> %s", url, zip_path.name)
            try:
                with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    with zip_path.open("wb") as fh:
                        for chunk in resp.iter_bytes(chunk_size=1 << 16):
                            if chunk:
                                fh.write(chunk)
            except httpx.HTTPError as exc:
                logger.warning(
                    "[leniency] download failed (%s): %s", url, exc,
                )
                return []
        else:
            logger.info("[leniency] reusing cached zip %s", zip_path.name)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                acordos_member = next(
                    (
                        n for n in zf.namelist()
                        if n.lower().endswith(".csv")
                        and "acordo" in n.lower()
                        and "efeito" not in n.lower()
                    ),
                    None,
                )
                if acordos_member is None:
                    logger.warning(
                        "[leniency] no *_Acordos.csv in %s", zip_path.name,
                    )
                    return []
                raw_csv_path = raw_dir / Path(acordos_member).name
                with zf.open(acordos_member) as src, raw_csv_path.open("wb") as dst:
                    while True:
                        block = src.read(1 << 20)
                        if not block:
                            break
                        dst.write(block)
        except zipfile.BadZipFile:
            logger.warning(
                "[leniency] bad zip %s -- deleting", zip_path.name,
            )
            zip_path.unlink(missing_ok=True)
            return []

    df = pd.read_csv(
        raw_csv_path,
        dtype=str,
        sep=";",
        encoding="latin-1",
        keep_default_na=False,
    )
    rename_map = {
        col: _LENIENCY_COL_RENAME[_normalize_leniency_col(col)]
        for col in df.columns
        if _normalize_leniency_col(col) in _LENIENCY_COL_RENAME
    }
    df = df.rename(columns=rename_map)
    df.to_csv(out_csv, index=False, sep=",", encoding="latin-1")
    logger.info(
        "[leniency] wrote %d rows to %s", len(df), out_csv,
    )
    return [out_csv, raw_csv_path]


class LeniencyPipeline(Pipeline):
    """ETL pipeline for Acordos de Leniencia (CGU leniency agreements)."""

    name = "leniency"
    source_id = "cgu_leniencia"

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
        self.agreements: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        leniency_dir = Path(self.data_dir) / "leniency"
        self._raw = pd.read_csv(
            leniency_dir / "leniencia.csv",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
        )

    def transform(self) -> None:
        agreements: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []

        for _idx, row in self._raw.iterrows():
            cnpj_raw = str(row.get("cnpj", ""))
            digits = strip_document(cnpj_raw)

            if len(digits) != 14:
                continue

            cnpj_formatted = format_cnpj(cnpj_raw)
            nome = normalize_name(str(row.get("razao_social", "")))
            start_date = parse_date(str(row.get("data_inicio", "")))
            end_date_raw = parse_date(str(row.get("data_fim", "")))
            end_date = end_date_raw if end_date_raw else None
            status = str(row.get("situacao", "")).strip()
            responsible_agency = str(row.get("orgao_responsavel", "")).strip()
            proceedings_raw = str(row.get("qtd_processos", "")).strip()
            proceedings_count = int(proceedings_raw) if proceedings_raw.isdigit() else 0

            leniency_id = f"leniencia_{digits}"

            agreements.append({
                "leniency_id": leniency_id,
                "cnpj": cnpj_formatted,
                "name": nome,
                "start_date": start_date,
                "end_date": end_date,
                "status": status,
                "responsible_agency": responsible_agency,
                "proceedings_count": proceedings_count,
                "source": "cgu_leniencia",
            })

            company_rels.append({
                "source_key": cnpj_formatted,
                "target_key": leniency_id,
                "company_name": nome,
            })

        self.agreements = deduplicate_rows(agreements, ["leniency_id"])
        self.company_rels = company_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.agreements:
            loader.load_nodes(
                "LeniencyAgreement", self.agreements, key_field="leniency_id",
            )

        # Ensure Company nodes exist
        for rel in self.company_rels:
            loader.load_nodes(
                "Company",
                [{
                    "cnpj": rel["source_key"],
                    "name": rel["company_name"],
                    "razao_social": rel["company_name"],
                }],
                key_field="cnpj",
            )

        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (l:LeniencyAgreement {leniency_id: row.target_key}) "
                "MERGE (c)-[:FIRMOU_LENIENCIA]->(l)"
            )
            loader.run_query_with_retry(query, self.company_rels)

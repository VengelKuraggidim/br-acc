from __future__ import annotations

import hashlib
import logging
import re
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
    strip_document,
)

logger = logging.getLogger(__name__)

# Portal da Transparencia "Empresas Impedidas de Contratar" widget
# publishes a single consolidated ZIP per day (mode "DIA"). The landing
# page embeds the current snapshot date in an inline
# ``arquivos.push({...})`` block; the download URL is
# ``/download-de-dados/cepim/<YYYYMMDD>`` which 302s to a dated ZIP on
# ``dadosabertos-download.cgu.gov.br``. Guessing a date that is not the
# officially-published snapshot returns 403 from S3.
_CEPIM_LANDING_URL = "https://portaldatransparencia.gov.br/download-de-dados/cepim"
_CEPIM_DOWNLOAD_BASE = "https://portaldatransparencia.gov.br/download-de-dados/cepim"
_CEPIM_USER_AGENT = "br-acc/bracc-etl download_cepim (httpx)"
_CEPIM_HTTP_TIMEOUT = 120.0

_CEPIM_PUSH_RE = re.compile(
    r'arquivos\.push\(\s*\{\s*"ano"\s*:\s*"(\d{4})"\s*,\s*'
    r'"mes"\s*:\s*"(\d{2})"\s*,\s*"dia"\s*:\s*"(\d{2})"',
)


def _discover_snapshot_date(
    client: httpx.Client,
    landing_url: str = _CEPIM_LANDING_URL,
) -> str | None:
    """Scrape the YYYYMMDD snapshot date off the CEPIM landing page."""
    try:
        resp = client.get(landing_url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[cepim] cannot fetch landing page: %s", exc)
        return None
    dates = [
        f"{y}{m}{d}" for (y, m, d) in _CEPIM_PUSH_RE.findall(resp.text)
    ]
    if not dates:
        logger.warning("[cepim] no arquivos.push entries on landing page")
        return None
    return max(dates)


def fetch_to_disk(
    output_dir: Path | str,
    *,
    date: str | None = None,
    skip_existing: bool = True,
    timeout: float = _CEPIM_HTTP_TIMEOUT,
) -> list[Path]:
    """Download the CGU CEPIM snapshot CSV to disk.

    Scrapes the current snapshot date off the landing page (unless
    ``date`` YYYYMMDD is passed), downloads the dated ZIP, extracts the
    inner ``*_CEPIM.csv``, and writes it as ``cepim.csv`` into
    ``output_dir`` using the upstream ``;``-delimited latin-1 layout
    that :class:`CepimPipeline` already expects.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    out_csv = output_dir / "cepim.csv"
    if skip_existing and out_csv.exists() and out_csv.stat().st_size > 0:
        logger.info("[cepim] skipping existing %s", out_csv.name)
        return [out_csv]

    headers = {"User-Agent": _CEPIM_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=False,
    ) as client:
        date_tag = date or _discover_snapshot_date(client)
        if not date_tag:
            logger.error(
                "[cepim] could not determine snapshot date; aborting",
            )
            return []
        if date is not None and date != _discover_snapshot_date(client):
            logger.warning(
                "[cepim] requested date %s differs from current "
                "published snapshot; non-current dates usually 403",
                date,
            )

        url = f"{_CEPIM_DOWNLOAD_BASE}/{date_tag}"
        zip_path = raw_dir / f"cepim_{date_tag}.zip"

        if not (skip_existing and zip_path.exists() and zip_path.stat().st_size > 0):
            logger.info("[cepim] downloading %s -> %s", url, zip_path.name)
            try:
                with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    with zip_path.open("wb") as fh:
                        for chunk in resp.iter_bytes(chunk_size=1 << 16):
                            if chunk:
                                fh.write(chunk)
            except httpx.HTTPError as exc:
                logger.warning(
                    "[cepim] download failed (%s): %s", url, exc,
                )
                return []
        else:
            logger.info("[cepim] reusing cached zip %s", zip_path.name)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_member = next(
                    (n for n in zf.namelist() if n.lower().endswith(".csv")),
                    None,
                )
                if csv_member is None:
                    logger.warning(
                        "[cepim] no CSV in %s", zip_path.name,
                    )
                    return []
                with zf.open(csv_member) as src, out_csv.open("wb") as dst:
                    while True:
                        block = src.read(1 << 20)
                        if not block:
                            break
                        dst.write(block)
        except zipfile.BadZipFile:
            logger.warning(
                "[cepim] bad zip %s -- deleting", zip_path.name,
            )
            zip_path.unlink(missing_ok=True)
            return []

    logger.info(
        "[cepim] wrote %s (%.1f KB)",
        out_csv,
        out_csv.stat().st_size / 1024,
    )
    return [out_csv]


def _generate_ngo_id(cnpj_digits: str, agreement_number: str) -> str:
    """Deterministic ID from CNPJ digits + agreement number."""
    raw = f"{cnpj_digits}:{agreement_number}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class CepimPipeline(Pipeline):
    """ETL pipeline for CEPIM (Cadastro de Entidades Privadas sem Fins Lucrativos Impedidas)."""

    name = "cepim"
    source_id = "cepim"

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
        self.ngos: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        cepim_dir = Path(self.data_dir) / "cepim"
        self._raw = pd.read_csv(
            cepim_dir / "cepim.csv",
            sep=";",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
        )

    def transform(self) -> None:
        ngos: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []

        for _idx, row in self._raw.iterrows():
            cnpj_raw = str(row.get("CNPJ ENTIDADE", ""))
            digits = strip_document(cnpj_raw)

            if len(digits) != 14:
                continue

            cnpj_formatted = format_cnpj(cnpj_raw)
            name = normalize_name(str(row.get("NOME ENTIDADE", "")))
            agreement_number = str(row.get("NÚMERO CONVÊNIO", "")).strip()
            agency = str(row.get("ÓRGÃO CONCEDENTE", "")).strip()
            reason = str(
                row.get("MOTIVO IMPEDIMENTO", row.get("MOTIVO DO IMPEDIMENTO", ""))
            ).strip()

            ngo_id = _generate_ngo_id(digits, agreement_number)

            ngos.append({
                "ngo_id": ngo_id,
                "cnpj": cnpj_formatted,
                "name": name,
                "reason": reason,
                "agreement_number": agreement_number,
                "agency": agency,
                "source": "cepim",
            })

            company_rels.append({
                "source_key": cnpj_formatted,
                "target_key": ngo_id,
            })

        self.ngos = deduplicate_rows(ngos, ["ngo_id"])
        self.company_rels = company_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.ngos:
            loader.load_nodes("BarredNGO", self.ngos, key_field="ngo_id")

        # Ensure Company nodes exist for CNPJ linking
        if self.company_rels:
            companies = [
                {"cnpj": rel["source_key"]} for rel in self.company_rels
            ]
            loader.load_nodes("Company", deduplicate_rows(companies, ["cnpj"]), key_field="cnpj")

        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (b:BarredNGO {ngo_id: row.target_key}) "
                "MERGE (c)-[:IMPEDIDA]->(b)"
            )
            loader.run_query_with_retry(query, self.company_rels)

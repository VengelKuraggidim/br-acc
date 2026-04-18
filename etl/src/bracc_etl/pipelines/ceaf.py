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
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# Portal da Transparencia "Cadastro de Expulsões da Administração
# Federal" widget publishes a single consolidated ZIP per day (mode
# "DIA"). The landing page embeds the current snapshot date in an inline
# ``arquivos.push({...})`` block; the download URL is
# ``/download-de-dados/ceaf/<YYYYMMDD>`` which 302s to a dated ZIP on
# ``dadosabertos-download.cgu.gov.br``. Guessing a date that is not the
# officially-published snapshot returns 403 from S3.
_CEAF_LANDING_URL = "https://portaldatransparencia.gov.br/download-de-dados/ceaf"
_CEAF_DOWNLOAD_BASE = "https://portaldatransparencia.gov.br/download-de-dados/ceaf"
_CEAF_USER_AGENT = "br-acc/bracc-etl download_ceaf (httpx)"
_CEAF_HTTP_TIMEOUT = 120.0

# Upstream CSV ships ``;``-delimited latin-1 with accented uppercase
# headers; CeafPipeline.extract() reads ``,``-delimited latin-1 with the
# snake_case keys below. We remap on download so the on-disk
# ``ceaf.csv`` matches the pipeline contract exactly.
_CEAF_COL_RENAME: dict[str, str] = {
    "CPF OU CNPJ DO SANCIONADO": "cpf",
    "NOME DO SANCIONADO": "nome",
    "CARGO EFETIVO": "cargo_efetivo",
    "CATEGORIA DA SANCAO": "tipo_punicao",
    "DATA PUBLICACAO": "data_publicacao",
    "NUMERO DO DOCUMENTO": "portaria",
    "UF ORGAO SANCIONADOR": "uf",
}

_CEAF_PUSH_RE = re.compile(
    r'arquivos\.push\(\s*\{\s*"ano"\s*:\s*"(\d{4})"\s*,\s*'
    r'"mes"\s*:\s*"(\d{2})"\s*,\s*"dia"\s*:\s*"(\d{2})"',
)


def _normalize_ceaf_col(name: str) -> str:
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
    landing_url: str = _CEAF_LANDING_URL,
) -> str | None:
    """Scrape the YYYYMMDD snapshot date off the CEAF landing page."""
    try:
        resp = client.get(landing_url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[ceaf] cannot fetch landing page: %s", exc)
        return None
    dates = [
        f"{y}{m}{d}" for (y, m, d) in _CEAF_PUSH_RE.findall(resp.text)
    ]
    if not dates:
        logger.warning("[ceaf] no arquivos.push entries on landing page")
        return None
    return max(dates)


def fetch_to_disk(
    output_dir: Path | str,
    *,
    date: str | None = None,
    skip_existing: bool = True,
    timeout: float = _CEAF_HTTP_TIMEOUT,
) -> list[Path]:
    """Download the CGU CEAF snapshot CSV to disk.

    Scrapes the current snapshot date off the landing page (unless
    ``date`` YYYYMMDD is passed), downloads the dated ZIP, extracts the
    inner ``*_Expulsoes.csv``, remaps its accented uppercase columns to
    the snake_case keys :class:`CeafPipeline` expects, and writes
    ``ceaf.csv`` into ``output_dir`` as ``,``-delimited latin-1.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    out_csv = output_dir / "ceaf.csv"
    if skip_existing and out_csv.exists() and out_csv.stat().st_size > 0:
        logger.info("[ceaf] skipping existing %s", out_csv.name)
        return [out_csv]

    headers = {"User-Agent": _CEAF_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=False,
    ) as client:
        date_tag = date or _discover_snapshot_date(client)
        if not date_tag:
            logger.error(
                "[ceaf] could not determine snapshot date; aborting",
            )
            return []

        url = f"{_CEAF_DOWNLOAD_BASE}/{date_tag}"
        zip_path = raw_dir / f"ceaf_{date_tag}.zip"

        if not (skip_existing and zip_path.exists() and zip_path.stat().st_size > 0):
            logger.info("[ceaf] downloading %s -> %s", url, zip_path.name)
            try:
                with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    with zip_path.open("wb") as fh:
                        for chunk in resp.iter_bytes(chunk_size=1 << 16):
                            if chunk:
                                fh.write(chunk)
            except httpx.HTTPError as exc:
                logger.warning(
                    "[ceaf] download failed (%s): %s", url, exc,
                )
                return []
        else:
            logger.info("[ceaf] reusing cached zip %s", zip_path.name)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_member = next(
                    (n for n in zf.namelist() if n.lower().endswith(".csv")),
                    None,
                )
                if csv_member is None:
                    logger.warning(
                        "[ceaf] no CSV in %s", zip_path.name,
                    )
                    return []
                raw_csv_path = raw_dir / Path(csv_member).name
                with zf.open(csv_member) as src, raw_csv_path.open("wb") as dst:
                    while True:
                        block = src.read(1 << 20)
                        if not block:
                            break
                        dst.write(block)
        except zipfile.BadZipFile:
            logger.warning(
                "[ceaf] bad zip %s -- deleting", zip_path.name,
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
        col: _CEAF_COL_RENAME[_normalize_ceaf_col(col)]
        for col in df.columns
        if _normalize_ceaf_col(col) in _CEAF_COL_RENAME
    }
    missing = set(_CEAF_COL_RENAME.values()) - set(rename_map.values())
    if missing:
        logger.warning(
            "[ceaf] missing expected columns after remap: %s "
            "(headers seen: %s)",
            sorted(missing),
            sorted(df.columns),
        )
    df = df.rename(columns=rename_map)
    keep = [c for c in _CEAF_COL_RENAME.values() if c in df.columns]
    df = df[keep]
    df.to_csv(out_csv, index=False, sep=",", encoding="latin-1")
    logger.info("[ceaf] wrote %d rows to %s", len(df), out_csv)
    return [out_csv]


class CeafPipeline(Pipeline):
    """ETL pipeline for CEAF (Cadastro de Expulsoes da Administracao Federal)."""

    name = "ceaf"
    source_id = "ceaf"

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
        self.expulsions: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        ceaf_dir = Path(self.data_dir) / "ceaf"
        self._raw = pd.read_csv(
            ceaf_dir / "ceaf.csv",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
        )

    def transform(self) -> None:
        expulsions: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []

        for idx, row in self._raw.iterrows():
            cpf_raw = str(row.get("cpf", ""))
            digits = strip_document(cpf_raw)

            nome = normalize_name(str(row.get("nome", "")))
            if not nome:
                continue

            position = str(row.get("cargo_efetivo", "")).strip()
            punishment_type = str(row.get("tipo_punicao", "")).strip()
            date = parse_date(str(row.get("data_publicacao", "")))
            decree = str(row.get("portaria", "")).strip()
            uf = str(row.get("uf", "")).strip()

            # Use full CPF when available, otherwise use partial + index
            if len(digits) == 11:
                cpf_formatted = format_cpf(cpf_raw)
                expulsion_id = f"ceaf_{digits}_{idx}"
            else:
                cpf_formatted = cpf_raw.strip()  # Keep masked format
                expulsion_id = f"ceaf_{digits}_{idx}"

            expulsions.append({
                "expulsion_id": expulsion_id,
                "cpf": cpf_formatted,
                "name": nome,
                "position": position,
                "punishment_type": punishment_type,
                "date": date,
                "decree": decree,
                "uf": uf,
                "source": "ceaf",
            })

            # Only create person relationships for full CPFs
            if len(digits) == 11:
                person_rels.append({
                    "source_key": cpf_formatted,
                    "target_key": expulsion_id,
                    "person_name": nome,
                })

        self.expulsions = deduplicate_rows(expulsions, ["expulsion_id"])
        self.person_rels = person_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.expulsions:
            loader.load_nodes("Expulsion", self.expulsions, key_field="expulsion_id")

        # Ensure Person nodes exist
        for rel in self.person_rels:
            loader.load_nodes(
                "Person",
                [{"cpf": rel["source_key"], "name": rel["person_name"]}],
                key_field="cpf",
            )

        if self.person_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (e:Expulsion {expulsion_id: row.target_key}) "
                "MERGE (p)-[:EXPULSO]->(e)"
            )
            loader.run_query_with_retry(query, self.person_rels)

from __future__ import annotations

import io
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
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# ── Download / fetch_to_disk (for scripts/download_sanctions.py) ─────────
#
# The CGU "Sanctions" feed for ``SanctionsPipeline`` is two separate
# Portal da Transparência widgets — CEIS (inidôneas/suspensas) and CNEP
# (empresas punidas) — both in widget-mode "DIA". The landing page for
# each embeds a date in an inline ``arquivos.push({...})`` JS block; the
# download URL is ``/download-de-dados/<ceis|cnep>/<YYYYMMDD>`` which
# 302-redirects to a dated ZIP on ``dadosabertos-download.cgu.gov.br``.
# Historical dates other than the current published snapshot return 403.
#
# Each ZIP contains a single ``<YYYYMMDD>_<CEIS|CNEP>.csv`` with accented
# uppercase headers (``"CPF OU CNPJ DO SANCIONADO"`` etc., latin-1,
# semicolon-delimited). The pipeline expects ``cpf_cnpj, nome,
# data_inicio, data_fim, motivo`` (lowercase, comma-delimited, latin-1).
# fetch_to_disk does the column remap + dialect translation in-memory.
_SANCTIONS_LANDING_BASE = (
    "https://portaldatransparencia.gov.br/download-de-dados"
)
_SANCTIONS_USER_AGENT = "br-acc/bracc-etl download_sanctions (httpx)"
_SANCTIONS_HTTP_TIMEOUT = 180.0

_SANCTIONS_PUSH_RE = re.compile(
    r'arquivos\.push\(\s*\{\s*"ano"\s*:\s*"(\d{4})"\s*,\s*'
    r'"mes"\s*:\s*"(\d{2})"\s*,\s*"dia"\s*:\s*"(\d{2})"',
)

# Upstream CGU header (normalized to ASCII upper) -> pipeline column name.
# ``_normalize_sanctions_col`` strips accents / collapses whitespace before
# matching so we are robust to small formatting drifts in future snapshots.
_SANCTIONS_COL_RENAME: dict[str, str] = {
    "CPF OU CNPJ DO SANCIONADO": "cpf_cnpj",
    "CPF CNPJ DO SANCIONADO": "cpf_cnpj",
    "CPF/CNPJ DO SANCIONADO": "cpf_cnpj",
    "NOME DO SANCIONADO": "nome",
    "DATA INICIO SANCAO": "data_inicio",
    "DATA INICIO DA SANCAO": "data_inicio",
    "DATA DE INICIO DA SANCAO": "data_inicio",
    "DATA FINAL SANCAO": "data_fim",
    "DATA FINAL DA SANCAO": "data_fim",
    "DATA DE FIM DA SANCAO": "data_fim",
    "FUNDAMENTACAO LEGAL": "motivo",
    "MOTIVO": "motivo",
    "MOTIVO DA SANCAO": "motivo",
}

# Output columns (and order) the pipeline's pd.read_csv consumes.
_SANCTIONS_OUT_COLS: list[str] = [
    "cpf_cnpj", "nome", "data_inicio", "data_fim", "motivo",
]


def _normalize_sanctions_col(name: str) -> str:
    """Strip accents/punctuation and uppercase a CGU header."""
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(
        c if c.isalnum() or c.isspace() else " "
        for c in decomposed
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(ascii_only.upper().split())


def _discover_sanctions_date(
    client: httpx.Client, dataset: str,
) -> str | None:
    """Scrape the YYYYMMDD snapshot date for ``ceis`` or ``cnep``."""
    landing_url = f"{_SANCTIONS_LANDING_BASE}/{dataset}"
    try:
        resp = client.get(landing_url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "[sanctions] cannot fetch %s landing page: %s", dataset, exc,
        )
        return None
    dates = [
        f"{y}{m}{d}" for (y, m, d) in _SANCTIONS_PUSH_RE.findall(resp.text)
    ]
    if not dates:
        logger.warning(
            "[sanctions] no arquivos.push entries for %s", dataset,
        )
        return None
    return max(dates)


def _download_sanctions_dataset(
    client: httpx.Client, dataset: str, snapshot: str,
) -> pd.DataFrame | None:
    """Download a CGU sanctions ZIP and return the CSV as a remapped DataFrame."""
    url = f"{_SANCTIONS_LANDING_BASE}/{dataset}/{snapshot}"
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "[sanctions] download failed (%s, %s): %s", dataset, snapshot, exc,
        )
        return None

    zip_bytes = resp.content
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile:
        logger.warning(
            "[sanctions] bad zip from %s (%d bytes)", url, len(zip_bytes),
        )
        return None

    csv_member = next(
        (n for n in zf.namelist() if n.lower().endswith(".csv")),
        None,
    )
    if csv_member is None:
        logger.warning(
            "[sanctions] no CSV in %s zip (%s)", dataset, zf.namelist(),
        )
        return None

    with zf.open(csv_member) as fh:
        df = pd.read_csv(
            fh,
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
        )

    rename_map: dict[str, str] = {}
    for col in df.columns:
        canonical = _SANCTIONS_COL_RENAME.get(_normalize_sanctions_col(col))
        if canonical and canonical not in rename_map.values():
            rename_map[col] = canonical
    df = df.rename(columns=rename_map)

    # Project onto the columns the pipeline reads — defaulting any missing
    # ones to empty so downstream str(row[...]) is always safe.
    out = pd.DataFrame({
        col: df[col] if col in df.columns else "" for col in _SANCTIONS_OUT_COLS
    })
    return out


def fetch_to_disk(
    output_dir: Path | str,
    *,
    date: str | None = None,
    timeout: float = _SANCTIONS_HTTP_TIMEOUT,
) -> list[Path]:
    """Download CEIS + CNEP CSVs to ``output_dir``.

    Scrapes the latest snapshot date off each widget landing page (unless
    ``date`` is passed as ``YYYYMMDD``), downloads both ZIPs, remaps the
    accented uppercase CGU headers to the pipeline's snake_case schema,
    and writes ``ceis.csv`` and ``cnep.csv`` (latin-1, comma-separated)
    into ``output_dir``.

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    date:
        Optional ``YYYYMMDD`` snapshot date applied to *both* widgets. The
        CGU widget mode is "DIA" — only the currently-published snapshot
        date is reachable (other dates return 403). Default: auto-discover
        each widget's own latest date.
    timeout:
        Per-request HTTP timeout in seconds.

    Returns
    -------
    List of CSV paths written. Each successfully downloaded dataset
    contributes one file; partial failures are logged but not raised.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    headers = {"User-Agent": _SANCTIONS_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=False,
    ) as client:
        for dataset in ("ceis", "cnep"):
            snapshot = date or _discover_sanctions_date(client, dataset)
            if not snapshot:
                logger.error(
                    "[sanctions] could not determine %s snapshot date; skipping",
                    dataset,
                )
                continue

            logger.info(
                "[sanctions] downloading %s snapshot %s", dataset, snapshot,
            )
            df = _download_sanctions_dataset(client, dataset, snapshot)
            if df is None:
                continue

            out_csv = output_dir / f"{dataset}.csv"
            df.to_csv(
                out_csv, index=False, sep=",", encoding="latin-1",
            )
            size_mb = out_csv.stat().st_size / 1024 / 1024
            logger.info(
                "[sanctions] wrote %s (%d rows, %.2f MB)",
                out_csv, len(df), size_mb,
            )
            written.append(out_csv)

    return written


class SanctionsPipeline(Pipeline):
    """ETL pipeline for CEIS/CNEP sanctions data."""

    name = "sanctions"
    source_id = "ceis_cnep"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_ceis: pd.DataFrame = pd.DataFrame()
        self._raw_cnep: pd.DataFrame = pd.DataFrame()
        self.sanctions: list[dict[str, Any]] = []
        self.sanctioned_entities: list[dict[str, Any]] = []

    def extract(self) -> None:
        sanctions_dir = Path(self.data_dir) / "sanctions"
        if not sanctions_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, sanctions_dir)
            return
        ceis_path = sanctions_dir / "ceis.csv"
        cnep_path = sanctions_dir / "cnep.csv"
        if not ceis_path.exists() or not cnep_path.exists():
            logger.warning("[%s] Required CSV files not found in %s", self.name, sanctions_dir)
            return
        self._raw_ceis = pd.read_csv(
            ceis_path, dtype=str, encoding="latin-1", keep_default_na=False,
        )
        self._raw_cnep = pd.read_csv(
            cnep_path, dtype=str, encoding="latin-1", keep_default_na=False,
        )

    def _process_rows(
        self, df: pd.DataFrame, sanction_type: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        sanctions: list[dict[str, Any]] = []
        entities: list[dict[str, Any]] = []

        for idx, row in df.iterrows():
            doc_raw = str(row["cpf_cnpj"])
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["nome"]))
            is_company = len(digits) == 14

            if is_company:
                doc_formatted = format_cnpj(doc_raw)
            elif len(digits) == 11:
                doc_formatted = format_cpf(doc_raw)
            else:
                doc_formatted = digits

            sanction_id = f"{sanction_type}_{digits}_{idx}"
            date_start = parse_date(str(row["data_inicio"]))
            date_end_raw = parse_date(str(row["data_fim"]))
            date_end = date_end_raw if date_end_raw else None

            sanctions.append({
                "sanction_id": sanction_id,
                "type": sanction_type,
                "date_start": date_start,
                "date_end": date_end,
                "reason": str(row["motivo"]).strip(),
                "source": sanction_type,
            })

            entity_label = "Company" if is_company else "Person"
            entity_key_field = "cnpj" if is_company else "cpf"

            entities.append({
                "source_key": doc_formatted,
                "target_key": sanction_id,
                "entity_label": entity_label,
                "entity_key_field": entity_key_field,
                "entity_name": nome,
                "entity_doc": doc_formatted,
            })

        return sanctions, entities

    def transform(self) -> None:
        ceis_sanctions, ceis_entities = self._process_rows(self._raw_ceis, "CEIS")
        cnep_sanctions, cnep_entities = self._process_rows(self._raw_cnep, "CNEP")

        all_sanctions = ceis_sanctions + cnep_sanctions
        all_entities = ceis_entities + cnep_entities

        self.sanctions = deduplicate_rows(all_sanctions, ["sanction_id"])
        self.sanctioned_entities = all_entities

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.sanctions:
            loader.load_nodes("Sanction", self.sanctions, key_field="sanction_id")

        for ent in self.sanctioned_entities:
            label = ent["entity_label"]
            key_field = ent["entity_key_field"]
            doc = ent["entity_doc"]
            name = ent["entity_name"]

            node_row: dict[str, Any] = {key_field: doc, "name": name}
            if label == "Company":
                node_row["razao_social"] = name
            loader.load_nodes(label, [node_row], key_field=key_field)

        if self.sanctioned_entities:
            rel_rows = [
                {"source_key": e["source_key"], "target_key": e["target_key"]}
                for e in self.sanctioned_entities
            ]

            query = (
                "UNWIND $rows AS row "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "OPTIONAL MATCH (c:Company {cnpj: row.source_key}) "
                "OPTIONAL MATCH (p:Person {cpf: row.source_key}) "
                "WITH s, coalesce(c, p) AS entity "
                "WHERE entity IS NOT NULL "
                "MERGE (entity)-[:SANCIONADA]->(s)"
            )
            loader.run_query(query, rel_rows)

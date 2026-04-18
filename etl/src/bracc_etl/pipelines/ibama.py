from __future__ import annotations

import csv
import io
import logging
import sys
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
    parse_numeric_comma,
    strip_document,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Module-level fetch_to_disk: CKAN "Termos de Embargo" bulk CSV download.
# --------------------------------------------------------------------------
#
# IBAMA's CKAN portal (https://dadosabertos.ibama.gov.br/) publishes the
# fiscalização dataset "termo de embargo" as a ZIP-wrapped CSV. This is the
# closest public bulk feed to the legacy SICAFI "areas_embargadas.csv" the
# pipeline was originally designed for. The modern schema differs: column
# names are shorter (`NOME_EMBARGADO`, `UF`, `MUNICIPIO`, `DES_TAD`) and
# there is no biome column and no WKT field. ``fetch_to_disk`` therefore:
#   1. streams the ZIP from CKAN,
#   2. extracts ``termo_embargo.csv`` in-memory,
#   3. remaps the modern column set onto the legacy names
#      ``IbamaPipeline.extract`` reads, writing ``areas_embargadas.csv``
#      with ``;`` separator / UTF-8 encoding so extract() is byte-compatible.
#
# Columns with no modern counterpart (``DES_TIPO_BIOMA``,
# ``WKT_GEOM_AREA_EMBARGADA``) are written as empty strings; the pipeline
# handles empty biome gracefully and never reads the WKT field.
#
# IBAMA does not expose a UF filter in the HTTP endpoint, so filtering (if
# requested) happens after the CSV is parsed in memory.

_IBAMA_TERMO_EMBARGO_ZIP = (
    "https://dadosabertos.ibama.gov.br/dados/SIFISC/termo_embargo/"
    "termo_embargo/termo_embargo_csv.zip"
)
_IBAMA_ZIP_MEMBER = "termo_embargo.csv"
_IBAMA_HTTP_TIMEOUT = 300.0
# Some rows embed MULTIPOLYGON WKT strings that exceed Python's default
# 128 KiB csv field cap; lift the cap to the system maximum so the reader
# never truncates a polygon mid-coordinate. Cap at ~1 GiB on 64-bit systems.
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# Legacy pipeline schema (minus WKT, which extract() already excludes).
_IBAMA_LEGACY_COLUMNS = [
    "SEQ_TAD",
    "CPF_CNPJ_EMBARGADO",
    "NOME_PESSOA_EMBARGADA",
    "DAT_EMBARGO",
    "QTD_AREA_EMBARGADA",
    "DES_TIPO_BIOMA",
    "SIG_UF_TAD",
    "NOM_MUNICIPIO_TAD",
    "DES_INFRACAO",
    "NUM_AUTO_INFRACAO",
    "NUM_PROCESSO",
]

# Modern CKAN header -> legacy canonical column.
_IBAMA_MODERN_TO_LEGACY = {
    "SEQ_TAD": "SEQ_TAD",
    "CPF_CNPJ_EMBARGADO": "CPF_CNPJ_EMBARGADO",
    "NOME_EMBARGADO": "NOME_PESSOA_EMBARGADA",
    "DAT_EMBARGO": "DAT_EMBARGO",
    "QTD_AREA_EMBARGADA": "QTD_AREA_EMBARGADA",
    "UF": "SIG_UF_TAD",
    "MUNICIPIO": "NOM_MUNICIPIO_TAD",
    "DES_TAD": "DES_INFRACAO",
    "NUM_AUTO_INFRACAO": "NUM_AUTO_INFRACAO",
    "NUM_PROCESSO": "NUM_PROCESSO",
}


def fetch_to_disk(
    output_dir: Path | str,
    uf: str | None = None,
    limit: int | None = None,
    url: str = _IBAMA_TERMO_EMBARGO_ZIP,
    timeout: float = _IBAMA_HTTP_TIMEOUT,
) -> list[Path]:
    """Download IBAMA Termos de Embargo and write ``areas_embargadas.csv``.

    The CKAN resource is a ZIP (~45 MB) containing a single CSV
    (``termo_embargo.csv``, ~160 MB unpacked, ~113k rows national).

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing. The file written is
        ``<output_dir>/areas_embargadas.csv`` (same path ``extract()`` reads).
    uf:
        Optional UF code (two-letter) — when set, only rows whose ``UF``
        column matches are kept. ``"ALL"``/``"*"`` is treated as no filter.
    limit:
        Optional cap on the number of kept rows (applied after UF filter).
        Useful for smoke tests on the full ~160 MB CSV.
    url:
        Override for the CKAN resource URL (tests + future rotations).
    timeout:
        Per-request HTTP timeout.

    Returns
    -------
    List of absolute paths written (one element when the download succeeds).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "areas_embargadas.csv"

    uf_token = (uf or "").strip().upper() or None
    if uf_token in {"ALL", "*"}:
        uf_token = None

    logger.info(
        "[ibama.fetch_to_disk] Downloading %s (uf=%s, limit=%s) -> %s",
        url, uf_token or "ALL", limit, out_path,
    )

    # Stream the ZIP to a temp buffer (it's ~45 MB; buffering in memory is
    # simpler than a spooled tmpfile and comfortably fits).
    buf = io.BytesIO()
    with httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "br-acc/bracc-etl download_ibama (httpx)"},
        timeout=timeout,
    ) as client, client.stream("GET", url) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_bytes(chunk_size=1 << 16):
            if chunk:
                buf.write(chunk)
    buf.seek(0)
    logger.info(
        "[ibama.fetch_to_disk] ZIP received: %.2f MB",
        buf.getbuffer().nbytes / 1024 / 1024,
    )

    # Extract the single CSV member in-stream and remap columns as we write.
    kept = 0
    scanned = 0
    with zipfile.ZipFile(buf) as zf:
        if _IBAMA_ZIP_MEMBER not in zf.namelist():
            # Fall back to whatever .csv is inside (CKAN has historically
            # shipped a single-CSV archive, but hedge against renames).
            csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_members:
                raise RuntimeError(
                    f"IBAMA ZIP contains no CSV member: {zf.namelist()}"
                )
            member = csv_members[0]
            logger.warning(
                "[ibama.fetch_to_disk] expected %s in ZIP, using %s",
                _IBAMA_ZIP_MEMBER, member,
            )
        else:
            member = _IBAMA_ZIP_MEMBER

        with zf.open(member) as src, out_path.open(
            "w", encoding="utf-8", newline="",
        ) as dst:
            reader = csv.DictReader(
                io.TextIOWrapper(src, encoding="utf-8", newline=""),
                delimiter=";",
            )
            writer = csv.DictWriter(
                dst, fieldnames=_IBAMA_LEGACY_COLUMNS, delimiter=";",
            )
            writer.writeheader()
            for row in reader:
                scanned += 1
                if uf_token:
                    row_uf = (row.get("UF") or "").strip().upper()
                    if row_uf != uf_token:
                        continue
                out_row = {col: "" for col in _IBAMA_LEGACY_COLUMNS}
                for modern, legacy in _IBAMA_MODERN_TO_LEGACY.items():
                    value = row.get(modern)
                    if value is not None:
                        out_row[legacy] = value
                writer.writerow(out_row)
                kept += 1
                if limit is not None and kept >= limit:
                    break

    logger.info(
        "[ibama.fetch_to_disk] wrote %d / %d rows to %s (%.2f MB)",
        kept, scanned, out_path, out_path.stat().st_size / 1024 / 1024,
    )
    return [out_path.resolve()]


class IbamaPipeline(Pipeline):
    """ETL pipeline for IBAMA environmental enforcement data.

    Ingests embargoed areas (Termos de Embargo) from IBAMA open data.
    Each record links a person or company to an environmental embargo
    with associated infraction data, biome, area, and location.
    """

    name = "ibama"
    source_id = "ibama"

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
        self.embargoes: list[dict[str, Any]] = []
        self.companies: list[dict[str, Any]] = []
        self.persons: list[dict[str, Any]] = []
        self.embargo_rels: list[dict[str, Any]] = []

    def _primary_biome(self, value: str) -> str:
        """Extract the primary biome from a comma-separated list."""
        value = value.strip()
        if not value:
            return ""
        return value.split(",")[0].strip()

    def extract(self) -> None:
        ibama_dir = Path(self.data_dir) / "ibama"
        if not ibama_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, ibama_dir)
            return
        csv_path = ibama_dir / "areas_embargadas.csv"
        if not csv_path.exists():
            logger.warning("[%s] CSV file not found: %s", self.name, csv_path)
            return
        logger.info("[ibama] Reading %s", csv_path)
        self._raw = pd.read_csv(
            csv_path,
            sep=";",
            dtype=str,
            encoding="utf-8",
            keep_default_na=False,
            on_bad_lines="skip",
            usecols=lambda c: c != "WKT_GEOM_AREA_EMBARGADA",
        )
        if self.limit:
            self._raw = self._raw.head(self.limit)
        logger.info("[ibama] Extracted %d rows", len(self._raw))

    def transform(self) -> None:
        embargoes: list[dict[str, Any]] = []
        companies: list[dict[str, Any]] = []
        persons: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            seq = str(row["SEQ_TAD"]).strip()
            if not seq:
                continue

            doc_raw = str(row["CPF_CNPJ_EMBARGADO"]).strip()
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["NOME_PESSOA_EMBARGADA"]))
            is_company = len(digits) == 14
            is_person = len(digits) == 11

            if not is_company and not is_person:
                continue

            doc_formatted = format_cnpj(doc_raw) if is_company else format_cpf(doc_raw)

            embargo_id = f"ibama_embargo_{seq}"
            date_embargo = parse_date(str(row["DAT_EMBARGO"]))
            area_ha = parse_numeric_comma(row["QTD_AREA_EMBARGADA"])
            biome = self._primary_biome(str(row["DES_TIPO_BIOMA"]))
            uf = str(row["SIG_UF_TAD"]).strip()
            municipio = str(row["NOM_MUNICIPIO_TAD"]).strip()
            infraction_desc = str(row["DES_INFRACAO"]).strip()[:500]
            auto_num = str(row["NUM_AUTO_INFRACAO"]).strip()
            processo = str(row["NUM_PROCESSO"]).strip()

            embargoes.append({
                "embargo_id": embargo_id,
                "date": date_embargo,
                "area_ha": area_ha,
                "biome": biome,
                "uf": uf,
                "municipio": municipio,
                "infraction": infraction_desc,
                "auto_infracao": auto_num,
                "processo": processo,
                "source": "ibama",
            })

            if is_company:
                companies.append({
                    "cnpj": doc_formatted,
                    "razao_social": nome,
                    "name": nome,
                })
            else:
                persons.append({
                    "cpf": doc_formatted,
                    "name": nome,
                })

            rels.append({
                "source_key": doc_formatted,
                "target_key": embargo_id,
                "is_company": is_company,
            })

        self.embargoes = deduplicate_rows(embargoes, ["embargo_id"])
        self.companies = deduplicate_rows(companies, ["cnpj"])
        self.persons = deduplicate_rows(persons, ["cpf"])
        self.embargo_rels = rels

        logger.info(
            "[ibama] Transformed: %d embargoes, %d companies, %d persons, %d rels",
            len(self.embargoes),
            len(self.companies),
            len(self.persons),
            len(self.embargo_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.embargoes:
            logger.info("[ibama] Loading %d Embargo nodes...", len(self.embargoes))
            loader.load_nodes("Embargo", self.embargoes, key_field="embargo_id")

        if self.companies:
            logger.info("[ibama] MERGEing %d Company nodes...", len(self.companies))
            loader.load_nodes("Company", self.companies, key_field="cnpj")

        if self.persons:
            logger.info("[ibama] MERGEing %d Person nodes...", len(self.persons))
            loader.load_nodes("Person", self.persons, key_field="cpf")

        if self.embargo_rels:
            logger.info("[ibama] Loading %d EMBARGADA rels...", len(self.embargo_rels))
            query = (
                "UNWIND $rows AS row "
                "MATCH (e:Embargo {embargo_id: row.target_key}) "
                "OPTIONAL MATCH (c:Company {cnpj: row.source_key}) "
                "OPTIONAL MATCH (p:Person {cpf: row.source_key}) "
                "WITH e, coalesce(c, p) AS entity "
                "WHERE entity IS NOT NULL "
                "MERGE (entity)-[:EMBARGADA]->(e)"
            )
            loader.run_query(query, self.embargo_rels)

        logger.info("[ibama] Load complete.")

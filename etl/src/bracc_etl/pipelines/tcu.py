from __future__ import annotations

import csv
import html as _html
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Module-level constants + fetch_to_disk (best-effort public scraper).
# --------------------------------------------------------------------------
#
# The TCU publishes its Sistema de Inabilitados e Inidôneos as an Oracle
# APEX 18 application at https://contas.tcu.gov.br/ords/f?p=1660:
#   * page 1 -> "Relação de inabilitados"  (704 rows nationally)
#   * page 2 -> "Relação de inidôneos"     (104 rows nationally)
#
# Neither page exposes a stable CSV/XLSX export or REST endpoint (the APEX
# App Builder is blocked outside TCU's internal network, and no Interactive
# Report export button is rendered in the public skin). The two other
# datasets the pipeline consumes — "contas julgadas irregulares" and the
# electoral variant — are not exposed publicly at all.
#
# ``fetch_to_disk`` therefore does a best-effort HTML scrape of the first
# page of each public APEX report (100 rows each, which is already the
# majority of the inidôneos universe and ~14% of inabilitados) and writes
# empty, header-only stubs for the two non-public datasets so that
# ``TcuPipeline.extract()`` does not FileNotFoundError when data/tcu/ has
# just been bootstrapped. See ``scripts/download_tcu.py`` for the CLI.
#
# UF filtering:
#   * inidôneos has a UF column on the public report, so rows are filtered
#     when ``uf`` is set (default "GO").
#   * inabilitados does NOT expose UF/MUNICIPIO publicly; ``uf`` is ignored
#     for that file (rows are emitted with empty UF/MUNICIPIO columns).
#
# This script is intentionally narrow — when the two blocked datasets
# become available (or an official CSV bulk endpoint is published), drop
# the existing file-manifest drops into data/tcu/ and fetch_to_disk will
# happily be superseded by them.

TCU_APEX_BASE = "https://contas.tcu.gov.br/ords"
TCU_APP_ID = 1660
TCU_INABILITADOS_PAGE = 1
TCU_INIDONEOS_PAGE = 2

# Columns the ETL pipeline (TcuPipeline.extract -> _read_csv) expects.
_INABILITADOS_COLS = [
    "CPF", "NOME", "PROCESSO", "DELIBERACAO",
    "DATA TRANSITO JULGADO", "DATA FINAL", "DATA ACORDAO",
    "UF", "MUNICIPIO",
]
_INIDONEOS_COLS = [
    "CPF_CNPJ", "NOME", "PROCESSO", "DELIBERACAO",
    "DATA TRANSITO JULGADO", "DATA FINAL", "DATA ACORDAO",
    "UF", "MUNICIPIO",
]
_IRREGULARES_COLS = [
    "CPF_CNPJ", "NOME", "PROCESSO", "DELIBERACAO",
    "DATA TRANSITO JULGADO", "UF", "MUNICIPIO",
]
_IRREGULARES_ELEITORAIS_COLS = [
    "CPF", "NOME", "PROCESSO", "DELIBERACAO",
    "DATA TRANSITO JULGADO", "DATA FINAL",
    "UF", "MUNICIPIO", "CARGO/FUNCAO",
]

_HEADER_RE = re.compile(
    r'<th[^>]*id="([^"]+)"[^>]*>.*?<a[^>]*>([^<]+)</a>', re.DOTALL,
)
_ROW_RE = re.compile(r'<tr\s[^>]*>(.*?)</tr>', re.DOTALL)
_CELL_RE = re.compile(
    r'<td[^>]*headers="([^"]+)"[^>]*>(.*?)</td>', re.DOTALL,
)
_TOTAL_RE = re.compile(r'Total de Linhas = (\d+)')


def _clean_cell(raw: str) -> str:
    """Strip inner tags, unescape HTML entities, collapse whitespace."""
    no_tags = re.sub(r'<[^>]+>', '', raw)
    return re.sub(r'\s+', ' ', _html.unescape(no_tags)).strip()


def _scrape_apex_ir_page(
    client: httpx.Client, page_id: int, timeout: float = 30.0,
) -> tuple[list[str], list[list[str]], int | None]:
    """Fetch a public APEX IR page and return (header_ids, rows, total_rows).

    ``rows`` is a list of cell-value lists indexed in the same order as
    ``header_ids``. ``total_rows`` is the server-reported universe size
    (None if the summary banner is absent).
    """
    url = f"{TCU_APEX_BASE}/f?p={TCU_APP_ID}:{page_id}"
    resp = client.get(url, timeout=timeout)
    resp.raise_for_status()
    html = resp.text

    # Locate the IR table block (open tag + body, to preserve summary attr).
    open_tag = re.search(r'<table[^>]*class="a-IRR-table"[^>]*>', html)
    if not open_tag:
        raise RuntimeError(
            f"TCU APEX IR table not found on page {page_id}; upstream skin "
            "may have changed."
        )
    table_body = re.search(
        r'<table[^>]*class="a-IRR-table"[^>]*>(.+?)</table>',
        html, re.DOTALL,
    )
    assert table_body is not None  # matched above
    table = table_body.group(1)

    headers: list[tuple[str, str]] = _HEADER_RE.findall(table)
    header_ids = [h[0] for h in headers]

    rows: list[list[str]] = []
    for tr in _ROW_RE.findall(table):
        cells = _CELL_RE.findall(tr)
        if not cells:
            continue
        by_header = {k: _clean_cell(v) for k, v in cells}
        rows.append([by_header.get(h, "") for h in header_ids])

    # Server-reported universe size lives in the <table summary="..."> attr,
    # e.g. 'Total de Linhas = 704' (HTML-entity-escaped as '=').
    banner = _html.unescape(open_tag.group(0))
    total = _TOTAL_RE.search(banner)
    total_rows = int(total.group(1)) if total else None
    return header_ids, rows, total_rows


# Header-id → pipeline-column mapping for each dataset. Header ids come
# straight from the APEX markup and are stable as long as the underlying
# worksheet is not rebuilt.
_INABILITADOS_MAP: dict[str, str] = {
    "NOME": "NOME",
    "NUM_CPFCNPJ": "CPF",
    "TC": "PROCESSO",
    "NUMDELIB": "DELIBERACAO",
    "TJ": "DATA TRANSITO JULGADO",
    "DATA_FINAL": "DATA FINAL",
    # "Data do acórdão" header id is a numeric column id; matched loosely.
}
_INIDONEOS_MAP: dict[str, str] = {
    "NOME": "NOME",
    "NUM_CPFCNPJ": "CPF_CNPJ",
    "UF": "UF",
    "TC": "PROCESSO",
    "NUMDELIB": "DELIBERACAO",
    "TJ": "DATA TRANSITO JULGADO",
    "DATA_FINAL": "DATA FINAL",
}


def _remap_row(
    header_ids: list[str],
    values: list[str],
    mapping: dict[str, str],
    all_cols: list[str],
) -> dict[str, str]:
    """Project a scraped row onto the pipeline's canonical schema.

    Unknown APEX columns (e.g. "Data do acórdão" with its numeric id) are
    heuristically routed to ``DATA ACORDAO`` when that column is empty.
    """
    out: dict[str, str] = {c: "" for c in all_cols}
    for hid, val in zip(header_ids, values, strict=False):
        canonical = mapping.get(hid)
        if canonical and canonical in out:
            out[canonical] = val
        elif "DATA ACORDAO" in out and not out["DATA ACORDAO"] and val:
            # Heuristic: the trailing "Data do acórdão" column has a
            # numeric header id; map it here if we haven't already.
            out["DATA ACORDAO"] = val
    return out


def _write_pipe_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Write ``rows`` as pipe-delimited UTF-8, matching _read_csv's dialect."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, delimiter="|")
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r.get(c, "") for c in columns})


def fetch_to_disk(
    output_dir: Path,
    uf: str | None = None,
    years: list[int] | None = None,
    limit: int | None = None,
) -> list[Path]:
    """Download TCU sanction data to ``output_dir`` (best-effort).

    Writes the four CSVs ``TcuPipeline.extract`` looks for, using the
    canonical pipe-delimited / UTF-8 dialect:

    * ``inabilitados-funcao-publica.csv`` — scraped from APEX page 1.
      UF/MUNICIPIO columns are left empty (source does not expose them on
      the public report). No source-side UF filter possible here.
    * ``licitantes-inidoneos.csv`` — scraped from APEX page 2. Rows are
      filtered by ``uf`` only when the caller passes it explicitly.
    * ``resp-contas-julgadas-irregulares.csv`` — header-only stub; the
      upstream dataset is not exposed publicly.
    * ``resp-contas-julgadas-irreg-implicacao-eleitoral.csv`` — header-only
      stub for the same reason.

    Parameters
    ----------
    output_dir:
        Destination. Created if missing.
    uf:
        Optional UF code (two-letter). When set, keeps only inidôneos rows
        whose UF matches. The APEX public report exposes at most ~100 rows
        and the ``UF`` column there appears to reflect TCU's processing
        unit (observed to be ``DF`` for every row sampled), so filtering
        is opt-in and typically left off so ``TcuPipeline`` sees the full
        public slice and correlates by CNPJ downstream.
    years:
        Accepted for API symmetry with other ``fetch_to_disk`` callers;
        the TCU reports do not expose a year filter, so this is informational
        only (logged).
    limit:
        If set, truncate the scraped rows of each public report to the
        first N (applied after UF filtering). Useful for smoke tests.

    Returns
    -------
    List of absolute paths to every CSV written (always 4 files, even
    when some are header-only stubs).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if years is not None:
        logger.info(
            "[tcu.fetch_to_disk] --years=%s ignored (TCU reports are "
            "rolling snapshots without a year filter)", years,
        )

    uf_token = (uf or "").strip().upper() or None
    if uf_token in {"ALL", "*"}:
        uf_token = None

    written: list[Path] = []
    with httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "br-acc/bracc-etl download_tcu (httpx)"},
    ) as client:
        # --- inabilitados ---
        hdrs, raw_rows, total = _scrape_apex_ir_page(client, TCU_INABILITADOS_PAGE)
        logger.info(
            "[tcu.fetch_to_disk] inabilitados: %d scraped / %s total "
            "(APEX public report exposes only the first page)",
            len(raw_rows), total,
        )
        inab_rows = [
            _remap_row(hdrs, v, _INABILITADOS_MAP, _INABILITADOS_COLS)
            for v in raw_rows
        ]
        # The public inabilitados report has no UF column — ``uf_token`` is
        # not applied here. Document this in the file by leaving UF empty.
        if limit is not None:
            inab_rows = inab_rows[:limit]
        inab_path = output_dir / "inabilitados-funcao-publica.csv"
        _write_pipe_csv(inab_path, _INABILITADOS_COLS, inab_rows)
        written.append(inab_path.resolve())

        # --- inidôneos ---
        hdrs2, raw_rows2, total2 = _scrape_apex_ir_page(client, TCU_INIDONEOS_PAGE)
        logger.info(
            "[tcu.fetch_to_disk] inidôneos: %d scraped / %s total "
            "(APEX public report exposes only the first page)",
            len(raw_rows2), total2,
        )
        inid_rows = [
            _remap_row(hdrs2, v, _INIDONEOS_MAP, _INIDONEOS_COLS)
            for v in raw_rows2
        ]
        if uf_token:
            before = len(inid_rows)
            inid_rows = [r for r in inid_rows if r.get("UF", "").upper() == uf_token]
            logger.info(
                "[tcu.fetch_to_disk] inidôneos: kept %d/%d rows matching UF=%s",
                len(inid_rows), before, uf_token,
            )
        if limit is not None:
            inid_rows = inid_rows[:limit]
        inid_path = output_dir / "licitantes-inidoneos.csv"
        _write_pipe_csv(inid_path, _INIDONEOS_COLS, inid_rows)
        written.append(inid_path.resolve())

    # --- blocked datasets: write header-only stubs so the pipeline does
    # not crash on missing files. These are intentionally empty. When the
    # TCU publishes the upstream CSV (or when a privileged bootstrap run
    # drops the real files here), TcuPipeline.extract() picks them up
    # transparently.
    irr_path = output_dir / "resp-contas-julgadas-irregulares.csv"
    _write_pipe_csv(irr_path, _IRREGULARES_COLS, [])
    written.append(irr_path.resolve())
    logger.warning(
        "[tcu.fetch_to_disk] resp-contas-julgadas-irregulares.csv: "
        "header-only stub (upstream dataset is not exposed publicly)",
    )

    irr_el_path = output_dir / "resp-contas-julgadas-irreg-implicacao-eleitoral.csv"
    _write_pipe_csv(irr_el_path, _IRREGULARES_ELEITORAIS_COLS, [])
    written.append(irr_el_path.resolve())
    logger.warning(
        "[tcu.fetch_to_disk] resp-contas-julgadas-irreg-implicacao-eleitoral.csv: "
        "header-only stub (upstream dataset is not exposed publicly)",
    )

    return written


class TcuPipeline(Pipeline):
    """ETL pipeline for TCU (Tribunal de Contas da Uniao) accountability data.

    Loads four datasets:
    - inabilitados: individuals barred from public office
    - licitantes inidoneos: companies declared unfit for public bidding
    - contas julgadas irregulares: persons with irregular accounts
    - contas irregulares com implicacao eleitoral: same with electoral context
    """

    name = "tcu"
    source_id = "tcu"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_inabilitados: pd.DataFrame = pd.DataFrame()
        self._raw_inidoneos: pd.DataFrame = pd.DataFrame()
        self._raw_irregulares: pd.DataFrame = pd.DataFrame()
        self._raw_irregulares_eleitorais: pd.DataFrame = pd.DataFrame()
        self.sanctions: list[dict[str, Any]] = []
        self.sanctioned_persons: list[dict[str, Any]] = []
        self.sanctioned_companies: list[dict[str, Any]] = []

    def _read_csv(self, path: Path) -> pd.DataFrame:
        return pd.read_csv(
            path,
            dtype=str,
            sep="|",
            encoding="utf-8",
            keep_default_na=False,
            quotechar='"',
        )

    def extract(self) -> None:
        tcu_dir = Path(self.data_dir) / "tcu"

        self._raw_inabilitados = self._read_csv(
            tcu_dir / "inabilitados-funcao-publica.csv"
        )
        self._raw_inidoneos = self._read_csv(
            tcu_dir / "licitantes-inidoneos.csv"
        )
        self._raw_irregulares = self._read_csv(
            tcu_dir / "resp-contas-julgadas-irregulares.csv"
        )
        self._raw_irregulares_eleitorais = self._read_csv(
            tcu_dir / "resp-contas-julgadas-irreg-implicacao-eleitoral.csv"
        )

        logger.info(
            "[tcu] Extracted: %d inabilitados, %d inidoneos, "
            "%d irregulares, %d irregulares eleitorais",
            len(self._raw_inabilitados),
            len(self._raw_inidoneos),
            len(self._raw_irregulares),
            len(self._raw_irregulares_eleitorais),
        )

    def _process_inabilitados(self) -> None:
        """Persons barred from public office (CPF-only)."""
        for idx, row in self._raw_inabilitados.iterrows():
            cpf_raw = str(row["CPF"]).strip()
            digits = strip_document(cpf_raw)
            if len(digits) != 11:
                continue

            cpf = format_cpf(cpf_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            date_end = parse_date(str(row["DATA FINAL"]))
            date_acordao = parse_date(str(row["DATA ACORDAO"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()

            sanction_id = f"tcu_inabilitado_{digits}_{idx}"
            self.sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_inabilitado",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": date_end,
                "date_acordao": date_acordao,
                "uf": uf,
                "municipio": municipio,
                "cargo": "",
                "source": "tcu",
            })
            self.sanctioned_persons.append({
                "cpf": cpf,
                "name": nome,
                "sanction_id": sanction_id,
            })

    def _process_inidoneos(self) -> None:
        """Companies declared unfit for public bidding (CNPJ-only)."""
        for idx, row in self._raw_inidoneos.iterrows():
            doc_raw = str(row["CPF_CNPJ"]).strip()
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            date_end = parse_date(str(row["DATA FINAL"]))
            date_acordao = parse_date(str(row["DATA ACORDAO"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()

            sanction_id = f"tcu_inidoneo_{digits}_{idx}"
            self.sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_inidoneo",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": date_end,
                "date_acordao": date_acordao,
                "uf": uf,
                "municipio": municipio,
                "cargo": "",
                "source": "tcu",
            })

            if len(digits) == 14:
                cnpj = format_cnpj(doc_raw)
                self.sanctioned_companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome,
                    "name": nome,
                    "sanction_id": sanction_id,
                })
            elif len(digits) == 11:
                cpf = format_cpf(doc_raw)
                self.sanctioned_persons.append({
                    "cpf": cpf,
                    "name": nome,
                    "sanction_id": sanction_id,
                })

    def _process_irregulares(self) -> None:
        """Persons with accounts judged irregular (may have CPF or CNPJ)."""
        for idx, row in self._raw_irregulares.iterrows():
            doc_raw = str(row["CPF_CNPJ"]).strip()
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()

            sanction_id = f"tcu_irregular_{digits}_{idx}"
            self.sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_conta_irregular",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": "",
                "date_acordao": "",
                "uf": uf,
                "municipio": municipio,
                "cargo": "",
                "source": "tcu",
            })

            if len(digits) == 14:
                cnpj = format_cnpj(doc_raw)
                self.sanctioned_companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome,
                    "name": nome,
                    "sanction_id": sanction_id,
                })
            elif len(digits) == 11:
                cpf = format_cpf(doc_raw)
                self.sanctioned_persons.append({
                    "cpf": cpf,
                    "name": nome,
                    "sanction_id": sanction_id,
                })

    def _process_irregulares_eleitorais(self) -> None:
        """Persons with irregular accounts and electoral implication (CPF-only)."""
        for idx, row in self._raw_irregulares_eleitorais.iterrows():
            cpf_raw = str(row["CPF"]).strip()
            digits = strip_document(cpf_raw)
            if len(digits) != 11:
                continue

            cpf = format_cpf(cpf_raw)
            nome = normalize_name(str(row["NOME"]))
            processo = str(row["PROCESSO"]).strip()
            deliberacao = str(row["DELIBERACAO"]).strip()
            date_start = parse_date(str(row["DATA TRANSITO JULGADO"]))
            date_end = parse_date(str(row["DATA FINAL"]))
            uf = str(row["UF"]).strip()
            municipio = str(row["MUNICIPIO"]).strip()
            cargo = str(row.get("CARGO/FUNCAO", "")).strip()

            sanction_id = f"tcu_irregular_eleitoral_{digits}_{idx}"
            self.sanctions.append({
                "sanction_id": sanction_id,
                "type": "tcu_conta_irregular_eleitoral",
                "court": "TCU",
                "processo": processo,
                "deliberacao": deliberacao,
                "date_start": date_start,
                "date_end": date_end,
                "date_acordao": "",
                "uf": uf,
                "municipio": municipio,
                "cargo": cargo,
                "source": "tcu",
            })
            self.sanctioned_persons.append({
                "cpf": cpf,
                "name": nome,
                "sanction_id": sanction_id,
            })

    def transform(self) -> None:
        self._process_inabilitados()
        self._process_inidoneos()
        self._process_irregulares()
        self._process_irregulares_eleitorais()

        self.sanctions = deduplicate_rows(self.sanctions, ["sanction_id"])

        logger.info(
            "[tcu] Transformed: %d sanctions, %d person links, %d company links",
            len(self.sanctions),
            len(self.sanctioned_persons),
            len(self.sanctioned_companies),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # Load Sanction nodes
        if self.sanctions:
            loader.load_nodes("Sanction", self.sanctions, key_field="sanction_id")
            logger.info("[tcu] Loaded %d Sanction nodes", len(self.sanctions))

        # Merge Person nodes and create relationships
        if self.sanctioned_persons:
            person_nodes = deduplicate_rows(
                [{"cpf": p["cpf"], "name": p["name"]} for p in self.sanctioned_persons],
                ["cpf"],
            )
            loader.load_nodes("Person", person_nodes, key_field="cpf")
            logger.info("[tcu] Merged %d Person nodes", len(person_nodes))

            person_rels = [
                {"source_key": p["cpf"], "target_key": p["sanction_id"]}
                for p in self.sanctioned_persons
            ]
            query_person = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "MERGE (p)-[:SANCIONADA]->(s)"
            )
            loader.run_query(query_person, person_rels)
            logger.info("[tcu] Created %d Person-SANCIONADA->Sanction rels", len(person_rels))

        # Merge Company nodes and create relationships
        if self.sanctioned_companies:
            company_nodes = deduplicate_rows(
                [
                    {"cnpj": c["cnpj"], "razao_social": c["razao_social"], "name": c["name"]}
                    for c in self.sanctioned_companies
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", company_nodes, key_field="cnpj")
            logger.info("[tcu] Merged %d Company nodes", len(company_nodes))

            company_rels = [
                {"source_key": c["cnpj"], "target_key": c["sanction_id"]}
                for c in self.sanctioned_companies
            ]
            query_company = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "MERGE (c)-[:SANCIONADA]->(s)"
            )
            loader.run_query(query_company, company_rels)
            logger.info("[tcu] Created %d Company-SANCIONADA->Sanction rels", len(company_rels))

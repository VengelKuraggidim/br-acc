from __future__ import annotations

import csv
import io
import logging
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import format_cnpj, normalize_name, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# TP_DEPENDENCIA codes
ADMIN_TYPE = {
    "1": "federal",
    "2": "estadual",
    "3": "municipal",
    "4": "privada",
}

# TP_SITUACAO_FUNCIONAMENTO codes
STATUS_MAP = {
    "1": "em_atividade",
    "2": "paralisada",
    "3": "extinta",
}

# ── Download / fetch_to_disk (for scripts/download_inep.py) ──────────────
#
# INEP publishes the Censo Escolar microdata as a yearly ZIP at
# ``download.inep.gov.br/dados_abertos/microdados_censo_escolar_<YYYY>.zip``.
# The 2022 archive is ~26 MB and unpacks to ~190 MB of CSVs; the file the
# pipeline consumes is ``microdados_ed_basica_<YYYY>.csv`` (latin-1,
# semicolon-delimited).
#
# Two practical caveats:
#   * The ``download.inep.gov.br`` certificate chain consistently fails
#     default-bundle verification on fresh httpx installs. The CLI defaults
#     to ``--insecure`` (verify=False) because the file integrity is
#     checked by the ZIP container; pass ``--no-insecure`` to opt out.
#   * The 2022 microdata omits ``QT_FUNCIONARIOS``; the pipeline's
#     ``_parse_int("")`` falls back to 0 (not a regression of this script).
_INEP_DOWNLOAD_BASE = "https://download.inep.gov.br/dados_abertos"
_INEP_USER_AGENT = "br-acc/bracc-etl download_inep (httpx)"
_INEP_HTTP_TIMEOUT = 600.0


def _find_main_csv(zf: zipfile.ZipFile, year: int) -> str | None:
    """Locate ``microdados_ed_basica_<year>.csv`` inside the INEP archive."""
    target = f"microdados_ed_basica_{year}.csv".lower()
    for name in zf.namelist():
        if Path(name).name.lower() == target:
            return name
    # Fallback: first CSV under a directory containing "ed_basica" or
    # "ed_basico" in case INEP renames the file in a future census.
    for name in zf.namelist():
        lname = name.lower()
        if lname.endswith(".csv") and "ed_basica" in lname:
            return name
    return None


def fetch_to_disk(
    output_dir: Path | str,
    *,
    year: int = 2022,
    limit: int | None = None,
    insecure: bool = True,
    timeout: float = _INEP_HTTP_TIMEOUT,
    url: str | None = None,
) -> list[Path]:
    """Download INEP Censo Escolar microdata to ``output_dir``.

    Streams ``microdados_censo_escolar_<year>.zip`` from
    ``download.inep.gov.br``, locates the main
    ``microdados_ed_basica_<year>.csv`` inside the archive, and writes it
    out in the same latin-1/semicolon dialect ``InepPipeline.extract``
    consumes.

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    year:
        Census year (default ``2022`` — the most recent stable release).
    limit:
        If set, truncates the output CSV to the first N data rows
        (header preserved). Useful for smoke tests against the 190 MB
        full file.
    insecure:
        When ``True`` (default), disables TLS verification because the
        INEP cert chain fails default-bundle verification. ZIP integrity
        catches tampering regardless.
    timeout:
        Per-request HTTP timeout in seconds.
    url:
        Override the source URL (test/forward-compat hook).

    Returns
    -------
    List of paths written (always one CSV — ``microdados_ed_basica_<year>.csv``).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    src_url = url or f"{_INEP_DOWNLOAD_BASE}/microdados_censo_escolar_{year}.zip"
    out_csv = output_dir / f"microdados_ed_basica_{year}.csv"

    logger.info(
        "[inep.fetch_to_disk] downloading %s (verify=%s) -> %s",
        src_url,
        not insecure,
        out_csv.name,
    )

    headers = {"User-Agent": _INEP_USER_AGENT}
    with httpx.Client(
        follow_redirects=True,
        headers=headers,
        timeout=timeout,
        verify=not insecure,
    ) as client:
        resp = client.get(src_url)
        resp.raise_for_status()
        zip_bytes = resp.content

    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        member = _find_main_csv(zf, year)
        if member is None:
            msg = (
                f"INEP zip for {year} does not contain a "
                f"microdados_ed_basica_{year}.csv member; got "
                f"{zf.namelist()[:5]}"
            )
            raise RuntimeError(msg)

        if limit is None:
            with zf.open(member) as src, out_csv.open("wb") as dst:
                while True:
                    block = src.read(1 << 20)
                    if not block:
                        break
                    dst.write(block)
        else:
            # Stream until ``limit`` data rows are written, plus header.
            written_rows = 0
            with zf.open(member) as src_bin, out_csv.open(
                "w", encoding="latin-1", newline=""
            ) as dst:
                text_src = io.TextIOWrapper(
                    src_bin, encoding="latin-1", newline=""
                )
                header = text_src.readline()
                if not header:
                    msg = f"INEP CSV {member!r} appears empty"
                    raise RuntimeError(msg)
                dst.write(header)
                for line in text_src:
                    dst.write(line)
                    written_rows += 1
                    if written_rows >= limit:
                        break
            logger.info(
                "[inep.fetch_to_disk] truncated to %d rows (limit=%d)",
                written_rows,
                limit,
            )

    size_mb = out_csv.stat().st_size / 1024 / 1024
    logger.info(
        "[inep.fetch_to_disk] wrote %s (%.2f MB)", out_csv, size_mb,
    )
    return [out_csv]


class InepPipeline(Pipeline):
    """ETL pipeline for INEP Censo Escolar (school census) data."""

    name = "inep"
    source_id = "inep_censo_escolar"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.schools: list[dict[str, Any]] = []
        self.school_company_links: list[dict[str, Any]] = []

    def extract(self) -> None:
        inep_dir = Path(self.data_dir) / "inep"
        csv_path = inep_dir / "microdados_ed_basica_2022.csv"

        if not csv_path.exists():
            msg = f"INEP CSV not found at {csv_path}"
            raise FileNotFoundError(msg)

        logger.info("[inep] Reading %s ...", csv_path)
        self._raw_rows: list[dict[str, str]] = []

        with open(csv_path, encoding="latin-1", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for i, row in enumerate(reader):
                self._raw_rows.append(row)
                if self.limit and i + 1 >= self.limit:
                    break

        logger.info("[inep] Extracted %d rows", len(self._raw_rows))

    def _parse_int(self, value: str) -> int:
        """Parse an integer string, returning 0 for empty/invalid."""
        value = value.strip()
        if not value:
            return 0
        try:
            return int(value)
        except ValueError:
            return 0

    def transform(self) -> None:
        schools: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []

        for row in self._raw_rows:
            school_id = row.get("CO_ENTIDADE", "").strip()
            if not school_id:
                continue

            name = normalize_name(row.get("NO_ENTIDADE", ""))
            municipality_code = row.get("CO_MUNICIPIO", "").strip()
            municipality_name = row.get("NO_MUNICIPIO", "").strip()
            uf = row.get("SG_UF", "").strip()
            admin_type_code = row.get("TP_DEPENDENCIA", "").strip()
            status_code = row.get("TP_SITUACAO_FUNCIONAMENTO", "").strip()

            enrollment = self._parse_int(row.get("QT_MAT_BAS", ""))
            staff = self._parse_int(row.get("QT_FUNCIONARIOS", ""))

            schools.append({
                "school_id": school_id,
                "name": name,
                "municipality_code": municipality_code,
                "municipality_name": municipality_name,
                "uf": uf,
                "admin_type": ADMIN_TYPE.get(admin_type_code, admin_type_code),
                "status": STATUS_MAP.get(status_code, status_code),
                "enrollment": enrollment,
                "staff": staff,
                "year": 2022,
                "source": "inep_censo_escolar",
            })

            # Link private schools to Company via CNPJ
            cnpj_raw = row.get("NU_CNPJ_ESCOLA_PRIVADA", "").strip()
            if cnpj_raw:
                digits = strip_document(cnpj_raw)
                if len(digits) == 14:
                    cnpj_formatted = format_cnpj(cnpj_raw)
                    links.append({
                        "source_key": cnpj_formatted,
                        "target_key": school_id,
                    })

            # Also link maintainer CNPJ if different
            cnpj_mant_raw = row.get("NU_CNPJ_MANTENEDORA", "").strip()
            if cnpj_mant_raw and cnpj_mant_raw != cnpj_raw:
                digits_mant = strip_document(cnpj_mant_raw)
                if len(digits_mant) == 14:
                    cnpj_mant_formatted = format_cnpj(cnpj_mant_raw)
                    links.append({
                        "source_key": cnpj_mant_formatted,
                        "target_key": school_id,
                    })

        self.schools = schools
        self.school_company_links = links
        logger.info(
            "[inep] Transformed %d schools, %d company links",
            len(self.schools),
            len(self.school_company_links),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.schools:
            loader.load_nodes("Education", self.schools, key_field="school_id")
            logger.info("[inep] Loaded %d Education nodes", len(self.schools))

        if self.school_company_links:
            query = (
                "UNWIND $rows AS row "
                "MATCH (e:Education {school_id: row.target_key}) "
                "MERGE (c:Company {cnpj: row.source_key}) "
                "MERGE (c)-[:MANTEDORA_DE]->(e)"
            )
            loader.run_query(query, self.school_company_links)
            logger.info(
                "[inep] Created %d MANTEDORA_DE relationships",
                len(self.school_company_links),
            )

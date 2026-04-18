from __future__ import annotations

import logging
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    parse_brl_flexible,
    strip_document,
)

logger = logging.getLogger(__name__)

# Portal da Transparencia bulk endpoint for parliamentary amendments.
# SIOP's own web UI (siop.planejamento.gov.br) is a QlikView dashboard with
# no machine-readable bulk export, so the CGU "emendas-parlamentares"
# dataset (same underlying budget data, refreshed yearly) is the practical
# open source. See docs/data-sources.md for the full rationale.
_TRANSPARENCIA_BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados"
_TRANSPARENCIA_DATASET = "emendas-parlamentares"

# Map of UF sigla -> full-name tokens that appear in the destination UF
# column ("UF") of the Portal da Transparencia emendas CSV. The column uses
# the full state name (e.g. "GOIÁS"), so we match case-insensitively against
# either the accented or unaccented form.
_UF_SIGLA_TO_FULLNAMES: dict[str, tuple[str, ...]] = {
    "AC": ("ACRE",),
    "AL": ("ALAGOAS",),
    "AP": ("AMAPÁ", "AMAPA"),
    "AM": ("AMAZONAS",),
    "BA": ("BAHIA",),
    "CE": ("CEARÁ", "CEARA"),
    "DF": ("DISTRITO FEDERAL",),
    "ES": ("ESPÍRITO SANTO", "ESPIRITO SANTO"),
    "GO": ("GOIÁS", "GOIAS"),
    "MA": ("MARANHÃO", "MARANHAO"),
    "MT": ("MATO GROSSO",),
    "MS": ("MATO GROSSO DO SUL",),
    "MG": ("MINAS GERAIS",),
    "PA": ("PARÁ", "PARA"),
    "PB": ("PARAÍBA", "PARAIBA"),
    "PR": ("PARANÁ", "PARANA"),
    "PE": ("PERNAMBUCO",),
    "PI": ("PIAUÍ", "PIAUI"),
    "RJ": ("RIO DE JANEIRO",),
    "RN": ("RIO GRANDE DO NORTE",),
    "RS": ("RIO GRANDE DO SUL",),
    "RO": ("RONDÔNIA", "RONDONIA"),
    "RR": ("RORAIMA",),
    "SC": ("SANTA CATARINA",),
    "SP": ("SÃO PAULO", "SAO PAULO"),
    "SE": ("SERGIPE",),
    "TO": ("TOCANTINS",),
}


def _classify_amendment_type(raw_type: str) -> str:
    """Normalize amendment type to a canonical category."""
    normalized = raw_type.strip().lower()
    if "individual" in normalized:
        return "individual"
    if "bancada" in normalized:
        return "bancada"
    if "comiss" in normalized:
        return "comissao"
    if "relator" in normalized:
        return "relator"
    return raw_type.strip()


class SiopPipeline(Pipeline):
    """ETL pipeline for SIOP parliamentary amendments detail.

    Source: Portal da Transparencia emendas-parlamentares yearly CSVs.
    Enriches existing Amendment nodes (from TransfereGov) or creates new ones
    with budget execution detail (authorized, committed, paid amounts),
    amendment type classification, program/action codes, and author linkage.
    """

    name = "siop"
    source_id = "siop"

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
        self.amendments: list[dict[str, Any]] = []
        self.authors: list[dict[str, Any]] = []
        self.author_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        siop_dir = Path(self.data_dir) / "siop"
        csv_files = sorted(siop_dir.glob("*.csv"))
        if not csv_files:
            return

        frames: list[pd.DataFrame] = []
        for csv_path in csv_files:
            df = pd.read_csv(
                csv_path,
                dtype=str,
                encoding="latin-1",
                sep=";",
                keep_default_na=False,
            )
            frames.append(df)

        self._raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    @staticmethod
    def _resolve_col(row: Any, *candidates: str) -> str:
        """Return the first non-empty value found among column name candidates."""
        for c in candidates:
            val = row.get(c)
            if val is not None and str(val).strip():
                return str(val).strip()
        return ""

    def transform(self) -> None:
        if self._raw.empty:
            return

        amendments: list[dict[str, Any]] = []
        authors: list[dict[str, Any]] = []
        author_rels: list[dict[str, Any]] = []

        # Detect the amendment code column (two naming conventions)
        col_code: str | None = None
        for candidate in ("CÓDIGO EMENDA", "Código da Emenda"):
            if candidate in self._raw.columns:
                col_code = candidate
                break
        if col_code is None:
            return

        grouped = self._raw.groupby(col_code)

        for code, group in grouped:
            code_str = str(code).strip()
            if not code_str or code_str.lower() == "sem informação":
                continue

            first = group.iloc[0]

            year = self._resolve_col(first, "ANO", "Ano da Emenda")
            amendment_number = self._resolve_col(
                first, "NÚMERO EMENDA", "Número da emenda"
            )
            raw_type = self._resolve_col(first, "TIPO EMENDA", "Tipo de Emenda")
            amendment_type = _classify_amendment_type(raw_type)
            author_name = normalize_name(
                self._resolve_col(first, "AUTOR EMENDA", "Nome do Autor da Emenda")
            )
            author_doc = self._resolve_col(
                first, "CPF/CNPJ AUTOR", "Código do Autor da Emenda"
            )
            locality = self._resolve_col(
                first, "LOCALIDADE", "Localidade de aplicação do recurso"
            )

            # Program/action from first row (consistent within an amendment)
            program = normalize_name(
                self._resolve_col(first, "NOME PROGRAMA", "Nome Programa")
            )
            program_code = self._resolve_col(
                first, "CÓDIGO PROGRAMA", "Código Programa"
            )
            action = normalize_name(
                self._resolve_col(first, "NOME AÇÃO", "Nome Ação")
            )
            action_code = self._resolve_col(
                first, "CÓDIGO AÇÃO", "Código Ação"
            )
            function_name = normalize_name(
                self._resolve_col(first, "NOME FUNÇÃO", "Nome Função")
            )

            # Sum monetary values across all rows for this amendment
            amount_committed = sum(
                parse_brl_flexible(self._resolve_col(r, "VALOR EMPENHADO", "Valor Empenhado"))
                for _, r in group.iterrows()
            )
            amount_settled = sum(
                parse_brl_flexible(self._resolve_col(r, "VALOR LIQUIDADO", "Valor Liquidado"))
                for _, r in group.iterrows()
            )
            amount_paid = sum(
                parse_brl_flexible(self._resolve_col(r, "VALOR PAGO", "Valor Pago"))
                for _, r in group.iterrows()
            )

            # Build unique amendment_id from code
            amendment_id = f"siop_{code_str}"

            amendments.append({
                "amendment_id": amendment_id,
                "amendment_code": code_str,
                "amendment_number": amendment_number,
                "year": year,
                "amendment_type": amendment_type,
                "author_name": author_name,
                "locality": locality,
                "function": function_name,
                "program": program,
                "program_code": program_code,
                "action": action,
                "action_code": action_code,
                "amount_committed": amount_committed,
                "amount_settled": amount_settled,
                "amount_paid": amount_paid,
                "source": "siop",
            })

            # Author linkage — only if CPF is present (11 digits)
            author_digits = strip_document(author_doc)
            if len(author_digits) == 11 and author_name:
                cpf_formatted = format_cpf(author_doc)
                authors.append({
                    "cpf": cpf_formatted,
                    "name": author_name,
                })
                author_rels.append({
                    "source_key": cpf_formatted,
                    "target_key": amendment_id,
                })

        self.amendments = deduplicate_rows(amendments, ["amendment_id"])
        self.authors = deduplicate_rows(authors, ["cpf"])
        self.author_rels = author_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # 1. Amendment nodes
        if self.amendments:
            loader.load_nodes("Amendment", self.amendments, key_field="amendment_id")

        # 2. Person nodes for authors with CPF
        if self.authors:
            loader.load_nodes("Person", self.authors, key_field="cpf")

        # 3. Person -[:AUTOR_EMENDA]-> Amendment
        if self.author_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (a:Amendment {amendment_id: row.target_key}) "
                "MERGE (p)-[:AUTOR_EMENDA]->(a)"
            )
            loader.run_query_with_retry(query, self.author_rels)


# ── Download / fetch-to-disk (for scripts/download_siop.py) ──────────


def _default_years() -> list[int]:
    """Return the last 5 completed calendar years (inclusive of current).

    Kept as a module-level helper so the Fiscal Cidadao wrapper and the
    pipeline share a single default window definition.
    """
    from datetime import date

    current = date.today().year
    return list(range(current - 4, current + 1))


def _download_yearly_zip(
    year: int,
    raw_dir: Path,
    *,
    skip_existing: bool,
    timeout: int,
) -> Path | None:
    """Download the yearly ``emendas-parlamentares`` ZIP from the CGU.

    Returns the ZIP path on success, ``None`` on HTTP failure. Uses the
    shared ``_download_utils`` helper (already used by ``download_siop.py``)
    for resume/Range support, which matters for the 100+ MB archives.
    """
    import sys

    # Import lazily so the module does not require httpx at import time
    # (pipelines loaded by the Neo4j runner only need pandas).
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    from _download_utils import download_file  # type: ignore[import-not-found]

    url = f"{_TRANSPARENCIA_BASE_URL}/{_TRANSPARENCIA_DATASET}/{year}"
    zip_path = raw_dir / f"{_TRANSPARENCIA_DATASET}_{year}.zip"

    if skip_existing and zip_path.exists() and zip_path.stat().st_size > 0:
        logger.info("[siop] skipping existing %s", zip_path.name)
        return zip_path

    if not download_file(url, zip_path, timeout=timeout):
        logger.warning("[siop] download failed for year %s", year)
        return None
    return zip_path


def _extract_main_csv(zip_path: Path, extract_dir: Path) -> Path | None:
    """Extract the main ``EmendasParlamentares.csv`` from the yearly ZIP.

    The ZIP also contains ``_Convenios.csv`` and ``_PorFavorecido.csv``
    which are auxiliary pivots; the SIOP pipeline only consumes the main
    grain (one row per amendment-action), so we return just that file.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            main = next(
                (
                    n for n in names
                    if n.endswith("EmendasParlamentares.csv")
                    or n.endswith("Emendas_Parlamentares.csv")
                ),
                None,
            )
            if main is None:
                # Fallback: first CSV that is not a known auxiliary variant.
                main = next(
                    (
                        n for n in names
                        if n.lower().endswith(".csv")
                        and "_convenios" not in n.lower()
                        and "_porfavorecido" not in n.lower()
                    ),
                    None,
                )
            if main is None:
                logger.warning("[siop] no usable CSV in %s", zip_path.name)
                return None
            zf.extract(main, extract_dir)
            return extract_dir / main
    except zipfile.BadZipFile:
        logger.warning("[siop] bad zip %s — deleting for re-download", zip_path.name)
        zip_path.unlink(missing_ok=True)
        return None


def _normalize_uf_tokens(uf: str) -> tuple[str, ...]:
    """Return the full-name tokens that match the given UF sigla.

    Accepts either a 2-letter sigla (``"GO"``) or the full state name in
    any casing. Returns a tuple of uppercase tokens to match against the
    ``UF`` column of the Portal da Transparencia CSV.
    """
    key = uf.strip().upper()
    if key in _UF_SIGLA_TO_FULLNAMES:
        return _UF_SIGLA_TO_FULLNAMES[key]
    # Allow callers to pass a full name directly (e.g. ``"Goiás"``).
    return (key,)


_UF_SUFFIX_RE = re.compile(r"[-–\s]([A-Z]{2})\s*$")


def _row_matches_uf(row: pd.Series, uf_tokens: tuple[str, ...], uf_sigla: str) -> bool:
    """Return True when an emendas row should be kept for the target UF.

    Signals considered, in priority order:

    1. ``UF`` column (destination of the resource) matches one of the
       full-name tokens. This is the strongest programmatic UF signal in
       this dataset.
    2. Bancada author name contains the UF full name (e.g.
       ``"BANCADA DE GOIAS"``) — covers rows where the destination UF is
       ``"Múltiplo"`` but the authoring bancada is clearly the target UF.
    3. ``Localidade de aplicação do recurso`` ends with ``"- GO"`` — catches
       municipality rows that were misclassified in the UF column.

    Author-level UF linkage for *individual* amendments is not available
    in this dataset (no UF-author column, CPFs often redacted). Callers
    who need that granularity must join SIOP to TSE candidate data
    downstream.
    """
    # 1. Destination UF
    uf_col = str(row.get("UF", "") or "").strip().upper()
    if uf_col and any(token in uf_col for token in uf_tokens):
        return True

    # 2. Bancada author name
    author = str(
        row.get("Nome do Autor da Emenda", "")
        or row.get("AUTOR EMENDA", "")
        or ""
    ).upper()
    if "BANCADA" in author and any(token in author for token in uf_tokens):
        return True

    # 3. Locality suffix (e.g. "GOIANIA - GO")
    loc = str(
        row.get("Localidade de aplicação do recurso", "")
        or row.get("LOCALIDADE", "")
        or ""
    ).upper()
    m = _UF_SUFFIX_RE.search(loc)
    return bool(m and m.group(1) == uf_sigla)


def fetch_to_disk(
    output_dir: Path | str,
    uf: str | None = None,
    years: list[int] | None = None,
    *,
    skip_existing: bool = True,
    timeout: int = 600,
) -> list[Path]:
    """Download Portal da Transparencia emendas-parlamentares CSVs to disk.

    This is the open-data backing source for the SIOP pipeline: SIOP's own
    web UI (siop.planejamento.gov.br) is a QlikView dashboard with no bulk
    export, so the CGU "emendas-parlamentares" dataset — which mirrors the
    same budget execution numbers — is the practical automated feed.

    Writes one ``emendas_<year>.csv`` per requested year into ``output_dir``
    (the location ``SiopPipeline.extract`` globs for ``*.csv``). The CSV
    preserves the native Portal da Transparencia column names (``Código da
    Emenda``, ``Ano da Emenda``, etc.) — the pipeline's ``_resolve_col``
    helper already accepts both naming conventions, so no schema
    translation is necessary.

    Args:
        output_dir: Destination directory. Created if missing. Usually
            ``data/siop``.
        uf: Optional 2-letter sigla (e.g. ``"GO"``) or full state name. When
            set, rows are kept only when the destination UF, bancada-author
            name, or locality suffix indicates the target state. When
            ``None``, the full national CSV is written. Note: the dataset
            does not expose author-UF for individual amendments; this is
            therefore a GO-scoped filter (author+destination heuristic),
            not a strict author-UF filter. Downstream analytics that need
            strict author UF must join SIOP to TSE candidate data.
        years: Optional list of years to download. Defaults to the last 5
            completed years (inclusive of the current year).
        skip_existing: When True (default), reuses ZIPs already under
            ``output_dir/raw`` rather than re-downloading.
        timeout: Per-file HTTP timeout in seconds.

    Returns:
        List of written CSV paths (one per successfully processed year).
        Empty when every year failed to download.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    requested_years = years if years else _default_years()
    uf_tokens: tuple[str, ...] = ()
    uf_sigla = ""
    if uf:
        uf_sigla = uf.strip().upper()[:2] if len(uf.strip()) >= 2 else uf.strip().upper()
        uf_tokens = _normalize_uf_tokens(uf)
        logger.info(
            "[siop] UF filter active: sigla=%s, match_tokens=%s",
            uf_sigla,
            uf_tokens,
        )

    # Portal da Transparência redirects every
    # ``/download-de-dados/emendas-parlamentares/<year>`` URL to the same
    # ``EmendasParlamentares.zip`` — one consolidated CSV covering every
    # year. Download it once (cached under the most recent requested year)
    # and split by ``Ano da Emenda`` locally.
    cache_year = max(requested_years)
    logger.info(
        "[siop] downloading consolidated emendas ZIP (cache key year=%s)",
        cache_year,
    )
    zip_path = _download_yearly_zip(
        cache_year, raw_dir, skip_existing=skip_existing, timeout=timeout,
    )
    if zip_path is None:
        return []

    extract_dir = raw_dir / f"{_TRANSPARENCIA_DATASET}_{cache_year}_extracted"
    csv_path = _extract_main_csv(zip_path, extract_dir)
    if csv_path is None:
        return []

    try:
        df_all = pd.read_csv(
            csv_path,
            sep=";",
            encoding="latin-1",
            dtype=str,
            keep_default_na=False,
        )
    except Exception as e:  # noqa: BLE001 — surface any parse failure
        logger.warning("[siop] failed to read %s: %s", csv_path.name, e)
        return []

    if df_all.empty:
        logger.warning("[siop] consolidated CSV is empty")
        return []

    year_col = next(
        (c for c in df_all.columns if c.strip() == "Ano da Emenda"),
        None,
    )
    if year_col is None:
        logger.warning(
            "[siop] no 'Ano da Emenda' column found; cannot split by year. "
            "Columns: %s", list(df_all.columns)[:10],
        )
        return []

    written: list[Path] = []
    for year in requested_years:
        logger.info("[siop] ---- year %s ----", year)
        df = df_all[df_all[year_col].astype(str).str.strip() == str(year)]
        logger.info(
            "[siop] year %s: %d rows from consolidated CSV", year, len(df),
        )
        if df.empty:
            logger.warning("[siop] year %s: no rows in upstream — skipping", year)
            continue

        if uf_tokens:
            before = len(df)
            mask = df.apply(
                lambda r: _row_matches_uf(r, uf_tokens, uf_sigla),
                axis=1,
            )
            df = df[mask].reset_index(drop=True)
            logger.info(
                "[siop] year %s: filtered %d -> %d rows for UF=%s",
                year, before, len(df), uf_sigla,
            )
            if df.empty:
                logger.warning(
                    "[siop] year %s produced zero rows after UF filter",
                    year,
                )
                continue

        out_path = output_dir / f"emendas_{year}.csv"
        # Keep the native encoding/separator so the pipeline reads it the
        # same way it reads manually-placed files.
        df.to_csv(out_path, index=False, sep=";", encoding="latin-1")
        logger.info("[siop] wrote %d rows to %s", len(df), out_path)
        written.append(out_path)

    return written

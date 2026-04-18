from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    mask_cpf,
    normalize_name,
    parse_number_smart,
    row_pick,
    strip_document,
)
from bracc_etl.transforms import (
    stable_id as _stable_id,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_COMMISSIONED_KEYWORDS = re.compile(
    r"comissionado|comissao|\bDAS\b|\bFCPE\b|\bCC-|\bCDS\b|\bDAI\b",
    re.IGNORECASE,
)

_CKAN_BASE = "https://dadosabertos.go.gov.br/api/3/action"
_PAGE_LIMIT = 5_000
_DEFAULT_DATASET = "folha-de-pagamento"
# CKAN ``datastore_search`` always devolve JSON (mesmo quando o resource
# é CSV); usado como fallback quando o servidor não carimba ``Content-Type``
# explicitamente.
_CKAN_JSON_CONTENT_TYPE = "application/json"
# Coluna privada no DataFrame que carrega a URI do snapshot archival
# por-linha (prefixo duplo underscore não colide com nomes reais do CKAN).
# ``transform`` lê essa coluna pra popular ``source_snapshot_uri`` em
# cada ``attach_provenance`` — e a filtra de volta antes de chegar ao
# Neo4jBatchLoader, porque archival é opt-in e não faz parte do schema
# dos nós StateEmployee/StateAgency.
_SNAPSHOT_COLUMN = "__snapshot_uri"
# The pipeline's offline fallback in ``extract`` reads this filename first;
# keeping the downloader aligned avoids drift between ``fetch_to_disk`` and
# the data_dir layout expected by the ETL runner. This name is used when a
# single ``resource_id`` is pinned via CLI; the historical multi-resource
# mode writes ``servidores_<period>.csv`` per snapshot (see
# ``fetch_to_disk`` and ``_period_slug_from_name``).
_DEFAULT_OUTPUT_FILENAME = "servidores.csv"

# Portuguese month names (as they appear in CKAN resource names like
# "Folha de Pagamento - Dezembro/2025") mapped to zero-padded numeric
# month for deterministic ``servidores_<period>.csv`` output filenames.
_PT_MONTHS = {
    "janeiro": "01",
    "fevereiro": "02",
    "marco": "03",
    "março": "03",
    "abril": "04",
    "maio": "05",
    "junho": "06",
    "julho": "07",
    "agosto": "08",
    "setembro": "09",
    "outubro": "10",
    "novembro": "11",
    "dezembro": "12",
}
_PERIOD_RE = re.compile(
    r"(janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|"
    r"setembro|outubro|novembro|dezembro)\s*/\s*(\d{4})",
    re.IGNORECASE,
)


def _is_commissioned(role: str) -> bool:
    """Check if a role/position is a commissioned position."""
    return bool(_COMMISSIONED_KEYWORDS.search(role))


def _period_slug_from_name(name: str) -> str | None:
    """Extract a ``YYYY-MM`` slug from a CKAN resource name if possible.

    Example: ``"Folha de Pagamento - Dezembro/2025"`` -> ``"2025-12"``.
    Returns ``None`` when the name does not contain a recognizable
    ``<month>/<year>`` token; callers should fall back to the short
    resource id for disambiguation.
    """
    match = _PERIOD_RE.search(name or "")
    if not match:
        return None
    month_key = match.group(1).lower().replace("ç", "c")
    month = _PT_MONTHS.get(month_key)
    if not month:
        return None
    return f"{match.group(2)}-{month}"


def _discover_resource_id(dataset_name: str = _DEFAULT_DATASET) -> str | None:
    """Return the most recent datastore-active CSV resource id for a dataset.

    CKAN lists the PDF data dictionary as the first resource, which has
    ``datastore_active=False``. Pick the first CSV whose datastore is
    active — that is the latest monthly payroll snapshot.

    Module-level so both the pipeline and the ``download_folha_go`` CLI
    wrapper share one discovery path. Kept for the single-snapshot /
    offline fallback path; use ``_discover_all_resources`` to enumerate
    every monthly CSV resource available in the datastore.
    """
    resources = _discover_all_resources(dataset_name)
    if not resources:
        return None
    return resources[0][0]


def _discover_all_resources(
    dataset_name: str = _DEFAULT_DATASET,
) -> list[tuple[str, str | None]]:
    """Return every datastore-active CSV resource as ``(id, period_slug)``.

    The CKAN ``folha-de-pagamento`` dataset exposes one resource per
    monthly payroll snapshot (plus yearly ZIP archives that we skip —
    their ``datastore_active`` is ``False`` so they cannot be paged via
    ``datastore_search``). This helper returns every resource eligible
    for ``datastore_search`` pagination, ordered as the CKAN API lists
    them (most recent first).

    Period slug is extracted from the resource name (``YYYY-MM``); it is
    ``None`` when the name has no recognizable month/year, in which case
    ``fetch_to_disk`` falls back to the short resource id.
    """
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{_CKAN_BASE}/package_show",
                params={"id": dataset_name},
            )
            resp.raise_for_status()
            resources = resp.json().get("result", {}).get("resources", [])
    except (httpx.HTTPError, KeyError, IndexError):
        logger.warning(
            "[folha_go] Could not discover resources for %s", dataset_name,
        )
        return []

    out: list[tuple[str, str | None]] = []
    for r in resources:
        if (
            r.get("datastore_active")
            and str(r.get("format", "")).upper() == "CSV"
        ):
            period = _period_slug_from_name(str(r.get("name") or ""))
            out.append((str(r["id"]), period))
    return out


def _fetch_ckan_records(
    resource_id: str,
    limit: int | None = None,
    *,
    run_id: str | None = None,
    source_id: str | None = None,
) -> tuple[list[dict[str, Any]], list[str | None]]:
    """Fetch all records from a CKAN datastore resource with pagination.

    Stops early when ``limit`` records are accumulated, when a page returns
    no records, or when a page is shorter than requested (end-of-dataset).

    When both ``run_id`` and ``source_id`` are provided, each raw page
    payload is persisted via :func:`bracc_etl.archival.archive_fetch` and
    the returned URI is replicated per record so ``transform`` can carimbar
    ``source_snapshot_uri`` nas rows. Omitir ambos (ex.: caminho do CLI de
    download) desliga archival — o comportamento original é preservado
    e o segundo elemento da tupla fica cheio de ``None``.
    """
    records: list[dict[str, Any]] = []
    snapshot_uris: list[str | None] = []
    offset = 0
    archival_enabled = bool(run_id and source_id)

    with httpx.Client(timeout=60) as client:
        while limit is None or len(records) < limit:
            remaining = (
                _PAGE_LIMIT
                if limit is None
                else min(_PAGE_LIMIT, limit - len(records))
            )
            resp = client.get(
                f"{_CKAN_BASE}/datastore_search",
                params={
                    "resource_id": resource_id,
                    "limit": remaining,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            page_uri: str | None = None
            if archival_enabled:
                # Archival é content-addressed: mesmo payload → mesma URI
                # → sem re-escrita. Seguro chamar a cada página.
                content_type = resp.headers.get(
                    "content-type", _CKAN_JSON_CONTENT_TYPE,
                )
                page_uri = archive_fetch(
                    url=str(resp.request.url),
                    content=resp.content,
                    content_type=content_type,
                    run_id=run_id,  # type: ignore[arg-type]
                    source_id=source_id,  # type: ignore[arg-type]
                )
            result = resp.json().get("result", {})
            page_records = result.get("records", [])
            if not page_records:
                break
            records.extend(page_records)
            snapshot_uris.extend([page_uri] * len(page_records))
            offset += len(page_records)
            if len(page_records) < remaining:
                break

    return records, snapshot_uris


def _records_to_dataframe(
    records: list[dict[str, Any]],
    snapshot_uris: list[str | None] | None = None,
) -> pd.DataFrame:
    """Convert CKAN datastore records into a DataFrame matching row_pick keys.

    When ``snapshot_uris`` is provided (same length as ``records``), it is
    attached as the hidden ``_SNAPSHOT_COLUMN`` column so ``transform`` pode
    ler o URI da snapshot por-linha e carimbar em ``attach_provenance``.
    Rows sem URI (ex.: offline/fixture path) simplesmente ficam com o valor
    ``None`` nessa coluna.
    """
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records).astype(str)
    # Normalize CKAN column names to match transform's row_pick keys.
    df.columns = df.columns.str.lower()
    df = df.rename(columns={
        "nomeservidor": "nome",
        "nomecargo": "cargo",
        "valorprovento": "remuneracao_bruta",
        "valorliquido": "salario_liquido",
        "codorgao": "orgao_codigo",
        "anomes": "periodo",
    })
    if snapshot_uris is not None and len(snapshot_uris) == len(df):
        # Usa object dtype pra preservar ``None`` em vez de virar ``"None"``.
        df[_SNAPSHOT_COLUMN] = pd.array(snapshot_uris, dtype="object")
    return df


def _write_resource_to_disk(
    resource_id: str,
    output_path: Path,
    limit: int | None,
) -> Path | None:
    """Paginate a single CKAN resource and write it to ``output_path``.

    Returns the written path on success, or ``None`` if the resource
    returned no records or pagination failed (logged, not raised, so the
    multi-resource loop in ``fetch_to_disk`` can keep going).
    """
    logger.info(
        "[folha_go] fetching CKAN resource_id=%s -> %s (limit=%s)",
        resource_id,
        output_path.name,
        limit,
    )
    try:
        # CLI path — archival desativado aqui porque os CSVs gravados em
        # disco são usados como cache intermediário pelo ``extract``, que
        # por sua vez roda (re-roda) archival quando cai no fallback
        # online. Archival na camada do CLI seria duplicado e ainda
        # precisaria de um ``run_id`` sintético que não casa com o run
        # do pipeline.
        records, _snapshot_uris = _fetch_ckan_records(resource_id, limit=limit)
    except httpx.HTTPError as exc:
        logger.error(
            "[folha_go] CKAN datastore_search failed for %s: %s",
            resource_id,
            exc,
        )
        return None

    if not records:
        logger.warning(
            "[folha_go] CKAN returned no records for resource %s", resource_id,
        )
        return None

    df = _records_to_dataframe(records)
    df.to_csv(output_path, index=False)
    logger.info(
        "[folha_go] wrote %s (%d records, %d columns)",
        output_path,
        len(df),
        len(df.columns),
    )
    return output_path


def fetch_to_disk(
    output_dir: Path,
    limit: int | None = None,
    resource_id: str | None = None,
    resource_limit: int | None = None,
) -> list[Path]:
    """Download Goias state payroll (``folha-de-pagamento``) CKAN data to disk.

    When ``resource_id`` is supplied, paginates that single resource and
    writes ``servidores.csv`` (the legacy single-snapshot layout kept for
    fixtures and offline fallbacks).

    When ``resource_id`` is ``None``, enumerates **every** datastore-active
    CSV resource exposed by the dataset and writes one file per snapshot
    named ``servidores_<YYYY-MM>.csv`` (or ``servidores_<short-id>.csv``
    when the resource name has no recognizable month/year). This lets
    the ETL ingest the full historical payroll, not just the latest
    monthly file. The pipeline's ``extract`` globs ``servidores*.csv``
    under ``data_dir/folha_go`` and concatenates, so both layouts coexist.

    Args:
        output_dir: Directory to write into. Created if missing.
        limit: Optional row cap **per resource** (applied during pagination,
            for smoke tests — not a global cap across all snapshots).
        resource_id: Optional CKAN resource id override. If ``None``,
            downloads all datastore-active CSV resources for the
            ``folha-de-pagamento`` dataset.
        resource_limit: Optional cap on the number of resources to fetch
            when iterating the full dataset. Defaults to ``None`` (fetch
            all). Useful for smoke tests and CI.

    Returns:
        List of files written (one per successfully downloaded resource).
        Empty when discovery found nothing or every request failed.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Single-resource mode: preserves the legacy ``servidores.csv``
    # filename so test fixtures and pinned historical downloads keep
    # working unchanged.
    if resource_id is not None:
        target = output_dir / _DEFAULT_OUTPUT_FILENAME
        written = _write_resource_to_disk(resource_id, target, limit)
        return [written] if written else []

    # Multi-resource mode: iterate every datastore-active CSV snapshot.
    resources = _discover_all_resources()
    if not resources:
        logger.error(
            "[folha_go] could not discover any CKAN resources for %s",
            _DEFAULT_DATASET,
        )
        return []

    if resource_limit is not None and resource_limit >= 0:
        resources = resources[:resource_limit]

    logger.info(
        "[folha_go] discovered %d datastore-active CSV resource(s); "
        "downloading (per-resource row limit=%s)",
        len(resources),
        limit,
    )

    written_paths: list[Path] = []
    seen_names: set[str] = set()
    for rid, period in resources:
        if period:
            filename = f"servidores_{period}.csv"
        else:
            filename = f"servidores_{rid[:8]}.csv"
        # Defensive: two resources could theoretically map to the same
        # slug (e.g. corrected re-uploads). Disambiguate by appending
        # the short id so we never silently overwrite another snapshot.
        if filename in seen_names:
            filename = f"servidores_{period or 'x'}_{rid[:8]}.csv"
        seen_names.add(filename)

        target = output_dir / filename
        written = _write_resource_to_disk(rid, target, limit)
        if written:
            written_paths.append(written)

    return written_paths


class FolhaGoPipeline(Pipeline):
    """ETL pipeline for Goias state payroll and commissioned positions data."""

    name = "folha_go"
    source_id = "folha_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)

        self._raw_servidores: pd.DataFrame = pd.DataFrame()

        self.employees: list[dict[str, Any]] = []
        self.agencies: list[dict[str, Any]] = []
        self.employee_agency_rels: list[dict[str, Any]] = []

    def _read_df_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    def _fetch_ckan_resource(self, resource_id: str) -> pd.DataFrame:
        """Fetch all records from a CKAN datastore resource using pagination.

        Thin delegator around the module-level helpers so the online
        fallback in ``extract`` and the ``fetch_to_disk`` CLI wrapper share
        one HTTP/pagination implementation.

        Ativa archival: cada página paginada é persistida via
        ``archive_fetch`` e a URI retorna anexada como coluna
        ``_SNAPSHOT_COLUMN`` no DataFrame, pra ``transform`` carimbar
        ``source_snapshot_uri`` por-linha. Offline-path (CSVs locais em
        ``data_dir/folha_go``) não passa por aqui e, portanto, não ganha
        URI — consistente com o caráter opt-in do campo.
        """
        records, snapshot_uris = _fetch_ckan_records(
            resource_id,
            limit=self.limit,
            run_id=self.run_id,
            source_id=self.source_id,
        )
        return _records_to_dataframe(records, snapshot_uris=snapshot_uris)

    def _discover_resource_id(self, dataset_name: str) -> str | None:
        """Instance delegator kept for backwards compatibility."""
        return _discover_resource_id(dataset_name)

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "folha_go"

        # Try local files first (fallback / offline mode). Glob every
        # ``servidores*.csv`` under the source directory so the new
        # multi-snapshot layout (``servidores_2025-12.csv``, ...) and
        # the legacy single ``servidores.csv`` both work without any
        # config change. Sorted for deterministic concatenation order.
        frames: list[pd.DataFrame] = []
        if src_dir.exists():
            csv_paths = sorted(src_dir.glob("servidores*.csv"))
            for path in csv_paths:
                df = self._read_df_optional(path)
                if not df.empty:
                    frames.append(df)
            # Legacy parquet fallback — only consulted if no CSV was found.
            if not frames:
                parquet_df = self._read_df_optional(src_dir / "servidores.parquet")
                if not parquet_df.empty:
                    frames.append(parquet_df)

        if frames:
            self._raw_servidores = pd.concat(frames, ignore_index=True)
        else:
            self._raw_servidores = pd.DataFrame()

        # If no local files, try CKAN API (single latest snapshot —
        # online full-history downloads should go through ``fetch_to_disk``
        # so the on-disk layout under ``data_dir/folha_go`` is reused
        # for incremental runs instead of re-paging every resource each
        # time ``extract`` is called).
        if self._raw_servidores.empty:
            logger.info("[folha_go] No local files found, trying CKAN API...")
            resource_id = _discover_resource_id(_DEFAULT_DATASET)
            if resource_id:
                try:
                    self._raw_servidores = self._fetch_ckan_resource(resource_id)
                except httpx.HTTPError as exc:
                    logger.error("[folha_go] CKAN API request failed: %s", exc)

        if self._raw_servidores.empty:
            logger.warning("[folha_go] No input data found in %s or CKAN API", src_dir)
            return

        if self.limit:
            self._raw_servidores = self._raw_servidores.head(self.limit)

        self.rows_in = len(self._raw_servidores)
        logger.info("[folha_go] extracted servidores=%d", len(self._raw_servidores))

    def transform(self) -> None:
        if self._raw_servidores.empty:
            return

        employees: list[dict[str, Any]] = []
        agencies: list[dict[str, Any]] = []
        employee_agency_rels: list[dict[str, Any]] = []
        seen_agencies: set[str] = set()

        # Snapshot URI por-linha só aparece quando o extract passou pelo
        # fallback online (``_fetch_ckan_resource``). Offline/fixture path
        # → coluna ausente → ``snapshot_uri`` permanece ``None`` e o campo
        # fica fora do row stamped (compat com contrato opt-in).
        has_snapshot_col = _SNAPSHOT_COLUMN in self._raw_servidores.columns

        for _, row in self._raw_servidores.iterrows():
            snapshot_uri: str | None = None
            if has_snapshot_col:
                raw_uri = row.get(_SNAPSHOT_COLUMN)
                if isinstance(raw_uri, str) and raw_uri:
                    snapshot_uri = raw_uri

            name = normalize_name(
                row_pick(row, "nome", "nome_servidor", "servidor", "name"),
            )
            cpf_raw = row_pick(row, "cpf", "nr_cpf", "documento")
            role = row_pick(
                row,
                "cargo",
                "cargo_efetivo",
                "funcao",
                "cargo_comissionado",
                "role",
            )
            agency_name = normalize_name(
                row_pick(row, "orgao", "orgao_lotacao", "lotacao", "agency", "unidade"),
            )
            salary_gross = parse_number_smart(
                row_pick(
                    row,
                    "remuneracao_bruta",
                    "salario_bruto",
                    "vencimento_bruto",
                    "salary_gross",
                ),
                default=None,
            )
            salary_net = parse_number_smart(
                row_pick(
                    row,
                    "remuneracao_liquida",
                    "salario_liquido",
                    "vencimento_liquido",
                    "salary_net",
                ),
                default=None,
            )
            municipality = row_pick(row, "municipio", "cidade", "municipality")
            is_commissioned = _is_commissioned(role)

            # Stable employee ID from name + CPF (last 4) + role + agency
            cpf_digits = strip_document(cpf_raw)
            cpf_suffix = cpf_digits[-4:] if len(cpf_digits) >= 4 else cpf_digits
            employee_id = _stable_id(name, cpf_suffix, role, agency_name)

            # Mask CPF for LGPD
            cpf_masked = mask_cpf(cpf_raw) if cpf_digits else ""

            employee_record_id = f"{name}|{role}|{agency_name}"
            employees.append(self.attach_provenance(
                {
                    "employee_id": employee_id,
                    "name": name,
                    "cpf": cpf_masked,
                    "role": role,
                    "agency": agency_name,
                    "salary_gross": salary_gross,
                    "salary_net": salary_net,
                    "is_commissioned": is_commissioned,
                    "uf": "GO",
                    "municipality": municipality,
                    "source": "folha_go",
                },
                record_id=employee_record_id,
                snapshot_uri=snapshot_uri,
            ))

            # Build agency node
            if agency_name and agency_name not in seen_agencies:
                agency_id = _stable_id(agency_name, "GO")
                agencies.append(self.attach_provenance(
                    {
                        "agency_id": agency_id,
                        "name": agency_name,
                        "uf": "GO",
                        "source": "folha_go",
                    },
                    record_id=agency_name,
                    snapshot_uri=snapshot_uri,
                ))
                seen_agencies.add(agency_name)

            # Build employee -> agency relationship
            if agency_name:
                agency_id = _stable_id(agency_name, "GO")
                employee_agency_rels.append(self.attach_provenance(
                    {
                        "source_key": employee_id,
                        "target_key": agency_id,
                    },
                    record_id=employee_record_id,
                    snapshot_uri=snapshot_uri,
                ))

        self.employees = deduplicate_rows(employees, ["employee_id"])
        self.agencies = deduplicate_rows(agencies, ["agency_id"])
        self.employee_agency_rels = deduplicate_rows(
            employee_agency_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = len(self.employees)

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.employees:
            loader.load_nodes("StateEmployee", self.employees, key_field="employee_id")

        if self.agencies:
            loader.load_nodes("StateAgency", self.agencies, key_field="agency_id")

        if self.employee_agency_rels:
            loader.load_relationships(
                rel_type="LOTADO_EM",
                rows=self.employee_agency_rels,
                source_label="StateEmployee",
                source_key="employee_id",
                target_label="StateAgency",
                target_key="agency_id",
            )

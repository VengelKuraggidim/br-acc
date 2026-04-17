from __future__ import annotations

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
    mask_cpf,
    normalize_name,
    parse_number_smart,
    row_pick,
    stable_id as _stable_id,
    strip_document,
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
# The pipeline's offline fallback in ``extract`` reads this filename first;
# keeping the downloader aligned avoids drift between ``fetch_to_disk`` and
# the data_dir layout expected by the ETL runner.
_DEFAULT_OUTPUT_FILENAME = "servidores.csv"


def _is_commissioned(role: str) -> bool:
    """Check if a role/position is a commissioned position."""
    return bool(_COMMISSIONED_KEYWORDS.search(role))


def _discover_resource_id(dataset_name: str = _DEFAULT_DATASET) -> str | None:
    """Return the most recent datastore-active CSV resource id for a dataset.

    CKAN lists the PDF data dictionary as the first resource, which has
    ``datastore_active=False``. Pick the first CSV whose datastore is
    active — that is the latest monthly payroll snapshot.

    Module-level so both the pipeline and the ``download_folha_go`` CLI
    wrapper share one discovery path.
    """
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{_CKAN_BASE}/package_show",
                params={"id": dataset_name},
            )
            resp.raise_for_status()
            resources = resp.json().get("result", {}).get("resources", [])
            for r in resources:
                if (
                    r.get("datastore_active")
                    and str(r.get("format", "")).upper() == "CSV"
                ):
                    return str(r["id"])
    except (httpx.HTTPError, KeyError, IndexError):
        logger.warning("[folha_go] Could not discover resource for %s", dataset_name)
    return None


def _fetch_ckan_records(
    resource_id: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch all records from a CKAN datastore resource with pagination.

    Stops early when ``limit`` records are accumulated, when a page returns
    no records, or when a page is shorter than requested (end-of-dataset).
    """
    records: list[dict[str, Any]] = []
    offset = 0

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
            result = resp.json().get("result", {})
            page_records = result.get("records", [])
            if not page_records:
                break
            records.extend(page_records)
            offset += len(page_records)
            if len(page_records) < remaining:
                break

    return records


def _records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert CKAN datastore records into a DataFrame matching row_pick keys."""
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
    return df


def fetch_to_disk(
    output_dir: Path,
    limit: int | None = None,
    resource_id: str | None = None,
) -> list[Path]:
    """Download Goias state payroll (``folha-de-pagamento``) CKAN data to disk.

    Discovers the latest datastore-active CSV resource via
    ``package_show`` (unless ``resource_id`` is supplied), paginates the
    datastore, normalizes CKAN column names, and writes the result as
    ``servidores.csv`` inside ``output_dir`` — the exact filename the
    pipeline's offline fallback in ``extract`` looks for.

    Args:
        output_dir: Directory to write into. Created if missing.
        limit: Optional row cap (applied during pagination, for smoke tests).
        resource_id: Optional CKAN resource id override. If ``None``,
            discovers the latest active CSV for the
            ``folha-de-pagamento`` dataset.

    Returns:
        List of files written (one entry on success, empty if the CKAN
        API returned no records or the resource could not be discovered).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if resource_id is None:
        resource_id = _discover_resource_id()
        if resource_id is None:
            logger.error(
                "[folha_go] could not discover CKAN resource id for %s",
                _DEFAULT_DATASET,
            )
            return []

    logger.info(
        "[folha_go] fetching CKAN resource_id=%s limit=%s",
        resource_id,
        limit,
    )
    try:
        records = _fetch_ckan_records(resource_id, limit=limit)
    except httpx.HTTPError as exc:
        logger.error("[folha_go] CKAN datastore_search failed: %s", exc)
        return []

    if not records:
        logger.warning(
            "[folha_go] CKAN returned no records for resource %s", resource_id
        )
        return []

    df = _records_to_dataframe(records)
    target = output_dir / _DEFAULT_OUTPUT_FILENAME
    df.to_csv(target, index=False)
    logger.info(
        "[folha_go] wrote %s (%d records, %d columns)",
        target,
        len(df),
        len(df.columns),
    )
    return [target]


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
        """
        records = _fetch_ckan_records(resource_id, limit=self.limit)
        return _records_to_dataframe(records)

    def _discover_resource_id(self, dataset_name: str) -> str | None:
        """Instance delegator kept for backwards compatibility."""
        return _discover_resource_id(dataset_name)

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "folha_go"

        # Try local files first (fallback / offline mode)
        self._raw_servidores = self._read_df_optional(src_dir / "servidores.csv")
        if self._raw_servidores.empty:
            self._raw_servidores = self._read_df_optional(src_dir / "servidores.parquet")

        # If no local files, try CKAN API
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

        for _, row in self._raw_servidores.iterrows():
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

            employees.append({
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
            })

            # Build agency node
            if agency_name and agency_name not in seen_agencies:
                agency_id = _stable_id(agency_name, "GO")
                agencies.append({
                    "agency_id": agency_id,
                    "name": agency_name,
                    "uf": "GO",
                    "source": "folha_go",
                })
                seen_agencies.add(agency_name)

            # Build employee -> agency relationship
            if agency_name:
                agency_id = _stable_id(agency_name, "GO")
                employee_agency_rels.append({
                    "source_key": employee_id,
                    "target_key": agency_id,
                })

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

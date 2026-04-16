from __future__ import annotations

import hashlib
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
    normalize_name,
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


def _stable_id(*parts: str, length: int = 24) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"[^0-9,.-]", "", text)
    if "," in text and "." in text and text.rfind(",") > text.rfind("."):
        text = text.replace(".", "").replace(",", ".")
    elif "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _pick(row: pd.Series, *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "")).strip()
        if value and value.lower() not in ("nan", "none", ""):
            return value
    return ""


def mask_cpf(cpf: str) -> str:
    """Mask CPF for LGPD compliance, showing only last 4 digits."""
    digits = strip_document(cpf)
    if len(digits) != 11:
        return "***.***.***-**"
    return f"***.***.*{digits[7]}{digits[8]}-{digits[9]}{digits[10]}"


def _is_commissioned(role: str) -> bool:
    """Check if a role/position is a commissioned position."""
    return bool(_COMMISSIONED_KEYWORDS.search(role))


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
        """Fetch all records from a CKAN datastore resource using pagination."""
        records: list[dict[str, Any]] = []
        offset = 0
        total_limit = self.limit or float("inf")

        with httpx.Client(timeout=60) as client:
            while len(records) < total_limit:
                page_size = min(_PAGE_LIMIT, int(total_limit) - len(records))
                resp = client.get(
                    f"{_CKAN_BASE}/datastore_search",
                    params={
                        "resource_id": resource_id,
                        "limit": page_size,
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
                if len(page_records) < page_size:
                    break

        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records).astype(str)

    def _discover_resource_id(self, dataset_name: str) -> str | None:
        """Search for a resource_id by dataset name via CKAN API."""
        try:
            with httpx.Client(timeout=30) as client:
                resp = client.get(
                    f"{_CKAN_BASE}/package_show",
                    params={"id": dataset_name},
                )
                resp.raise_for_status()
                resources = resp.json().get("result", {}).get("resources", [])
                if resources:
                    return resources[0]["id"]
        except (httpx.HTTPError, KeyError, IndexError):
            logger.warning("[folha_go] Could not discover resource for %s", dataset_name)
        return None

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "folha_go"

        # Try local files first (fallback / offline mode)
        self._raw_servidores = self._read_df_optional(src_dir / "servidores.csv")
        if self._raw_servidores.empty:
            self._raw_servidores = self._read_df_optional(src_dir / "servidores.parquet")

        # If no local files, try CKAN API
        if self._raw_servidores.empty:
            logger.info("[folha_go] No local files found, trying CKAN API...")
            resource_id = self._discover_resource_id("folha-de-pagamento")
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
                _pick(row, "nome", "nome_servidor", "servidor", "name"),
            )
            cpf_raw = _pick(row, "cpf", "nr_cpf", "documento")
            role = _pick(
                row,
                "cargo",
                "cargo_efetivo",
                "funcao",
                "cargo_comissionado",
                "role",
            )
            agency_name = normalize_name(
                _pick(row, "orgao", "orgao_lotacao", "lotacao", "agency", "unidade"),
            )
            salary_gross = _to_float(
                _pick(row, "remuneracao_bruta", "salario_bruto", "vencimento_bruto", "salary_gross"),
            )
            salary_net = _to_float(
                _pick(row, "remuneracao_liquida", "salario_liquido", "vencimento_liquido", "salary_net"),
            )
            municipality = _pick(row, "municipio", "cidade", "municipality")
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

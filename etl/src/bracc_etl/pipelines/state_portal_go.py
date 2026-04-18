"""ETL pipeline for the Goias state transparency portal (dados abertos GO).

Ingests state-level data published by the Secretaria da Administracao (SEAD)
via the CKAN portal at https://dadosabertos.go.gov.br. Initial scope covers:

- ``contratos``         -> GoStateContract nodes + CONTRATOU_ESTADO_GO rels
- ``fornecedores``      -> GoStateSupplier nodes (suppliers registered with GO)
- ``licitantes-sancionados-administrativamente`` -> GoStateSanction nodes
  + SANCIONADO_GO rels

The CKAN API supports paginated retrieval via ``datastore_search`` but several
SEAD resources are file-only (CSV downloads). The pipeline prefers local CSV
files under ``data/state_portal_go/*.csv`` and falls back to fetching the
latest monthly resource via CKAN when offline files are missing.

Data source: https://dadosabertos.go.gov.br/
"""

from __future__ import annotations

import hashlib
import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    cap_contract_value,
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_brl_flexible,
    parse_date,
    row_pick,
    strip_document,
)
from bracc_etl.transforms import stable_id as _stable_id

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_CKAN_BASE = "https://dadosabertos.go.gov.br/api/3/action"
_HTTP_TIMEOUT = 60

# Datasets we pull from the CKAN portal. Values are CKAN package IDs.
_DATASETS = {
    "contratos": "contratos",
    "fornecedores": "fornecedores",
    "sancoes": "licitantes-sancionados-administrativamente",
}


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


class StatePortalGoPipeline(Pipeline):
    """ETL pipeline for the Goias state transparency portal (dadosabertos.go.gov.br)."""

    name = "state_portal_go"
    source_id = "state_portal_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_contracts: pd.DataFrame = pd.DataFrame()
        self._raw_suppliers: pd.DataFrame = pd.DataFrame()
        self._raw_sanctions: pd.DataFrame = pd.DataFrame()

        self.contracts: list[dict[str, Any]] = []
        self.suppliers: list[dict[str, Any]] = []
        self.sanctions: list[dict[str, Any]] = []
        self.contract_rels: list[dict[str, Any]] = []
        self.sanction_rels: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        for sep in (";", ","):
            try:
                df = pd.read_csv(
                    path,
                    sep=sep,
                    dtype=str,
                    keep_default_na=False,
                    encoding="utf-8",
                    engine="python",
                    on_bad_lines="skip",
                )
                if len(df.columns) > 1:
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        try:
            return pd.read_csv(
                path,
                sep=";",
                dtype=str,
                keep_default_na=False,
                encoding="latin-1",
                engine="python",
                on_bad_lines="skip",
            )
        except (OSError, pd.errors.ParserError) as exc:
            logger.warning("[state_portal_go] Failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def _read_local_folder(self, subname: str) -> pd.DataFrame:
        """Read and concatenate all CSVs under data/state_portal_go/<subname>*.csv."""
        src_dir = Path(self.data_dir) / "state_portal_go"
        if not src_dir.exists():
            return pd.DataFrame()
        files = sorted(src_dir.glob(f"{subname}*.csv"))
        frames: list[pd.DataFrame] = []
        for f in files:
            df = self._read_csv_optional(f)
            if not df.empty:
                frames.append(df)
                logger.info(
                    "[state_portal_go] loaded %d rows from %s", len(df), f.name,
                )
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _latest_resource_url(self, client: httpx.Client, package_id: str) -> str | None:
        """Ask CKAN for a package and return the URL of the most recent CSV resource."""
        try:
            resp = client.get(
                f"{_CKAN_BASE}/package_show",
                params={"id": package_id},
            )
            resp.raise_for_status()
            body = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning(
                "[state_portal_go] CKAN package_show failed for %s: %s",
                package_id, exc,
            )
            return None

        resources = body.get("result", {}).get("resources", [])
        csv_resources = [
            r for r in resources
            if str(r.get("format", "")).lower() == "csv" and r.get("url")
        ]
        if not csv_resources:
            return None
        csv_resources.sort(
            key=lambda r: str(r.get("created", "")),
            reverse=True,
        )
        return str(csv_resources[0]["url"])

    def _fetch_ckan_csv(self, package_id: str) -> pd.DataFrame:
        with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
            url = self._latest_resource_url(client, package_id)
            if not url:
                return pd.DataFrame()
            logger.info("[state_portal_go] fetching %s -> %s", package_id, url)
            try:
                resp = client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning(
                    "[state_portal_go] download failed for %s: %s", url, exc,
                )
                return pd.DataFrame()

            content = resp.content
            for encoding in ("utf-8", "latin-1"):
                for sep in (";", ","):
                    try:
                        df = pd.read_csv(
                            io.BytesIO(content),
                            sep=sep,
                            dtype=str,
                            keep_default_na=False,
                            encoding=encoding,
                            engine="python",
                            on_bad_lines="skip",
                        )
                        if len(df.columns) > 1:
                            return df
                    except (UnicodeDecodeError, pd.errors.ParserError):
                        continue
            return pd.DataFrame()

    def extract(self) -> None:
        # Try local files first (canonical fallback used by bootstrap scripts).
        self._raw_contracts = self._read_local_folder("contratos")
        self._raw_suppliers = self._read_local_folder("fornecedores")
        self._raw_sanctions = self._read_local_folder("sancoes")
        if self._raw_sanctions.empty:
            self._raw_sanctions = self._read_local_folder(
                "licitantes-sancionados",
            )

        # Fall back to CKAN API when local files are missing.
        if self._raw_contracts.empty:
            self._raw_contracts = self._fetch_ckan_csv(_DATASETS["contratos"])
        if self._raw_suppliers.empty:
            self._raw_suppliers = self._fetch_ckan_csv(_DATASETS["fornecedores"])
        if self._raw_sanctions.empty:
            self._raw_sanctions = self._fetch_ckan_csv(_DATASETS["sancoes"])

        if self.limit:
            self._raw_contracts = self._raw_contracts.head(self.limit)
            self._raw_suppliers = self._raw_suppliers.head(self.limit)
            self._raw_sanctions = self._raw_sanctions.head(self.limit)

        self.rows_in = (
            len(self._raw_contracts)
            + len(self._raw_suppliers)
            + len(self._raw_sanctions)
        )
        logger.info(
            "[state_portal_go] extracted contracts=%d suppliers=%d sanctions=%d",
            len(self._raw_contracts),
            len(self._raw_suppliers),
            len(self._raw_sanctions),
        )

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        self._transform_contracts()
        self._transform_suppliers()
        self._transform_sanctions()

        self.contracts = deduplicate_rows(self.contracts, ["contract_id"])
        self.suppliers = deduplicate_rows(self.suppliers, ["cnpj"])
        self.sanctions = deduplicate_rows(self.sanctions, ["sanction_id"])
        self.contract_rels = deduplicate_rows(
            self.contract_rels, ["source_key", "target_key"],
        )
        self.sanction_rels = deduplicate_rows(
            self.sanction_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = (
            len(self.contracts) + len(self.suppliers) + len(self.sanctions)
        )

    def _transform_contracts(self) -> None:
        if self._raw_contracts.empty:
            return
        for _, row in self._raw_contracts.iterrows():
            numero = row_pick(
                row, "numero_contrato", "nr_contrato", "contrato",
                "numero", "nr_instrumento",
            ).strip()
            orgao = normalize_name(
                row_pick(row, "orgao", "unidade_gestora", "orgao_contratante"),
            )
            cnpj_raw = row_pick(
                row, "cnpj_fornecedor", "cnpj_contratado", "cnpj", "documento",
            )
            cnpj_digits = strip_document(cnpj_raw)
            supplier_name = normalize_name(
                row_pick(
                    row, "razao_social", "fornecedor", "contratado", "nome_fornecedor",
                ),
            )
            objeto = normalize_name(row_pick(row, "objeto", "descricao", "finalidade"))
            valor_raw = row_pick(
                row, "valor", "valor_total", "valor_contrato", "vl_total",
            )
            published = row_pick(
                row, "data_publicacao", "dt_publicacao", "data_assinatura", "data",
            )
            vigencia_inicio = row_pick(
                row, "vigencia_inicio", "dt_inicio", "data_inicio",
            )
            vigencia_fim = row_pick(
                row, "vigencia_fim", "dt_fim", "data_fim", "data_termino",
            )

            if not numero and not cnpj_digits and not objeto:
                continue

            contract_id = _hash_id(numero, cnpj_digits, orgao, published)
            cnpj_fmt = format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""
            amount = parse_brl_flexible(valor_raw, default=None)
            amount = cap_contract_value(amount) if amount is not None else None

            contract_record_id = f"{numero}|{cnpj_fmt}|{published}"
            self.contracts.append(self.attach_provenance(
                {
                    "contract_id": contract_id,
                    "contract_number": numero,
                    "agency": orgao,
                    "cnpj_supplier": cnpj_fmt,
                    "supplier_name": supplier_name,
                    "object": objeto,
                    "amount": amount,
                    "published_at": parse_date(published) if published else "",
                    "vigencia_inicio": parse_date(vigencia_inicio) if vigencia_inicio else "",
                    "vigencia_fim": parse_date(vigencia_fim) if vigencia_fim else "",
                    "uf": "GO",
                    "source": "state_portal_go",
                },
                record_id=contract_record_id,
            ))

            if cnpj_fmt:
                self.contract_rels.append(self.attach_provenance(
                    {
                        "source_key": cnpj_fmt,
                        "target_key": contract_id,
                    },
                    record_id=contract_record_id,
                ))

    def _transform_suppliers(self) -> None:
        if self._raw_suppliers.empty:
            return
        for _, row in self._raw_suppliers.iterrows():
            cnpj_raw = row_pick(row, "cnpj", "cpf_cnpj", "documento", "nr_documento")
            cnpj_digits = strip_document(cnpj_raw)
            if len(cnpj_digits) != 14:
                # Skip CPFs and malformed entries - only GO state companies here.
                continue
            name = normalize_name(
                row_pick(
                    row, "razao_social", "nome", "fornecedor", "nome_fornecedor",
                ),
            )
            situacao = row_pick(row, "situacao", "status", "sit_cadastral")
            registered_at = row_pick(
                row, "data_cadastro", "dt_cadastro", "data_inscricao",
            )
            cnpj_fmt = format_cnpj(cnpj_raw)
            self.suppliers.append(self.attach_provenance(
                {
                    "cnpj": cnpj_fmt,
                    "name": name,
                    "situacao": situacao,
                    "registered_at": parse_date(registered_at) if registered_at else "",
                    "uf": "GO",
                    "source": "state_portal_go",
                },
                record_id=cnpj_fmt,
            ))

    def _transform_sanctions(self) -> None:
        if self._raw_sanctions.empty:
            return
        for _, row in self._raw_sanctions.iterrows():
            cnpj_raw = row_pick(
                row, "cnpj", "cpf_cnpj", "documento", "nr_documento", "cnpj_cpf",
            )
            cnpj_digits = strip_document(cnpj_raw)
            name = normalize_name(
                row_pick(
                    row, "razao_social", "fornecedor", "nome_sancionado", "nome",
                ),
            )
            tipo = row_pick(
                row, "tipo_sancao", "sancao", "tipo", "penalidade", "medida",
            )
            orgao = normalize_name(row_pick(row, "orgao_sancionador", "orgao"))
            inicio = row_pick(
                row, "data_inicio", "dt_inicio", "inicio_sancao", "inicio",
            )
            fim = row_pick(row, "data_fim", "dt_fim", "fim_sancao", "fim")
            processo = row_pick(row, "processo", "nr_processo", "numero_processo")

            if not cnpj_digits and not name:
                continue

            sanction_id = _hash_id(cnpj_digits, name, tipo, processo, inicio)
            cnpj_fmt = format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""

            sanction_record_id = f"{cnpj_fmt}|{tipo}|{processo}"
            self.sanctions.append(self.attach_provenance(
                {
                    "sanction_id": sanction_id,
                    "cnpj": cnpj_fmt,
                    "name": name,
                    "tipo": tipo,
                    "orgao": orgao,
                    "processo": processo,
                    "data_inicio": parse_date(inicio) if inicio else "",
                    "data_fim": parse_date(fim) if fim else "",
                    "uf": "GO",
                    "source": "state_portal_go",
                },
                record_id=sanction_record_id,
            ))
            if cnpj_fmt:
                self.sanction_rels.append(self.attach_provenance(
                    {
                        "source_key": cnpj_fmt,
                        "target_key": sanction_id,
                    },
                    record_id=sanction_record_id,
                ))

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not (self.contracts or self.suppliers or self.sanctions):
            logger.warning("[state_portal_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)

        if self.contracts:
            loader.load_nodes(
                "GoStateContract", self.contracts, key_field="contract_id",
            )

        if self.suppliers:
            loader.load_nodes(
                "Company",
                [
                    self.attach_provenance(
                        {
                            "cnpj": s["cnpj"],
                            "razao_social": s["name"],
                            "uf": s["uf"],
                            "source": s["source"],
                        },
                        record_id=strip_document(str(s["cnpj"])),
                    )
                    for s in self.suppliers
                ],
                key_field="cnpj",
            )
            loader.load_nodes(
                "GoStateSupplier", self.suppliers, key_field="cnpj",
            )

        if self.sanctions:
            loader.load_nodes(
                "GoStateSanction", self.sanctions, key_field="sanction_id",
            )
            # Ensure Company nodes exist for sanctioned entities. Raw CNPJ
            # digits are the natural record_id for the Company entity.
            sanctioned_companies = deduplicate_rows(
                [
                    self.attach_provenance(
                        {"cnpj": s["cnpj"], "razao_social": s["name"]},
                        record_id=strip_document(str(s["cnpj"])),
                    )
                    for s in self.sanctions
                    if s["cnpj"]
                ],
                ["cnpj"],
            )
            if sanctioned_companies:
                loader.load_nodes(
                    "Company", sanctioned_companies, key_field="cnpj",
                )

        # Ensure Company nodes exist for contract suppliers.
        if self.contracts:
            contract_companies = deduplicate_rows(
                [
                    self.attach_provenance(
                        {"cnpj": c["cnpj_supplier"], "razao_social": c["supplier_name"]},
                        record_id=strip_document(str(c["cnpj_supplier"])),
                    )
                    for c in self.contracts
                    if c["cnpj_supplier"]
                ],
                ["cnpj"],
            )
            if contract_companies:
                loader.load_nodes(
                    "Company", contract_companies, key_field="cnpj",
                )

        if self.contract_rels:
            loader.load_relationships(
                rel_type="CONTRATOU_ESTADO_GO",
                rows=self.contract_rels,
                source_label="Company",
                source_key="cnpj",
                target_label="GoStateContract",
                target_key="contract_id",
            )

        if self.sanction_rels:
            loader.load_relationships(
                rel_type="SANCIONADO_GO",
                rows=self.sanction_rels,
                source_label="Company",
                source_key="cnpj",
                target_label="GoStateSanction",
                target_key="sanction_id",
            )

        # Reference to stable_id keeps the helper imported when downstream agents
        # extend the scaffold with person/agency linkages.
        _ = _stable_id

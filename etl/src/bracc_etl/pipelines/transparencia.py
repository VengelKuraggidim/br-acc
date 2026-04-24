from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    cap_contract_value,
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_brl_flexible,
    parse_date,
    strip_document,
)

logger = logging.getLogger(__name__)

# Classified contracts (Polícia Federal etc.) use this sentinel CNPJ.
_SIGILOSO_CNPJ = "-11"

# Base URL canônica do Portal da Transparência. Usada como ``record_url``
# pra ``attach_provenance`` — pipeline-wide constante, porque o Portal não
# expõe deep-links estáveis por contrato individual (a página agrega o
# bulk download). ``source_id`` é canônico (``transparencia``, alinhado com
# ``docs/source_registry_br_v1.csv``).
_TRANSPARENCIA_SOURCE_URL = "https://portaldatransparencia.gov.br/download-de-dados"


def _extract_cpf_middle6(cpf_raw: str) -> str | None:
    """Extract 6 middle digits from LGPD-masked CPF (***.ABC.DEF-**)."""
    digits = strip_document(cpf_raw)
    if len(digits) == 6:
        return digits
    return None


def _make_servidor_id(cpf_partial: str | None, name: str) -> str:
    """Generate stable ID for servidor Person from partial CPF + name."""
    raw = f"{cpf_partial or ''}_{name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _make_office_id(cpf_partial: str | None, name: str, org: str) -> str:
    """Generate stable ID for PublicOffice from partial CPF + name + org."""
    raw = f"{cpf_partial or ''}_{name}_{org}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class TransparenciaPipeline(Pipeline):
    """ETL pipeline for Portal da Transparencia federal spending data."""

    name = "transparencia"
    source_id = "transparencia"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_contratos: pd.DataFrame = pd.DataFrame()
        self._raw_servidores: pd.DataFrame = pd.DataFrame()
        self._raw_emendas: pd.DataFrame = pd.DataFrame()
        self.contracts: list[dict[str, Any]] = []
        self.offices: list[dict[str, Any]] = []
        self.amendments: list[dict[str, Any]] = []

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "transparencia"
        if not src_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, src_dir)
            return
        contratos_path = src_dir / "contratos.csv"
        servidores_path = src_dir / "servidores.csv"
        emendas_path = src_dir / "emendas.csv"
        if not contratos_path.exists():
            logger.warning("[%s] contratos.csv not found in %s", self.name, src_dir)
        else:
            self._raw_contratos = pd.read_csv(
                contratos_path, dtype=str, keep_default_na=False, encoding="utf-8",
            )
        if not servidores_path.exists():
            logger.warning("[%s] servidores.csv not found in %s", self.name, src_dir)
        else:
            self._raw_servidores = pd.read_csv(
                servidores_path, dtype=str, keep_default_na=False, encoding="utf-8",
            )
        if not emendas_path.exists():
            logger.warning("[%s] emendas.csv not found in %s", self.name, src_dir)
        else:
            self._raw_emendas = pd.read_csv(
                emendas_path, dtype=str, keep_default_na=False, encoding="utf-8",
            )
        # rows_in = total de linhas cruas extraídas (antes de filtros de
        # qualidade em transform). É o denominador do funnel que vai pro
        # IngestionRun pra operador saber se o download foi útil.
        self.rows_in = (
            len(self._raw_contratos)
            + len(self._raw_servidores)
            + len(self._raw_emendas)
        )
        logger.info(
            "[%s] extracted contratos=%d servidores=%d emendas=%d (rows_in=%d)",
            self.name,
            len(self._raw_contratos),
            len(self._raw_servidores),
            len(self._raw_emendas),
            self.rows_in,
        )

    def transform(self) -> None:
        contracts: list[dict[str, Any]] = []
        for _, row in self._raw_contratos.iterrows():
            raw_cnpj = str(row["cnpj_contratada"]).strip()

            # Skip classified contracts (sigiloso) — no usable CNPJ
            if raw_cnpj == _SIGILOSO_CNPJ:
                continue

            # Skip rows where CNPJ has no digits (produces malformed contract_ids)
            cnpj_digits = strip_document(raw_cnpj)
            if len(cnpj_digits) != 14:
                continue

            cnpj = format_cnpj(raw_cnpj)
            date = parse_date(str(row["data_inicio"]))
            contracts.append({
                "contract_id": f"{cnpj_digits}_{row['data_inicio']}",
                "object": normalize_name(str(row["objeto"])),
                "value": cap_contract_value(parse_brl_flexible(str(row["valor"]))),
                "contracting_org": normalize_name(str(row["orgao_contratante"])),
                "date": date,
                "cnpj": cnpj,
                "razao_social": normalize_name(str(row["razao_social"])),
            })
        self.contracts = deduplicate_rows(contracts, ["contract_id"])

        offices: list[dict[str, Any]] = []
        for _, row in self._raw_servidores.iterrows():
            raw_cpf = str(row["cpf"])
            cpf_partial = _extract_cpf_middle6(raw_cpf)
            name = normalize_name(str(row["nome"]))
            org = normalize_name(str(row["orgao"]))
            salary = parse_brl_flexible(str(row["remuneracao"]))

            servidor_id = _make_servidor_id(cpf_partial, name)
            office_id = _make_office_id(cpf_partial, name, org)

            offices.append({
                "office_id": office_id,
                "servidor_id": servidor_id,
                "cpf_partial": cpf_partial,
                "name": name,
                "org": org,
                "salary": salary,
            })
        self.offices = deduplicate_rows(offices, ["office_id"])

        amendments: list[dict[str, Any]] = []
        for _, row in self._raw_emendas.iterrows():
            # Scope GO: o CSV do Portal é nacional, mas o app é Goiás-only.
            # Sem esse filtro, Persons-autor de outras UFs mesclam por nome
            # em Persons GO homônimos e contaminam perfis.
            uf_raw = str(row.get("uf", "")).strip().upper()
            if uf_raw not in ("GO", "GOIÁS", "GOIAS"):
                continue
            codigo = str(row.get("codigo_autor", "")).strip()
            nome = normalize_name(str(row["nome_autor"]))
            author_key = codigo if codigo else nome.replace(" ", "_")
            objeto = normalize_name(str(row["objeto"]))
            numero = str(row.get("numero", "")).strip()
            municipio = str(row.get("municipio", "")).strip()
            ano = str(row.get("ano", "")).strip()
            aid = (
                f"{author_key}_{ano}_{numero}_{municipio}"
                if numero
                else f"{author_key}_{objeto}_{municipio}"
            )

            amendments.append({
                "amendment_id": aid,
                "author_key": author_key,
                "name": nome,
                "object": objeto,
                "value": parse_brl_flexible(str(row["valor"])),
                "amendment_type": str(row.get("tipo", "")).strip(),
                "function": str(row.get("funcao", "")).strip(),
                "municipality": municipio,
                "uf": str(row.get("uf", "")).strip(),
                "year": ano,
                "value_committed": parse_brl_flexible(
                    str(row.get("valor_empenhado", row["valor"])),
                ),
                "value_paid": parse_brl_flexible(str(row.get("valor_pago", "0"))),
            })
        self.amendments = deduplicate_rows(amendments, ["amendment_id"])

    def _stamp(self, row: dict[str, Any], *, record_id: object) -> dict[str, Any]:
        """Shorthand pra ``attach_provenance`` com URL canônica embutida.

        Portal da Transparência não expõe deep-link por registro — todo
        row carimba o mesmo ``source_url`` (bulk download page), por isso
        passamos ``record_url`` explicitamente em vez de confiar em
        ``primary_url_for(source_id)``.
        """
        return self.attach_provenance(
            row,
            record_id=record_id,
            record_url=_TRANSPARENCIA_SOURCE_URL,
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.contracts:
            contract_rows = [
                self._stamp(
                    {
                        "contract_id": c["contract_id"],
                        "object": c["object"],
                        "value": c["value"],
                        "contracting_org": c["contracting_org"],
                        "date": c["date"],
                    },
                    record_id=c["contract_id"],
                )
                for c in self.contracts
            ]
            loaded += loader.load_nodes("Contract", contract_rows, key_field="contract_id")

            # Ensure Company nodes exist for contracted companies
            companies = deduplicate_rows(
                [{"cnpj": c["cnpj"], "razao_social": c["razao_social"]} for c in self.contracts],
                ["cnpj"],
            )
            company_rows = [
                self._stamp(co, record_id=co["cnpj"]) for co in companies
            ]
            loaded += loader.load_nodes("Company", company_rows, key_field="cnpj")

            # VENCEU: Company -> Contract
            venceu_rows = [
                self._stamp(
                    {"source_key": c["cnpj"], "target_key": c["contract_id"]},
                    record_id=c["contract_id"],
                )
                for c in self.contracts
            ]
            loaded += loader.load_relationships(
                rel_type="VENCEU",
                rows=venceu_rows,
                source_label="Company",
                source_key="cnpj",
                target_label="Contract",
                target_key="contract_id",
            )

        if self.offices:
            # PublicOffice nodes — keyed on office_id (hash of cpf_partial+name+org)
            office_rows = [
                self._stamp(o, record_id=o["office_id"]) for o in self.offices
            ]
            po_query = (
                "UNWIND $rows AS row "
                "MERGE (po:PublicOffice {office_id: row.office_id}) "
                "SET po.cpf_partial = row.cpf_partial, po.name = row.name, "
                "po.org = row.org, po.salary = row.salary, "
                "po.source_id = row.source_id, po.source_url = row.source_url, "
                "po.source_record_id = row.source_record_id, "
                "po.ingested_at = row.ingested_at, po.run_id = row.run_id"
            )
            loaded += loader.run_query(po_query, office_rows)

            # Person nodes — keyed on servidor_id (hash of cpf_partial+name)
            # DO NOT set cpf — would conflict with uniqueness constraint
            persons = deduplicate_rows(
                [
                    {
                        "servidor_id": o["servidor_id"],
                        "cpf_partial": o["cpf_partial"],
                        "name": o["name"],
                    }
                    for o in self.offices
                ],
                ["servidor_id"],
            )
            person_rows = [
                self._stamp(p, record_id=p["servidor_id"]) for p in persons
            ]
            person_query = (
                "UNWIND $rows AS row "
                "MERGE (p:Person {servidor_id: row.servidor_id}) "
                "SET p.cpf_partial = row.cpf_partial, p.name = row.name, "
                "p.source = 'transparencia', "
                "p.source_id = row.source_id, p.source_url = row.source_url, "
                "p.source_record_id = row.source_record_id, "
                "p.ingested_at = row.ingested_at, p.run_id = row.run_id"
            )
            loaded += loader.run_query(person_query, person_rows)

            # RECEBEU_SALARIO: Person -> PublicOffice
            rel_query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {servidor_id: row.servidor_id}) "
                "MATCH (po:PublicOffice {office_id: row.office_id}) "
                "MERGE (p)-[r:RECEBEU_SALARIO]->(po) "
                "SET r.source_id = row.source_id, r.source_url = row.source_url, "
                "r.source_record_id = row.source_record_id, "
                "r.ingested_at = row.ingested_at, r.run_id = row.run_id"
            )
            rel_rows = [
                self._stamp(
                    {"servidor_id": o["servidor_id"], "office_id": o["office_id"]},
                    record_id=o["office_id"],
                )
                for o in self.offices
            ]
            loaded += loader.run_query(rel_query, rel_rows)

        if self.amendments:
            # Amendment nodes — each emenda is its own entity
            amendment_rows = [
                self._stamp(
                    {
                        "amendment_id": a["amendment_id"],
                        "object": a["object"],
                        "value": a["value"],
                        "type": a.get("amendment_type", ""),
                        "function": a.get("function", ""),
                        "municipality": a.get("municipality", ""),
                        "uf": a.get("uf", ""),
                        "year": a.get("year", ""),
                        "value_committed": a.get("value_committed", 0.0),
                        "value_paid": a.get("value_paid", 0.0),
                    },
                    record_id=a["amendment_id"],
                )
                for a in self.amendments
            ]
            loaded += loader.load_nodes(
                "Amendment", amendment_rows, key_field="amendment_id",
            )

            # Person nodes for amendment authors (keyed by author_key).
            # Entity resolution links these to TSE candidates later.
            persons = deduplicate_rows(
                [{"name": a["name"], "author_key": a["author_key"]} for a in self.amendments],
                ["author_key"],
            )
            person_rows = [
                self._stamp(p, record_id=p["author_key"]) for p in persons
            ]
            loaded += loader.load_nodes("Person", person_rows, key_field="author_key")

            # AUTOR_EMENDA: Person -> Amendment
            autor_rows = [
                self._stamp(
                    {"source_key": a["author_key"], "target_key": a["amendment_id"]},
                    record_id=a["amendment_id"],
                )
                for a in self.amendments
            ]
            loaded += loader.load_relationships(
                rel_type="AUTOR_EMENDA",
                rows=autor_rows,
                source_label="Person",
                source_key="author_key",
                target_label="Amendment",
                target_key="amendment_id",
            )

        # rows_loaded = total de rows escritas no Neo4j (nodes + rels). É o
        # numerador do funnel; combined com rows_in vira quality signal.
        self.rows_loaded = loaded
        logger.info("[%s] loaded %d rows into Neo4j", self.name, loaded)

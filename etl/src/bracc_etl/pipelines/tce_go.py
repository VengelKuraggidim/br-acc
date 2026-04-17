"""ETL pipeline scaffold for TCE Goias (Tribunal de Contas do Estado de Goias).

TCE-GO publishes several operational dashboards (fiscalizacoes em andamento,
contas irregulares, decisoes, diario eletronico) at https://portal.tce.go.gov.br/
but does not currently expose a public JSON API or bulk CSV export. This
pipeline follows the repo convention of accepting pre-downloaded CSV files
placed under ``data/tce_go/``:

- ``decisoes.csv``    -> TceGoDecision nodes
- ``irregulares.csv`` -> TceGoIrregularAccount nodes + IMPEDIDO_TCE_GO rels
- ``fiscalizacoes.csv`` -> TceGoAudit nodes

Human validation required before production use:

1. Confirm the CSV schema exported from each TCE-GO dashboard.
2. Verify legal terms of use for each dataset (some portals require a data
   request form rather than open redistribution).
3. Decide whether to fetch via scraping (respecting robots.txt) or to rely
   exclusively on operator-provided exports.

Data source: https://portal.tce.go.gov.br/
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_date,
    row_pick,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


class TceGoPipeline(Pipeline):
    """Scaffold pipeline for TCE Goias audit data.

    Reads pre-downloaded CSV files under ``data/tce_go/``. No remote API
    fallback because TCE-GO has not published a documented open data
    endpoint at time of writing.
    """

    name = "tce_go"
    source_id = "tce_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_decisions: pd.DataFrame = pd.DataFrame()
        self._raw_irregular: pd.DataFrame = pd.DataFrame()
        self._raw_audits: pd.DataFrame = pd.DataFrame()

        self.decisions: list[dict[str, Any]] = []
        self.irregular_accounts: list[dict[str, Any]] = []
        self.audits: list[dict[str, Any]] = []
        self.impedido_rels: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        for sep in (";", ","):
            try:
                df = pd.read_csv(
                    path, sep=sep, dtype=str, keep_default_na=False,
                    encoding="utf-8", engine="python", on_bad_lines="skip",
                )
                if len(df.columns) > 1:
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        try:
            return pd.read_csv(
                path, sep=";", dtype=str, keep_default_na=False,
                encoding="latin-1", engine="python", on_bad_lines="skip",
            )
        except (OSError, pd.errors.ParserError) as exc:
            logger.warning("[tce_go] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "tce_go"
        if not src_dir.exists():
            logger.warning(
                "[tce_go] expected directory %s is missing; "
                "export TCE-GO dashboards to CSV there.",
                src_dir,
            )
            return

        self._raw_decisions = self._read_csv_optional(src_dir / "decisoes.csv")
        self._raw_irregular = self._read_csv_optional(src_dir / "irregulares.csv")
        self._raw_audits = self._read_csv_optional(src_dir / "fiscalizacoes.csv")

        if self.limit:
            self._raw_decisions = self._raw_decisions.head(self.limit)
            self._raw_irregular = self._raw_irregular.head(self.limit)
            self._raw_audits = self._raw_audits.head(self.limit)

        self.rows_in = (
            len(self._raw_decisions)
            + len(self._raw_irregular)
            + len(self._raw_audits)
        )
        logger.info(
            "[tce_go] extracted decisions=%d irregular=%d audits=%d",
            len(self._raw_decisions),
            len(self._raw_irregular),
            len(self._raw_audits),
        )

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        self._transform_decisions()
        self._transform_irregular()
        self._transform_audits()

        self.decisions = deduplicate_rows(self.decisions, ["decision_id"])
        self.irregular_accounts = deduplicate_rows(
            self.irregular_accounts, ["account_id"],
        )
        self.audits = deduplicate_rows(self.audits, ["audit_id"])
        self.impedido_rels = deduplicate_rows(
            self.impedido_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = (
            len(self.decisions)
            + len(self.irregular_accounts)
            + len(self.audits)
        )

    def _transform_decisions(self) -> None:
        for _, row in self._raw_decisions.iterrows():
            numero = row_pick(row, "numero", "nr_processo", "acordao", "decisao").strip()
            tipo = row_pick(row, "tipo", "tipo_decisao", "modalidade")
            data = row_pick(row, "data", "dt_publicacao", "data_decisao")
            orgao = normalize_name(row_pick(row, "orgao", "unidade"))
            ementa = normalize_name(row_pick(row, "ementa", "resumo", "descricao"))
            relator = normalize_name(row_pick(row, "relator", "conselheiro"))
            if not numero and not ementa:
                continue
            decision_id = _hash_id(numero, data, tipo)
            self.decisions.append({
                "decision_id": decision_id,
                "numero": numero,
                "tipo": tipo,
                "orgao": orgao,
                "relator": relator,
                "ementa": ementa,
                "published_at": parse_date(data) if data else "",
                "uf": "GO",
                "source": "tce_go",
            })

    def _transform_irregular(self) -> None:
        for _, row in self._raw_irregular.iterrows():
            cnpj_raw = row_pick(row, "cnpj", "cpf_cnpj", "documento")
            cnpj_digits = strip_document(cnpj_raw)
            name = normalize_name(
                row_pick(row, "nome", "razao_social", "responsavel"),
            )
            processo = row_pick(row, "processo", "nr_processo")
            julgamento = row_pick(row, "julgamento", "data_julgamento", "data")
            motivo = normalize_name(row_pick(row, "motivo", "fundamento", "decisao"))
            if not cnpj_digits and not name:
                continue
            account_id = _hash_id(cnpj_digits, name, processo, julgamento)
            cnpj_fmt = format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""
            self.irregular_accounts.append({
                "account_id": account_id,
                "cnpj": cnpj_fmt,
                "name": name,
                "processo": processo,
                "motivo": motivo,
                "julgamento": parse_date(julgamento) if julgamento else "",
                "uf": "GO",
                "source": "tce_go",
            })
            if cnpj_fmt:
                self.impedido_rels.append({
                    "source_key": cnpj_fmt,
                    "target_key": account_id,
                })

    def _transform_audits(self) -> None:
        for _, row in self._raw_audits.iterrows():
            numero = row_pick(row, "numero", "nr_processo", "processo").strip()
            titulo = normalize_name(row_pick(row, "titulo", "objeto", "descricao"))
            orgao = normalize_name(row_pick(row, "orgao", "unidade", "jurisdicionado"))
            status = row_pick(row, "status", "situacao", "fase")
            inicio = row_pick(row, "data_inicio", "dt_inicio", "inicio")
            if not numero and not titulo:
                continue
            audit_id = _hash_id(numero, titulo, inicio)
            self.audits.append({
                "audit_id": audit_id,
                "numero": numero,
                "titulo": titulo,
                "orgao": orgao,
                "status": status,
                "data_inicio": parse_date(inicio) if inicio else "",
                "uf": "GO",
                "source": "tce_go",
            })

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not (self.decisions or self.irregular_accounts or self.audits):
            logger.warning("[tce_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)

        if self.decisions:
            loader.load_nodes(
                "TceGoDecision", self.decisions, key_field="decision_id",
            )

        if self.irregular_accounts:
            loader.load_nodes(
                "TceGoIrregularAccount",
                self.irregular_accounts,
                key_field="account_id",
            )
            companies = deduplicate_rows(
                [
                    {"cnpj": r["cnpj"], "razao_social": r["name"]}
                    for r in self.irregular_accounts
                    if r["cnpj"]
                ],
                ["cnpj"],
            )
            if companies:
                loader.load_nodes("Company", companies, key_field="cnpj")

        if self.audits:
            loader.load_nodes("TceGoAudit", self.audits, key_field="audit_id")

        if self.impedido_rels:
            loader.load_relationships(
                rel_type="IMPEDIDO_TCE_GO",
                rows=self.impedido_rels,
                source_label="Company",
                source_key="cnpj",
                target_label="TceGoIrregularAccount",
                target_key="account_id",
            )

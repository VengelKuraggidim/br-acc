"""ETL pipeline scaffold for TCM-GO (Tribunal de Contas dos Municipios de Goias).

The TCM-GO publishes a list of individuals "impedidos de licitar, contratar
ou exercer cargo publico" and rejected municipal accounts at
https://www.tcmgo.tc.br/site/. No documented JSON API or bulk CSV export is
available at time of writing; this scaffold accepts operator-exported CSVs
under ``data/tcmgo_sancoes/``:

- ``impedidos.csv``    -> TcmGoImpedido nodes + IMPEDIDO_TCMGO rels
- ``rejeitados.csv``   -> TcmGoRejectedAccount nodes

Human validation required:

1. Confirm CSV schema once an operator produces a sample export.
2. Check whether TCM-GO provides a machine-readable list (some TCMs expose
   JSON through their jurisprudence search).
3. Distinguish TCM-GO (municipal tribunal) from TCE-GO (state tribunal)
   when merging into the existing sanctions graph.

Note: this is separate from the ``tcm_go`` pipeline already in the registry,
which ingests SICONFI fiscal data for GO municipalities (different source).

Data source: https://www.tcmgo.tc.br/
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
    mask_cpf,
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


class TcmgoSancoesPipeline(Pipeline):
    """Scaffold pipeline for TCM-GO impedidos and rejected accounts."""

    name = "tcmgo_sancoes"
    source_id = "tcmgo_sancoes"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_impedidos: pd.DataFrame = pd.DataFrame()
        self._raw_rejeitados: pd.DataFrame = pd.DataFrame()

        self.impedidos: list[dict[str, Any]] = []
        self.rejected_accounts: list[dict[str, Any]] = []
        self.impedido_rels: list[dict[str, Any]] = []

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
            logger.warning("[tcmgo_sancoes] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "tcmgo_sancoes"
        if not src_dir.exists():
            logger.warning(
                "[tcmgo_sancoes] expected directory %s missing; "
                "export TCM-GO portal CSVs there.",
                src_dir,
            )
            return
        self._raw_impedidos = self._read_csv_optional(src_dir / "impedidos.csv")
        self._raw_rejeitados = self._read_csv_optional(src_dir / "rejeitados.csv")

        if self.limit:
            self._raw_impedidos = self._raw_impedidos.head(self.limit)
            self._raw_rejeitados = self._raw_rejeitados.head(self.limit)

        self.rows_in = len(self._raw_impedidos) + len(self._raw_rejeitados)

    def transform(self) -> None:
        for _, row in self._raw_impedidos.iterrows():
            doc_raw = row_pick(row, "cpf_cnpj", "documento", "cnpj", "cpf")
            doc_digits = strip_document(doc_raw)
            name = normalize_name(
                row_pick(row, "nome", "razao_social", "responsavel"),
            )
            motivo = normalize_name(
                row_pick(row, "motivo", "fundamento", "decisao"),
            )
            processo = row_pick(row, "processo", "nr_processo")
            inicio = row_pick(row, "data_inicio", "inicio_impedimento", "dt_inicio")
            fim = row_pick(row, "data_fim", "fim_impedimento", "dt_fim")
            if not doc_digits and not name:
                continue
            record_id = _hash_id(doc_digits, name, processo, inicio)
            doc_kind, doc_fmt = "", ""
            if len(doc_digits) == 14:
                doc_kind = "CNPJ"
                doc_fmt = format_cnpj(doc_raw)
            elif len(doc_digits) == 11:
                doc_kind = "CPF"
                doc_fmt = mask_cpf(doc_raw)
            self.impedidos.append({
                "impedido_id": record_id,
                "document": doc_fmt,
                "document_kind": doc_kind,
                "name": name,
                "motivo": motivo,
                "processo": processo,
                "data_inicio": parse_date(inicio) if inicio else "",
                "data_fim": parse_date(fim) if fim else "",
                "uf": "GO",
                "source": "tcmgo_sancoes",
            })
            if doc_kind == "CNPJ":
                self.impedido_rels.append({
                    "source_key": doc_fmt,
                    "target_key": record_id,
                })

        for _, row in self._raw_rejeitados.iterrows():
            municipio = normalize_name(
                row_pick(row, "municipio", "ente", "nome_ente"),
            )
            cod_ibge = row_pick(row, "cod_ibge", "codigo_ibge", "ibge")
            exercicio = row_pick(row, "exercicio", "ano", "ano_exercicio")
            processo = row_pick(row, "processo", "nr_processo")
            parecer = row_pick(row, "parecer", "julgamento", "decisao")
            relator = normalize_name(row_pick(row, "relator", "conselheiro"))
            if not municipio and not processo:
                continue
            record_id = _hash_id(cod_ibge, municipio, exercicio, processo)
            self.rejected_accounts.append({
                "account_id": record_id,
                "cod_ibge": cod_ibge,
                "municipality": municipio,
                "exercicio": exercicio,
                "processo": processo,
                "parecer": parecer,
                "relator": relator,
                "uf": "GO",
                "source": "tcmgo_sancoes",
            })

        self.impedidos = deduplicate_rows(self.impedidos, ["impedido_id"])
        self.rejected_accounts = deduplicate_rows(
            self.rejected_accounts, ["account_id"],
        )
        self.impedido_rels = deduplicate_rows(
            self.impedido_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = len(self.impedidos) + len(self.rejected_accounts)

    def load(self) -> None:
        if not (self.impedidos or self.rejected_accounts):
            logger.warning("[tcmgo_sancoes] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        if self.impedidos:
            loader.load_nodes(
                "TcmGoImpedido", self.impedidos, key_field="impedido_id",
            )
            companies = deduplicate_rows(
                [
                    {"cnpj": r["document"], "razao_social": r["name"]}
                    for r in self.impedidos
                    if r["document_kind"] == "CNPJ"
                ],
                ["cnpj"],
            )
            if companies:
                loader.load_nodes("Company", companies, key_field="cnpj")
        if self.rejected_accounts:
            loader.load_nodes(
                "TcmGoRejectedAccount",
                self.rejected_accounts,
                key_field="account_id",
            )
        if self.impedido_rels:
            loader.load_relationships(
                rel_type="IMPEDIDO_TCMGO",
                rows=self.impedido_rels,
                source_label="Company",
                source_key="cnpj",
                target_label="TcmGoImpedido",
                target_key="impedido_id",
            )

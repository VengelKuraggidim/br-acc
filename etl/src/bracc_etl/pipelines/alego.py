"""ETL pipeline scaffold for ALEGO (Assembleia Legislativa de Goias).

ALEGO maintains a transparency portal at https://transparencia.al.go.leg.br/
but does not expose a documented open-data API or CSV bulk endpoints at time
of writing. This scaffold accepts CSV exports placed under ``data/alego/``:

- ``deputados.csv``           -> StateLegislator nodes (UF=GO)
- ``cota_parlamentar.csv``    -> LegislativeExpense nodes + GASTOU_COTA_GO rels
- ``proposicoes.csv``         -> LegislativeProposition nodes

Human validation required:

1. Confirm CSV schema from the transparency portal (field names, encoding).
2. Determine whether ALEGO offers structured JSON feeds (e.g. via Camara-like
   /dadosabertos/deputados endpoint on alegodigital.al.go.leg.br).
3. Verify licensing/terms of reuse.

Data source: https://transparencia.al.go.leg.br/
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
    parse_brl_flexible,
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


class AlegoPipeline(Pipeline):
    """Scaffold pipeline for ALEGO transparency data."""

    name = "alego"
    source_id = "alego"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_deputados: pd.DataFrame = pd.DataFrame()
        self._raw_cota: pd.DataFrame = pd.DataFrame()
        self._raw_propositions: pd.DataFrame = pd.DataFrame()

        self.legislators: list[dict[str, Any]] = []
        self.expenses: list[dict[str, Any]] = []
        self.propositions: list[dict[str, Any]] = []
        self.expense_rels: list[dict[str, Any]] = []

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
            logger.warning("[alego] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "alego"
        if not src_dir.exists():
            logger.warning(
                "[alego] expected directory %s missing; export ALEGO transparency "
                "portal CSVs there.",
                src_dir,
            )
            return
        self._raw_deputados = self._read_csv_optional(src_dir / "deputados.csv")
        self._raw_cota = self._read_csv_optional(src_dir / "cota_parlamentar.csv")
        self._raw_propositions = self._read_csv_optional(src_dir / "proposicoes.csv")

        if self.limit:
            self._raw_deputados = self._raw_deputados.head(self.limit)
            self._raw_cota = self._raw_cota.head(self.limit)
            self._raw_propositions = self._raw_propositions.head(self.limit)

        self.rows_in = (
            len(self._raw_deputados)
            + len(self._raw_cota)
            + len(self._raw_propositions)
        )

    def transform(self) -> None:
        for _, row in self._raw_deputados.iterrows():
            name = normalize_name(
                row_pick(row, "nome", "deputado", "nome_parlamentar"),
            )
            cpf_raw = row_pick(row, "cpf", "documento")
            party = row_pick(row, "partido", "sigla_partido")
            legislature = row_pick(row, "legislatura", "mandato")
            if not name:
                continue
            cpf_digits = strip_document(cpf_raw)
            legislator_id = _hash_id(
                name, cpf_digits[-4:] if cpf_digits else "", legislature,
            )
            self.legislators.append({
                "legislator_id": legislator_id,
                "name": name,
                "cpf": mask_cpf(cpf_raw) if cpf_digits else "",
                "party": party,
                "legislature": legislature,
                "uf": "GO",
                "source": "alego",
            })

        for _, row in self._raw_cota.iterrows():
            legislator_name = normalize_name(
                row_pick(row, "deputado", "nome", "nome_parlamentar"),
            )
            fornecedor = normalize_name(
                row_pick(row, "fornecedor", "razao_social"),
            )
            cnpj_raw = row_pick(row, "cnpj_fornecedor", "cnpj")
            cnpj_digits = strip_document(cnpj_raw)
            amount = parse_brl_flexible(
                row_pick(row, "valor", "valor_liquido", "valor_total"),
                default=None,
            )
            data = row_pick(row, "data", "data_emissao", "dt_documento")
            tipo = row_pick(row, "tipo_despesa", "natureza", "descricao")
            if not legislator_name and not fornecedor:
                continue
            expense_id = _hash_id(
                legislator_name, cnpj_digits, tipo, data, str(amount or ""),
            )
            self.expenses.append({
                "expense_id": expense_id,
                "legislator": legislator_name,
                "supplier": fornecedor,
                "cnpj_supplier": (
                    format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""
                ),
                "tipo": tipo,
                "amount": amount,
                "date": parse_date(data) if data else "",
                "uf": "GO",
                "source": "alego",
            })
            if legislator_name:
                legislator_id = _hash_id(legislator_name, "", "")
                self.expense_rels.append({
                    "source_key": legislator_id,
                    "target_key": expense_id,
                })

        for _, row in self._raw_propositions.iterrows():
            numero = row_pick(row, "numero", "nr_proposicao", "identificacao")
            titulo = normalize_name(row_pick(row, "titulo", "ementa", "assunto"))
            autor = normalize_name(row_pick(row, "autor", "proponente"))
            data = row_pick(row, "data", "data_apresentacao")
            if not numero and not titulo:
                continue
            prop_id = _hash_id(numero, titulo, data)
            self.propositions.append({
                "proposition_id": prop_id,
                "numero": numero,
                "titulo": titulo,
                "autor": autor,
                "date": parse_date(data) if data else "",
                "uf": "GO",
                "source": "alego",
            })

        self.legislators = deduplicate_rows(self.legislators, ["legislator_id"])
        self.expenses = deduplicate_rows(self.expenses, ["expense_id"])
        self.propositions = deduplicate_rows(self.propositions, ["proposition_id"])
        self.expense_rels = deduplicate_rows(
            self.expense_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = (
            len(self.legislators) + len(self.expenses) + len(self.propositions)
        )

    def load(self) -> None:
        if not (self.legislators or self.expenses or self.propositions):
            logger.warning("[alego] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        if self.legislators:
            loader.load_nodes(
                "StateLegislator", self.legislators, key_field="legislator_id",
            )
        if self.expenses:
            loader.load_nodes(
                "LegislativeExpense", self.expenses, key_field="expense_id",
            )
        if self.propositions:
            loader.load_nodes(
                "LegislativeProposition",
                self.propositions,
                key_field="proposition_id",
            )
        if self.expense_rels:
            loader.load_relationships(
                rel_type="GASTOU_COTA_GO",
                rows=self.expense_rels,
                source_label="StateLegislator",
                source_key="legislator_id",
                target_label="LegislativeExpense",
                target_key="expense_id",
            )

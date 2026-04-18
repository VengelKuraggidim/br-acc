from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    cap_contract_value,
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_date,
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

# BigQuery dataset used by the world_wb_mides profile (Base dos Dados mirror
# of the World Bank Brazilian municipal-procurement collection). The legacy
# br_mides dataset is also accepted for backward compatibility.
MIDES_WORLD_WB_DATASET = "basedosdados.world_wb_mides"
MIDES_LEGACY_DATASET = "basedosdados.br_mides"


def fetch_to_disk(
    output_dir: Path,
    *,
    date: str | None = None,
    billing_project: str | None = None,
    dataset: str = MIDES_WORLD_WB_DATASET,
    start_year: int = 2021,
    end_year: int = 2100,
    skip_existing: bool = True,
) -> list[Path]:
    """Download the three canonical MiDES tables from BigQuery to ``output_dir``.

    The :class:`MidesPipeline` extract step reads ``licitacao.csv``,
    ``contrato.csv``, ``item.csv`` (or their ``.parquet`` siblings) from
    ``data/mides/``; this writer materialises those exact filenames via
    BigQuery queries against the World Bank MiDES dataset on Base dos Dados.

    REQUIRES: optional ``[bigquery]`` extra (``google-cloud-bigquery``,
    ``pyarrow``) and a configured ``GOOGLE_APPLICATION_CREDENTIALS`` env var
    pointing at a GCP service account with BigQuery user role on the
    billing project. If those preconditions are missing this function
    raises a clear RuntimeError without partial writes.

    Parameters
    ----------
    output_dir:
        Destination directory (created if missing).
    date:
        Accepted for API symmetry; if a YYYY-prefixed token is passed and
        ``start_year``/``end_year`` were left at defaults, narrows the year
        filter to that single year.
    billing_project:
        GCP project used as the BigQuery billing target. Falls back to the
        ``GCP_BILLING_PROJECT`` env var. If neither is set, raises
        ``RuntimeError`` — no silent default.
    dataset:
        Either ``MIDES_WORLD_WB_DATASET`` (preferred, default) or
        ``MIDES_LEGACY_DATASET`` (older Base dos Dados MiDES schema).
    start_year, end_year:
        Year range filter applied via ``ano`` column in the BQ queries.
    skip_existing:
        Skip a target file if it already exists with non-zero size.

    Returns
    -------
    Sorted list of CSV paths actually materialised.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if date and start_year == 2021 and end_year == 2100:
        token = date.replace("-", "")
        if len(token) >= 4:
            try:
                year = int(token[:4])
                start_year = year
                end_year = year
            except ValueError:
                pass

    try:
        # Lazy imports: the [bigquery] optional extra ships google-cloud-*
        # and pyarrow. Keeping the import inside fetch_to_disk lets the rest
        # of the pipeline (extract/transform/load) run on systems that only
        # have the Receita-side base CSVs already on disk.
        import google.auth  # type: ignore[import-not-found,unused-ignore]
        from google.cloud import bigquery  # type: ignore[import-not-found,unused-ignore]
    except ImportError as exc:  # pragma: no cover - exercised in the CLI
        raise RuntimeError(
            "[mides] fetch_to_disk requires the [bigquery] extra: "
            "uv sync --extra bigquery (or pip install '.[bigquery]')."
        ) from exc

    import os

    project = billing_project or os.environ.get("GCP_BILLING_PROJECT")
    if not project:
        raise RuntimeError(
            "[mides] billing_project arg ausente e GCP_BILLING_PROJECT env var nao "
            "esta setada. Defina um dos dois pro BigQuery billing target."
        )
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        # Surface this as a clear error rather than letting google.auth raise
        # the much less actionable DefaultCredentialsError mid-query.
        raise RuntimeError(
            "[mides] GOOGLE_APPLICATION_CREDENTIALS is not set — "
            "BigQuery downloads require a service-account JSON path.",
        )

    queries = _mides_queries(dataset, start_year, end_year)

    credentials, _ = google.auth.default()
    client = bigquery.Client(project=project, credentials=credentials)

    written: list[Path] = []
    for filename, query in queries.items():
        out_path = output_dir / filename
        if skip_existing and out_path.exists() and out_path.stat().st_size > 0:
            logger.info(
                "[mides] %s exists (%d bytes), skipping",
                out_path, out_path.stat().st_size,
            )
            written.append(out_path)
            continue
        logger.info("[mides] running BigQuery -> %s", filename)
        df = client.query(query).result().to_dataframe(create_bqstorage_client=True)
        df.to_csv(out_path, index=False)
        logger.info("[mides] wrote %d rows -> %s", len(df), out_path)
        written.append(out_path)

    return sorted(written)


def _mides_queries(
    dataset: str, start_year: int, end_year: int,
) -> dict[str, str]:
    """Build the three CSV-yielding queries shipped by the legacy CLI.

    Mirrors the queries previously embedded in
    ``etl/scripts/download_mides.py``; lifting them up to the pipeline module
    keeps the CLI thin and lets future tests assert their shape.
    """
    if "world_wb_mides" in dataset.lower():
        year_filter_licitacao = (
            f"WHERE SAFE_CAST(l.ano AS INT64) BETWEEN {start_year} AND {end_year}"
        )
        year_filter_items = (
            f"WHERE SAFE_CAST(i.ano AS INT64) BETWEEN {start_year} AND {end_year}"
        )
        year_filter_participante = (
            f"WHERE SAFE_CAST(ano AS INT64) BETWEEN {start_year} AND {end_year}"
        )
        data_publicacao_expr = (
            "CAST(COALESCE(l.data_publicacao_dispensa, l.data_edital, "
            "l.data_abertura, l.data_homologacao) AS STRING)"
        )
        data_publicacao_filter = (
            "AND (COALESCE(l.data_publicacao_dispensa, l.data_edital, "
            "l.data_abertura, l.data_homologacao) IS NULL "
            "OR COALESCE(l.data_publicacao_dispensa, l.data_edital, "
            "l.data_abertura, l.data_homologacao) <= CURRENT_DATE() + INTERVAL 365 DAY)"
        )
        valor_estimado_expr = (
            "CAST(COALESCE(l.valor_orcamento, l.valor, l.valor_corrigido) AS FLOAT64)"
        )
        valor_expr = (
            "CAST(COALESCE(l.valor, l.valor_corrigido, l.valor_orcamento) AS FLOAT64)"
        )
        data_assinatura_expr = (
            "CAST(COALESCE(l.data_homologacao, l.data_abertura, l.data_edital) AS STRING)"
        )
        data_assinatura_filter = (
            "AND (COALESCE(l.data_homologacao, l.data_abertura, l.data_edital) IS NULL "
            "OR COALESCE(l.data_homologacao, l.data_abertura, l.data_edital) "
            "<= CURRENT_DATE() + INTERVAL 365 DAY)"
        )
        winners_cte = (
            "WITH winners AS ("
            "SELECT id_licitacao, id_municipio, sigla_uf, orgao, id_unidade_gestora, "
            "ANY_VALUE(documento) AS winner_document "
            f"FROM `{dataset}.licitacao_participante` "
            f"{year_filter_participante} "
            "AND SAFE_CAST(vencedor AS INT64) = 1 "
            "GROUP BY id_licitacao, id_municipio, sigla_uf, orgao, id_unidade_gestora"
            ") "
        )
        return {
            "licitacao.csv": (
                winners_cte
                + "SELECT "
                "CAST(l.id_licitacao AS STRING) AS licitacao_id, "
                "CAST(l.id_licitacao_bd AS STRING) AS id_licitacao, "
                "CAST(l.id_licitacao AS STRING) AS numero_processo, "
                "CAST(l.id_municipio AS STRING) AS cod_ibge, "
                "CAST('' AS STRING) AS municipio, "
                "CAST(l.sigla_uf AS STRING) AS estado, "
                "CAST(l.modalidade AS STRING) AS modalidade, "
                "CAST(l.descricao_objeto AS STRING) AS objeto, "
                f"{data_publicacao_expr} AS data_publicacao, "
                "CAST(l.ano AS STRING) AS ano, "
                f"{valor_estimado_expr} AS valor_estimado, "
                f"{valor_expr} AS valor, "
                "CAST(w.winner_document AS STRING) AS cnpj_vencedor, "
                "CAST('https://basedosdados.org/dataset/world-wb-mides' AS STRING) AS url "
                f"FROM `{dataset}.licitacao` l "
                "LEFT JOIN winners w "
                "ON w.id_licitacao = l.id_licitacao "
                "AND w.id_municipio = l.id_municipio "
                "AND w.sigla_uf = l.sigla_uf "
                "AND w.orgao = l.orgao "
                "AND w.id_unidade_gestora = l.id_unidade_gestora "
                f"{year_filter_licitacao} "
                f"{data_publicacao_filter}"
            ),
            "contrato.csv": (
                winners_cte
                + "SELECT "
                "CAST(l.id_licitacao AS STRING) AS contrato_id, "
                "CAST(l.id_licitacao AS STRING) AS numero_contrato, "
                "CAST(l.id_licitacao AS STRING) AS licitacao_id, "
                "CAST(l.id_licitacao AS STRING) AS numero_processo, "
                "CAST(l.id_municipio AS STRING) AS cod_ibge, "
                "CAST('' AS STRING) AS municipio, "
                "CAST(l.sigla_uf AS STRING) AS estado, "
                f"{data_assinatura_expr} AS data_assinatura, "
                "CAST(l.descricao_objeto AS STRING) AS objeto, "
                f"{valor_expr} AS valor, "
                "CAST(w.winner_document AS STRING) AS cnpj_fornecedor, "
                "CAST('https://basedosdados.org/dataset/world-wb-mides' AS STRING) AS url "
                f"FROM `{dataset}.licitacao` l "
                "LEFT JOIN winners w "
                "ON w.id_licitacao = l.id_licitacao "
                "AND w.id_municipio = l.id_municipio "
                "AND w.sigla_uf = l.sigla_uf "
                "AND w.orgao = l.orgao "
                "AND w.id_unidade_gestora = l.id_unidade_gestora "
                f"{year_filter_licitacao} "
                f"{data_assinatura_filter}"
            ),
            "item.csv": (
                "SELECT "
                "CAST(i.id_licitacao AS STRING) AS contrato_id, "
                "CAST(i.id_licitacao AS STRING) AS licitacao_id, "
                "CAST(i.id_item_bd AS STRING) AS id_item, "
                "CAST(i.numero AS STRING) AS numero_item, "
                "CAST(i.descricao AS STRING) AS descricao, "
                "CAST(i.quantidade AS FLOAT64) AS quantidade, "
                "CAST(i.valor_unitario AS FLOAT64) AS valor_unitario, "
                "CAST(COALESCE(i.valor_total, i.valor_vencedor) AS FLOAT64) AS valor_total, "
                "CAST(i.documento AS STRING) AS cnpj_vencedor "
                f"FROM `{dataset}.licitacao_item` i "
                f"{year_filter_items}"
            ),
        }

    # Legacy br_mides — minimal SELECT * fallback.
    year_filter = f"WHERE SAFE_CAST(ano AS INT64) BETWEEN {start_year} AND {end_year}"
    return {
        "licitacao.csv": f"SELECT * FROM `{dataset}.licitacao` {year_filter}",
        "contrato.csv": f"SELECT * FROM `{dataset}.contrato` {year_filter}",
        "item.csv": f"SELECT * FROM `{dataset}.item` {year_filter}",
    }


def _valid_cnpj(value: str) -> str:
    digits = strip_document(value)
    if len(digits) != 14:
        return ""
    return format_cnpj(digits)


class MidesPipeline(Pipeline):
    """ETL pipeline for municipal procurement data (MiDES / Base dos Dados)."""

    name = "mides"
    source_id = "mides"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)

        self._raw_bids: pd.DataFrame = pd.DataFrame()
        self._raw_contracts: pd.DataFrame = pd.DataFrame()
        self._raw_items: pd.DataFrame = pd.DataFrame()

        self.bids: list[dict[str, Any]] = []
        self.contracts: list[dict[str, Any]] = []
        self.items: list[dict[str, Any]] = []
        self.bid_company_rels: list[dict[str, Any]] = []
        self.contract_company_rels: list[dict[str, Any]] = []
        self.contract_bid_rels: list[dict[str, Any]] = []
        self.contract_item_rels: list[dict[str, Any]] = []

    def _read_df_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "mides"
        self._raw_bids = self._read_df_optional(src_dir / "licitacao.csv")
        if self._raw_bids.empty:
            self._raw_bids = self._read_df_optional(src_dir / "licitacao.parquet")

        self._raw_contracts = self._read_df_optional(src_dir / "contrato.csv")
        if self._raw_contracts.empty:
            self._raw_contracts = self._read_df_optional(src_dir / "contrato.parquet")

        self._raw_items = self._read_df_optional(src_dir / "item.csv")
        if self._raw_items.empty:
            self._raw_items = self._read_df_optional(src_dir / "item.parquet")

        if self._raw_bids.empty and self._raw_contracts.empty and self._raw_items.empty:
            logger.warning("[mides] no input files found in %s", src_dir)
            return

        if self.limit:
            self._raw_bids = self._raw_bids.head(self.limit)
            self._raw_contracts = self._raw_contracts.head(self.limit)
            self._raw_items = self._raw_items.head(self.limit)

        logger.info(
            "[mides] extracted bids=%d contracts=%d items=%d",
            len(self._raw_bids),
            len(self._raw_contracts),
            len(self._raw_items),
        )

    def transform(self) -> None:
        self._transform_bids()
        self._transform_contracts()
        self._transform_items()

    def _transform_bids(self) -> None:
        if self._raw_bids.empty:
            return

        bids: list[dict[str, Any]] = []
        bid_company_rels: list[dict[str, Any]] = []

        for _, row in self._raw_bids.iterrows():
            bid_id = row_pick(row, "municipal_bid_id", "licitacao_id", "id_licitacao", "id")
            process_number = row_pick(row, "process_number", "numero_processo", "numero")
            org_code = row_pick(row, "municipality_code", "cod_ibge", "codigo_ibge")
            org_name = row_pick(row, "municipality_name", "municipio", "nome_municipio")
            uf = row_pick(row, "uf", "estado")
            modality = row_pick(row, "modality", "modalidade")
            obj = normalize_name(row_pick(row, "object", "objeto", "descricao"))
            pub_date = parse_date(row_pick(row, "published_at", "data_publicacao", "data"))
            year = row_pick(row, "year", "ano")
            amount_estimated = cap_contract_value(
                parse_number_smart(
                    row_pick(row, "amount_estimated", "valor_estimado", "valor"),
                    default=None,
                ),
            )
            source_url = row_pick(row, "source_url", "url")

            if not bid_id:
                bid_id = _stable_id(process_number, org_code, obj[:180], pub_date)

            bids.append({
                "municipal_bid_id": bid_id,
                "process_number": process_number,
                "municipality_code": org_code,
                "municipality_name": org_name,
                "uf": uf,
                "modality": modality,
                "object": obj,
                "published_at": pub_date,
                "year": year,
                "amount_estimated": amount_estimated,
                "source_url": source_url,
                "source": "mides",
            })

            supplier_cnpj = _valid_cnpj(row_pick(
                row,
                "supplier_cnpj",
                "winner_cnpj",
                "cnpj_fornecedor",
                "cnpj_vencedor",
            ))
            if supplier_cnpj:
                bid_company_rels.append({
                    "cnpj": supplier_cnpj,
                    "target_key": bid_id,
                })

        self.bids = deduplicate_rows(bids, ["municipal_bid_id"])
        self.bid_company_rels = deduplicate_rows(bid_company_rels, ["cnpj", "target_key"])

    def _transform_contracts(self) -> None:
        if self._raw_contracts.empty:
            return

        contracts: list[dict[str, Any]] = []
        contract_company_rels: list[dict[str, Any]] = []
        contract_bid_rels: list[dict[str, Any]] = []

        for _, row in self._raw_contracts.iterrows():
            contract_id = row_pick(row, "municipal_contract_id", "contrato_id", "id_contrato", "id")
            number = row_pick(row, "contract_number", "numero_contrato", "numero")
            bid_ref = row_pick(row, "municipal_bid_id", "licitacao_id", "id_licitacao")
            process_number = row_pick(row, "process_number", "numero_processo")
            municipality_code = row_pick(row, "municipality_code", "cod_ibge", "codigo_ibge")
            municipality_name = row_pick(row, "municipality_name", "municipio", "nome_municipio")
            uf = row_pick(row, "uf", "estado")
            signed_at = parse_date(row_pick(row, "signed_at", "data_assinatura", "data"))
            obj = normalize_name(row_pick(row, "object", "objeto", "descricao"))
            amount = cap_contract_value(
                parse_number_smart(
                    row_pick(row, "amount", "valor", "valor_contrato"),
                    default=None,
                ),
            )
            source_url = row_pick(row, "source_url", "url")

            if not contract_id:
                contract_id = _stable_id(number, municipality_code, obj[:160], signed_at)

            contracts.append({
                "municipal_contract_id": contract_id,
                "contract_number": number,
                "process_number": process_number,
                "municipality_code": municipality_code,
                "municipality_name": municipality_name,
                "uf": uf,
                "signed_at": signed_at,
                "object": obj,
                "amount": amount,
                "source_url": source_url,
                "source": "mides",
            })

            supplier_cnpj = _valid_cnpj(
                row_pick(row, "supplier_cnpj", "cnpj_fornecedor", "cnpj_vencedor"),
            )
            if supplier_cnpj:
                contract_company_rels.append({
                    "cnpj": supplier_cnpj,
                    "target_key": contract_id,
                })

            if bid_ref:
                contract_bid_rels.append({
                    "source_key": contract_id,
                    "target_key": bid_ref,
                })

        self.contracts = deduplicate_rows(contracts, ["municipal_contract_id"])
        self.contract_company_rels = deduplicate_rows(contract_company_rels, ["cnpj", "target_key"])
        self.contract_bid_rels = deduplicate_rows(contract_bid_rels, ["source_key", "target_key"])

    def _transform_items(self) -> None:
        if self._raw_items.empty:
            return

        items: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        for _, row in self._raw_items.iterrows():
            contract_id = row_pick(row, "municipal_contract_id", "contrato_id", "id_contrato")
            bid_id = row_pick(row, "municipal_bid_id", "licitacao_id", "id_licitacao")

            item_id = row_pick(row, "municipal_item_id", "item_id", "id_item")
            item_number = row_pick(row, "item_number", "numero_item")
            description = normalize_name(row_pick(row, "description", "descricao", "objeto_item"))
            quantity = parse_number_smart(row_pick(row, "quantity", "quantidade"), default=None)
            unit_price = cap_contract_value(
                parse_number_smart(
                    row_pick(row, "unit_price", "valor_unitario"),
                    default=None,
                ),
            )
            total_price = cap_contract_value(
                parse_number_smart(
                    row_pick(row, "total_price", "valor_total", "valor"),
                    default=None,
                ),
            )

            if not item_id:
                item_id = _stable_id(contract_id, bid_id, item_number, description[:120])

            items.append({
                "municipal_item_id": item_id,
                "item_number": item_number,
                "description": description,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": total_price,
                "source": "mides",
            })

            if contract_id:
                rels.append({"source_key": contract_id, "target_key": item_id})

        self.items = deduplicate_rows(items, ["municipal_item_id"])
        self.contract_item_rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.bids:
            loader.load_nodes("MunicipalBid", self.bids, key_field="municipal_bid_id")

        if self.contracts:
            loader.load_nodes(
                "MunicipalContract",
                self.contracts,
                key_field="municipal_contract_id",
            )

        if self.items:
            loader.load_nodes("MunicipalBidItem", self.items, key_field="municipal_item_id")

        if self.bid_company_rels:
            companies = deduplicate_rows(
                [
                    {
                        "cnpj": row["cnpj"],
                        "razao_social": row["cnpj"],
                    }
                    for row in self.bid_company_rels
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", companies, key_field="cnpj")

            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.cnpj}) "
                "MATCH (b:MunicipalBid {municipal_bid_id: row.target_key}) "
                "MERGE (c)-[:MUNICIPAL_LICITOU]->(b)"
            )
            loader.run_query_with_retry(query, self.bid_company_rels)

        if self.contract_company_rels:
            companies = deduplicate_rows(
                [
                    {
                        "cnpj": row["cnpj"],
                        "razao_social": row["cnpj"],
                    }
                    for row in self.contract_company_rels
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", companies, key_field="cnpj")

            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.cnpj}) "
                "MATCH (mc:MunicipalContract {municipal_contract_id: row.target_key}) "
                "MERGE (c)-[:MUNICIPAL_VENCEU]->(mc)"
            )
            loader.run_query_with_retry(query, self.contract_company_rels)

        if self.contract_bid_rels:
            loader.load_relationships(
                rel_type="REFERENTE_A",
                rows=self.contract_bid_rels,
                source_label="MunicipalContract",
                source_key="municipal_contract_id",
                target_label="MunicipalBid",
                target_key="municipal_bid_id",
            )

        if self.contract_item_rels:
            loader.load_relationships(
                rel_type="TEM_ITEM",
                rows=self.contract_item_rels,
                source_label="MunicipalContract",
                source_key="municipal_contract_id",
                target_label="MunicipalBidItem",
                target_key="municipal_item_id",
            )

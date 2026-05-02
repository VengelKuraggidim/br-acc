from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    normalize_name,
    parse_date,
    parse_numeric_comma,
)

logger = logging.getLogger(__name__)

# URL canonica da PGFN pra carimbar source_url. Fonte nao expoe deep-link por
# inscricao — usamos a landing page do dataset de divida ativa. Igual ao padrao
# do transparencia.py (bulk CSVs sem per-record URL).
_PGFN_SOURCE_URL = (
    "https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/"
    "dados-abertos"
)


class PgfnPipeline(Pipeline):
    """ETL pipeline for PGFN active tax debt (divida ativa da Uniao).

    Ingests company-only records (CNPJ). Person CPFs are pre-masked by PGFN
    and cannot be matched to existing Person nodes.
    Only PRINCIPAL debtors are loaded to avoid double-counting.
    """

    name = "pgfn"
    source_id = "pgfn"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._csv_files: list[Path] = []
        self.finances: list[dict[str, Any]] = []
        self.relationships: list[dict[str, Any]] = []

    def extract(self) -> None:
        pgfn_dir = Path(self.data_dir) / "pgfn"
        if not pgfn_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, pgfn_dir)
            return
        self._csv_files = sorted(pgfn_dir.glob("arquivo_lai_SIDA_*_*.csv"))
        if not self._csv_files:
            logger.warning("[%s] No PGFN CSV files found in %s", self.name, pgfn_dir)
            return
        logger.info("[pgfn] Found %d CSV files to process", len(self._csv_files))

    def transform(self) -> None:
        finances: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []
        skipped_pf = 0
        skipped_corresponsavel = 0
        skipped_bad_cnpj = 0
        seen_inscricoes: set[str] = set()
        total_rows_scanned = 0

        for csv_file in self._csv_files:
            logger.info("[pgfn] Processing %s", csv_file.name)

            for chunk in pd.read_csv(
                csv_file,
                dtype=str,
                delimiter=";",
                encoding="latin-1",
                keep_default_na=False,
                chunksize=100_000,
            ):
                total_rows_scanned += len(chunk)
                # Filter to company principal debtors using vectorized ops
                mask_pj = chunk["TIPO_PESSOA"].str.contains("jur", case=False, na=False)
                mask_principal = chunk["TIPO_DEVEDOR"] == "PRINCIPAL"
                skipped_pf += int((~mask_pj).sum())
                skipped_corresponsavel += int((mask_pj & ~mask_principal).sum())

                filtered = chunk.loc[mask_pj & mask_principal]
                if filtered.empty:
                    continue

                # Vectorize document/inscricao filters before per-row work.
                cnpj_digits = filtered["CPF_CNPJ"].str.replace(r"\D", "", regex=True)
                mask_cnpj_ok = cnpj_digits.str.len() == 14
                skipped_bad_cnpj += int((~mask_cnpj_ok).sum())

                inscricao_clean = filtered["NUMERO_INSCRICAO"].str.strip()
                mask_has_inscricao = inscricao_clean.ne("")

                kept = filtered.loc[mask_cnpj_ok & mask_has_inscricao].copy()
                if kept.empty:
                    continue

                kept_digits = cnpj_digits.loc[kept.index]
                kept_inscricao = inscricao_clean.loc[kept.index]

                # In-chunk dedup (keep first), then drop already-seen across chunks.
                first_in_chunk = ~kept_inscricao.duplicated(keep="first")
                kept = kept.loc[first_in_chunk]
                kept_digits = kept_digits.loc[kept.index]
                kept_inscricao = kept_inscricao.loc[kept.index]

                cross_chunk_mask = ~kept_inscricao.isin(seen_inscricoes)
                kept = kept.loc[cross_chunk_mask]
                if kept.empty:
                    continue
                kept_digits = kept_digits.loc[kept.index]
                kept_inscricao = kept_inscricao.loc[kept.index]

                if self.limit:
                    remaining = self.limit - len(finances)
                    if remaining <= 0:
                        break
                    if len(kept) > remaining:
                        kept = kept.iloc[:remaining]
                        kept_digits = kept_digits.iloc[:remaining]
                        kept_inscricao = kept_inscricao.iloc[:remaining]

                seen_inscricoes.update(kept_inscricao.tolist())

                cnpj_formatted = (
                    kept_digits.str.slice(0, 2)
                    + "."
                    + kept_digits.str.slice(2, 5)
                    + "."
                    + kept_digits.str.slice(5, 8)
                    + "/"
                    + kept_digits.str.slice(8, 12)
                    + "-"
                    + kept_digits.str.slice(12, 14)
                )
                finance_ids = "pgfn_" + kept_inscricao
                values = kept["VALOR_CONSOLIDADO"].map(parse_numeric_comma)
                dates = kept["DATA_INSCRICAO"].astype(str).map(parse_date)
                nomes = kept["NOME_DEVEDOR"].astype(str).map(normalize_name)
                situacoes = kept["SITUACAO_INSCRICAO"].str.strip()
                receitas = kept["RECEITA_PRINCIPAL"].str.strip()
                ajuizados = kept["INDICADOR_AJUIZADO"].str.strip()

                finance_frame = pd.DataFrame({
                    "finance_id": finance_ids.to_numpy(),
                    "type": "divida_ativa",
                    "inscription_number": kept_inscricao.to_numpy(),
                    "value": values.to_numpy(),
                    "date": dates.to_numpy(),
                    "situation": situacoes.to_numpy(),
                    "revenue_type": receitas.to_numpy(),
                    "court_action": ajuizados.to_numpy(),
                    "source": "pgfn",
                })
                rel_frame = pd.DataFrame({
                    "source_key": cnpj_formatted.to_numpy(),
                    "target_key": finance_ids.to_numpy(),
                    "value": values.to_numpy(),
                    "date": dates.to_numpy(),
                    "company_name": nomes.to_numpy(),
                })

                finances.extend(finance_frame.to_dict(orient="records"))
                relationships.extend(rel_frame.to_dict(orient="records"))

                if self.limit and len(finances) >= self.limit:
                    break
            if self.limit and len(finances) >= self.limit:
                break

        self.finances = finances
        self.relationships = relationships
        self.rows_in = total_rows_scanned

        logger.info(
            "[pgfn] Transformed %d Finance nodes, %d relationships",
            len(self.finances),
            len(self.relationships),
        )
        logger.info(
            "[pgfn] Skipped: %d person (masked CPF), %d co-responsible, %d bad CNPJ",
            skipped_pf,
            skipped_corresponsavel,
            skipped_bad_cnpj,
        )

    def _stamp(self, row: dict[str, Any], *, record_id: object) -> dict[str, Any]:
        """Shorthand pra ``attach_provenance`` com URL canonica da PGFN.

        Igual ao padrao de ``transparencia.py::_stamp``: fonte nao tem deep-
        link por inscricao, entao todo row carimba o mesmo ``source_url``.
        """
        return self.attach_provenance(
            row,
            record_id=record_id,
            record_url=_PGFN_SOURCE_URL,
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.finances:
            finance_rows = [
                self._stamp(f, record_id=f["finance_id"]) for f in self.finances
            ]
            loaded = loader.load_nodes("Finance", finance_rows, key_field="finance_id")
            self.rows_loaded += loaded
            logger.info("[pgfn] Loaded %d Finance nodes", loaded)

        if self.relationships:
            rel_rows = [
                self._stamp(r, record_id=r["target_key"]) for r in self.relationships
            ]
            query = (
                "UNWIND $rows AS row "
                "MERGE (c:Company {cnpj: row.source_key}) "
                "ON CREATE SET c.razao_social = row.company_name, c.name = row.company_name "
                "WITH c, row "
                "MATCH (f:Finance {finance_id: row.target_key}) "
                "MERGE (c)-[r:DEVE]->(f) "
                "SET r.value = row.value, "
                "    r.date = row.date, "
                "    r.source_id = row.source_id, "
                "    r.source_record_id = row.source_record_id, "
                "    r.source_url = row.source_url, "
                "    r.ingested_at = row.ingested_at, "
                "    r.run_id = row.run_id"
            )
            loaded = loader.run_query_with_retry(query, rel_rows, batch_size=2000)
            logger.info("[pgfn] Loaded %d DEVE relationships", loaded)

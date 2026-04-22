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
    format_cnpj,
    normalize_name,
    parse_date,
    parse_numeric_comma,
    strip_document,
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

                filtered = chunk[mask_pj & mask_principal]

                for _, row in filtered.iterrows():
                    cnpj_raw = str(row["CPF_CNPJ"]).strip()
                    digits = strip_document(cnpj_raw)
                    if len(digits) != 14:
                        skipped_bad_cnpj += 1
                        continue

                    inscricao = str(row["NUMERO_INSCRICAO"]).strip()
                    if not inscricao or inscricao in seen_inscricoes:
                        continue
                    seen_inscricoes.add(inscricao)

                    cnpj_formatted = format_cnpj(cnpj_raw)
                    finance_id = f"pgfn_{inscricao}"
                    valor = parse_numeric_comma(row["VALOR_CONSOLIDADO"])
                    date = parse_date(str(row["DATA_INSCRICAO"]))
                    nome = normalize_name(str(row["NOME_DEVEDOR"]))
                    situacao = str(row["SITUACAO_INSCRICAO"]).strip()
                    receita = str(row["RECEITA_PRINCIPAL"]).strip()
                    ajuizado = str(row["INDICADOR_AJUIZADO"]).strip()

                    finances.append({
                        "finance_id": finance_id,
                        "type": "divida_ativa",
                        "inscription_number": inscricao,
                        "value": valor,
                        "date": date,
                        "situation": situacao,
                        "revenue_type": receita,
                        "court_action": ajuizado,
                        "source": "pgfn",
                    })

                    relationships.append({
                        "source_key": cnpj_formatted,
                        "target_key": finance_id,
                        "value": valor,
                        "date": date,
                        "company_name": nome,
                    })

                    if self.limit and len(finances) >= self.limit:
                        break
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

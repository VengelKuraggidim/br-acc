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
from bracc_etl.transforms import deduplicate_rows, normalize_name

logger = logging.getLogger(__name__)

# Base dos Dados mirror of the STF "Corte Aberta" decisions table.
# Ingestion requires an authenticated GCP billing project — this is a hard
# external requirement (not a bypassable paywall), so fetch_to_disk fails
# open (returns []) when no project is available, letting public-mode
# bootstrap skip gracefully.
_BQ_TABLE = "basedosdados.br_stf_corte_aberta.decisoes"
_BQ_COLUMNS = (
    "ano",
    "classe",
    "numero",
    "relator",
    "link",
    "subgrupo_andamento",
    "andamento",
    "observacao_andamento_decisao",
    "modalidade_julgamento",
    "tipo_julgamento",
    "meio_tramitacao",
    "indicador_tramitacao",
    "assunto_processo",
    "ramo_direito",
    "data_autuacao",
    "data_decisao",
    "data_baixa_processo",
)
_BQ_PAGE_SIZE = 100_000


def _generate_case_id(case_class: str, case_number: str, year: str) -> str:
    """Deterministic ID from case class + number + year."""
    raw = f"{case_class}:{case_number}:{year}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class StfPipeline(Pipeline):
    """ETL pipeline for STF (Supremo Tribunal Federal) decisions.

    Data source: BigQuery table basedosdados.br_stf_corte_aberta.decisoes,
    pre-exported to CSV via download script.
    """

    name = "stf"
    source_id = "stf"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.cases: list[dict[str, Any]] = []
        self.rapporteur_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        stf_dir = Path(self.data_dir) / "stf"
        self._raw = pd.read_csv(
            stf_dir / "decisoes.csv",
            dtype=str,
            keep_default_na=False,
        )

    def transform(self) -> None:
        cases: list[dict[str, Any]] = []
        rapporteur_rels: list[dict[str, Any]] = []

        for _idx, row in self._raw.iterrows():
            case_class = str(row.get("classe", "")).strip()
            case_number = str(row.get("numero", "")).strip()
            year = str(row.get("ano", "")).strip()

            if not case_class or not case_number or not year:
                continue

            case_id = _generate_case_id(case_class, case_number, year)
            rapporteur_raw = str(row.get("relator", "")).strip()
            rapporteur = normalize_name(rapporteur_raw)
            decision_type = str(
                row.get("tipo_decisao", "") or row.get("andamento", "")
            ).strip()
            decision_date = str(row.get("data_decisao", "")).strip()
            subject = str(
                row.get("assunto", "") or row.get("assunto_processo", "")
            ).strip()
            origin = str(
                row.get("procedencia", "") or row.get("ramo_direito", "")
            ).strip()

            case: dict[str, Any] = {
                "case_id": case_id,
                "case_class": case_class,
                "case_number": case_number,
                "year": year,
                "rapporteur": rapporteur,
                "decision_type": decision_type,
                "decision_date": decision_date,
                "subject": subject,
                "origin": origin,
                "source": "stf",
            }
            cases.append(case)

            if rapporteur:
                rapporteur_rels.append(
                    {
                        "source_key": rapporteur,
                        "target_key": case_id,
                    }
                )

        self.cases = deduplicate_rows(cases, ["case_id"])
        self.rapporteur_rels = rapporteur_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.cases:
            loader.load_nodes("LegalCase", self.cases, key_field="case_id")

        if self.rapporteur_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {name: row.source_key}) "
                "MATCH (lc:LegalCase {case_id: row.target_key}) "
                "MERGE (p)-[:RELATOR_DE]->(lc)"
            )
            loader.run_query_with_retry(query, self.rapporteur_rels)


# ────────────────────────────────────────────────────────────────────
# Acquisition helper — Base dos Dados (BigQuery) export to CSV
# ────────────────────────────────────────────────────────────────────


def fetch_to_disk(
    output_dir: Path,
    *,
    billing_project: str | None = None,
    date: str | None = None,  # noqa: ARG001 — accepted for bootstrap symmetry
    skip_existing: bool = True,
) -> list[Path]:
    """Download STF decisions from Base dos Dados to ``<output_dir>/decisoes.csv``.

    Requires ``billing_project`` — STF does not publish a stable bulk
    endpoint, so the only automated open source is the Base dos Dados mirror
    on BigQuery (``basedosdados.br_stf_corte_aberta.decisoes``). Without a
    billing project the helper logs a clear skip message and returns ``[]``
    so the bootstrap contract can proceed in public mode.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    dest = output_dir / "decisoes.csv"

    if skip_existing and dest.exists() and dest.stat().st_size > 0:
        logger.info("[stf] skipping (exists): %s", dest)
        return [dest]

    if not billing_project:
        logger.warning(
            "[stf] no --billing-project provided; STF decisions are only "
            "available via Base dos Dados on BigQuery. Skipping. "
            "To ingest, rerun with --billing-project <gcp-project-id>.",
        )
        return []

    try:
        from google.cloud import bigquery  # type: ignore[import-not-found]
    except ImportError:
        logger.warning(
            "[stf] google-cloud-bigquery not installed; "
            "`pip install '.[bigquery]'` (in etl/) and pass --billing-project.",
        )
        return []

    client = bigquery.Client(project=billing_project)
    schema_fields = [bigquery.SchemaField(c, "STRING") for c in _BQ_COLUMNS]

    logger.info(
        "[stf] streaming %s (%d columns, page_size=%d) -> %s",
        _BQ_TABLE,
        len(_BQ_COLUMNS),
        _BQ_PAGE_SIZE,
        dest,
    )

    if dest.exists():
        dest.unlink()
    rows_written = 0
    for i, chunk_df in enumerate(
        client.list_rows(
            _BQ_TABLE,
            selected_fields=schema_fields,
            page_size=_BQ_PAGE_SIZE,
        ).to_dataframe_iterable(),
    ):
        chunk_df.to_csv(dest, mode="a", header=(i == 0), index=False)
        rows_written += len(chunk_df)
        if i == 0 or rows_written % (_BQ_PAGE_SIZE * 5) == 0:
            logger.info("[stf]   rows written: %d", rows_written)

    logger.info("[stf] wrote %d rows → %s", rows_written, dest)
    return [dest] if rows_written > 0 else []

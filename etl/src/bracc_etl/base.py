import logging
import os
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

from neo4j import Driver

from bracc_etl.provenance import primary_url_for

logger = logging.getLogger(__name__)


class Pipeline(ABC):
    """Base class for all ETL pipelines."""

    name: str
    source_id: str

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        neo4j_database: str | None = None,
        history: bool = False,
    ) -> None:
        self.driver = driver
        self.data_dir = data_dir
        self.limit = limit
        self.chunk_size = chunk_size
        self.neo4j_database = neo4j_database or os.getenv("NEO4J_DATABASE", "neo4j")
        self.history = history
        self.rows_in: int = 0
        self.rows_loaded: int = 0
        source_key = getattr(self, "source_id", getattr(self, "name", "unknown_source"))
        self.run_id = f"{source_key}_{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S')}"
        self._provenance_ingested_at = datetime.now(tz=UTC).isoformat()
        self._primary_url_cache: str | None = None

    def attach_provenance(
        self,
        row: dict[str, Any],
        *,
        record_id: object,
        record_url: str | None = None,
    ) -> dict[str, Any]:
        """Stamp a node or relationship row with the five provenance fields.

        Pipelines MUST route every dict destined for ``Neo4jBatchLoader``
        through this helper so ``source_id`` / ``source_record_id`` /
        ``source_url`` / ``ingested_at`` / ``run_id`` are never forgotten.
        See ``docs/provenance.md`` for the contract.

        ``record_url`` is preferred when the source exposes a deep-link;
        otherwise the ``primary_url`` from ``docs/source_registry_br_v1.csv``
        is used. ``record_id`` is coerced to string (empty string when falsy).
        """
        source_url = (record_url or self._get_primary_url() or "").strip()
        if not source_url.startswith("http"):
            raise ValueError(
                f"[{self.source_id}] attach_provenance: no valid source_url "
                f"(record_url={record_url!r}, primary_url={self._primary_url_cache!r}); "
                "ensure docs/source_registry_br_v1.csv has a primary_url for this source",
            )
        normalized_record_id = "" if record_id in (None, "") else str(record_id)
        return {
            **row,
            "source_id": self.source_id,
            "source_record_id": normalized_record_id,
            "source_url": source_url,
            "ingested_at": self._provenance_ingested_at,
            "run_id": self.run_id,
        }

    def _get_primary_url(self) -> str:
        if self._primary_url_cache is None:
            self._primary_url_cache = primary_url_for(self.source_id)
        return self._primary_url_cache

    @abstractmethod
    def extract(self) -> None:
        """Download raw data from source."""

    @abstractmethod
    def transform(self) -> None:
        """Normalize, deduplicate, and prepare data for loading."""

    @abstractmethod
    def load(self) -> None:
        """Load transformed data into Neo4j."""

    def run(self) -> None:
        """Execute the full ETL pipeline."""
        started_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._upsert_ingestion_run(status="running", started_at=started_at)
        try:
            logger.info("[%s] Starting extraction...", self.name)
            self.extract()
            logger.info("[%s] Starting transformation...", self.name)
            self.transform()
            logger.info("[%s] Starting load...", self.name)
            self.load()
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="loaded",
                started_at=started_at,
                finished_at=finished_at,
            )
            logger.info("[%s] Pipeline complete.", self.name)
        except Exception as exc:
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="quality_fail",
                started_at=started_at,
                finished_at=finished_at,
                error=str(exc)[:1000],
            )
            raise

    def _upsert_ingestion_run(
        self,
        *,
        status: str,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        """Persist ingestion run state for operational traceability."""
        source_id = getattr(self, "source_id", getattr(self, "name", "unknown_source"))
        query = (
            "MERGE (r:IngestionRun {run_id: $run_id}) "
            "SET r.source_id = $source_id, "
            "    r.status = $status, "
            "    r.started_at = coalesce($started_at, r.started_at), "
            "    r.finished_at = coalesce($finished_at, r.finished_at), "
            "    r.error = coalesce($error, r.error), "
            "    r.rows_in = $rows_in, "
            "    r.rows_loaded = $rows_loaded"
        )
        run_id = getattr(self, "run_id", f"{source_id}_manual")
        params = {
            "run_id": run_id,
            "source_id": source_id,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "error": error,
            "rows_in": self.rows_in,
            "rows_loaded": self.rows_loaded,
        }
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                session.run(query, params)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] failed to persist IngestionRun: %s", self.name, exc)

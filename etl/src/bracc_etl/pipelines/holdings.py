from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    format_cnpj,
    strip_document,
)

logger = logging.getLogger(__name__)

# Brasil.IO socios-brasil dataset — derives company-company holdings (CNPJ to
# CNPJ ownership) from the RFB partner CSVs. The pipeline reads
# ``holding.csv.gz`` directly so we just stream the upstream artefact.
HOLDING_PRIMARY_URL = "https://data.brasil.io/dataset/socios-brasil/holding.csv.gz"
# S3 mirror — used when the friendly-cdn URL 404/503s, which has happened a
# few times during Brasil.IO maintenance windows.
HOLDING_FALLBACK_URL = (
    "https://brasil-io-public.s3.amazonaws.com/dataset/socios-brasil/holding.csv.gz"
)
_DEFAULT_TIMEOUT = 600  # seconds — full file is ~50-100 MB compressed.


def fetch_to_disk(
    output_dir: Path,
    *,
    date: str | None = None,
    skip_existing: bool = True,
    timeout: int = _DEFAULT_TIMEOUT,
) -> list[Path]:
    """Download the Brasil.IO ``holding.csv.gz`` file to ``output_dir``.

    The HoldingsPipeline reads ``holding.csv.gz`` (or ``holding.csv``) from
    ``data/holdings/``; we mirror that by streaming the gzipped CSV from
    Brasil.IO, falling back to the S3 mirror on transient errors.

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    date:
        Accepted for API symmetry with other ``fetch_to_disk`` callers; the
        Brasil.IO dataset is a rolling snapshot without a date selector, so
        this argument is informational only and not appended to the URL.
    skip_existing:
        When True (default) and the destination already exists with non-zero
        bytes, the download is skipped — useful for bootstrap re-runs.
    timeout:
        httpx total timeout in seconds. Default 600s.

    Returns
    -------
    List of paths actually written (1 element on success, empty on failure).
    """
    del date  # not used upstream; accepted for caller symmetry.
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dest = output_dir / "holding.csv.gz"
    if skip_existing and dest.exists() and dest.stat().st_size > 0:
        logger.info(
            "[holdings] %s already exists (%d bytes), skipping download",
            dest, dest.stat().st_size,
        )
        return [dest]

    written = _stream_to(HOLDING_PRIMARY_URL, dest, timeout=timeout)
    if written is None:
        logger.warning(
            "[holdings] primary URL failed; trying S3 mirror %s",
            HOLDING_FALLBACK_URL,
        )
        written = _stream_to(HOLDING_FALLBACK_URL, dest, timeout=timeout)

    if written is None:
        logger.error("[holdings] both download URLs failed")
        return []

    logger.info(
        "[holdings] wrote %s (%d bytes)", written, written.stat().st_size,
    )
    return [written]


def _stream_to(url: str, dest: Path, *, timeout: int) -> Path | None:
    """Stream ``url`` into ``dest``. Returns the path on success, else None."""
    partial = dest.with_suffix(dest.suffix + ".partial")
    try:
        with httpx.stream(
            "GET", url, follow_redirects=True, timeout=timeout,
        ) as resp:
            resp.raise_for_status()
            with open(partial, "wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=65_536):
                    fh.write(chunk)
        partial.rename(dest)
        return dest
    except httpx.HTTPError as exc:
        logger.warning("[holdings] download failed (%s): %s", url, exc)
        if partial.exists():
            with contextlib.suppress(OSError):
                partial.unlink()
        return None


class HoldingsPipeline(Pipeline):
    """ETL pipeline for Brasil.IO company-company ownership (holdings) data.

    Creates HOLDING_DE relationships between existing Company nodes.
    A HOLDING_DE relationship means Company A holds shares in Company B
    (Company A is a corporate shareholder of Company B).
    """

    name = "holdings"
    source_id = "brasil_io_holdings"

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
        self.holding_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        holdings_dir = Path(self.data_dir) / "holdings"

        # Try gzipped CSV first, then plain CSV
        gz_path = holdings_dir / "holding.csv.gz"
        csv_path = holdings_dir / "holding.csv"

        read_opts: dict[str, Any] = {
            "dtype": str,
            "keep_default_na": False,
        }

        if gz_path.exists():
            logger.info("[holdings] Reading %s", gz_path)
            self._raw = pd.read_csv(gz_path, compression="gzip", **read_opts)
        elif csv_path.exists():
            logger.info("[holdings] Reading %s", csv_path)
            self._raw = pd.read_csv(csv_path, **read_opts)
        else:
            logger.warning(
                "[holdings] No holding.csv or holding.csv.gz found at %s", holdings_dir
            )
            return

        if self.limit:
            self._raw = self._raw.head(self.limit)

        logger.info("[holdings] Extracted %d rows", len(self._raw))

    def transform(self) -> None:
        rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            # Support both column naming conventions
            cnpj_empresa_raw = str(
                row.get("cnpj_empresa") or row.get("cnpj") or ""
            ).strip()
            cnpj_socia_raw = str(
                row.get("cnpj_socia") or row.get("holding_cnpj") or ""
            ).strip()

            # Validate both CNPJs have exactly 14 digits
            digits_empresa = strip_document(cnpj_empresa_raw)
            digits_socia = strip_document(cnpj_socia_raw)

            if len(digits_empresa) != 14 or len(digits_socia) != 14:
                continue

            cnpj_empresa = format_cnpj(digits_empresa)
            cnpj_socia = format_cnpj(digits_socia)

            # Skip self-holding (company owns itself)
            if cnpj_empresa == cnpj_socia:
                continue

            rels.append({
                "source_key": cnpj_socia,
                "target_key": cnpj_empresa,
            })

        self.holding_rels = rels

        logger.info(
            "[holdings] Transformed %d HOLDING_DE relationships",
            len(self.holding_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.holding_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (holder:Company {cnpj: row.source_key}) "
                "MATCH (held:Company {cnpj: row.target_key}) "
                "MERGE (holder)-[:HOLDING_DE]->(held)"
            )
            loaded = loader.run_query_with_retry(query, self.holding_rels)
            logger.info("[holdings] Loaded %d HOLDING_DE relationships", loaded)

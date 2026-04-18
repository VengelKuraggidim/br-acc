from __future__ import annotations

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
    deduplicate_rows,
    normalize_name,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Module-level constants + fetch_to_disk (public bulk download).
# --------------------------------------------------------------------------
#
# The US Treasury OFAC Specially Designated Nationals list is published as
# a header-less CSV at a legacy URL that 302-redirects to the current
# sanctionslistservice.ofac.treas.gov endpoint. No authentication required.
#
# Primary URL (redirects to the live export):
#   https://www.treasury.gov/ofac/downloads/sdn.csv
# Authoritative (post-redirect) endpoint:
#   https://sanctionslistservice.ofac.treas.gov/api/publicationpreview/exports/sdn.csv
#
# The file is ~7-10 MB (~11k rows) and has no header row; OfacPipeline
# assigns column names positionally from SDN_COLUMNS above.

OFAC_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"


def fetch_to_disk(
    output_dir: Path,
    limit: int | None = None,
    url: str = OFAC_SDN_URL,
    timeout: float = 120.0,
) -> list[Path]:
    """Download the OFAC SDN CSV to ``output_dir/sdn.csv``.

    The file is header-less; OfacPipeline.extract() reads it with
    positional column names from SDN_COLUMNS. fetch_to_disk therefore
    preserves the raw bytes when possible, and only rewrites the file
    line-by-line when ``limit`` is set (to preserve the header-less
    layout exactly).

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    limit:
        If set, truncate the downloaded CSV to the first N data lines.
        Useful for smoke tests.
    url:
        Override the source URL (default: OFAC_SDN_URL).
    timeout:
        HTTP timeout in seconds.

    Returns
    -------
    List with the absolute path of the CSV written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "sdn.csv"

    logger.info("[ofac.fetch_to_disk] GET %s", url)
    with httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "br-acc/bracc-etl download_ofac (httpx)"},
    ) as client:
        resp = client.get(url, timeout=timeout)
        resp.raise_for_status()
        body = resp.content

    if limit is not None:
        # SDN is header-less, so the first `limit` lines ARE data rows.
        text = body.decode("latin-1", errors="replace")
        lines = text.splitlines(keepends=True)
        kept = "".join(lines[:limit])
        csv_path.write_bytes(kept.encode("latin-1", errors="replace"))
        logger.info(
            "[ofac.fetch_to_disk] wrote %d lines (limit=%d) to %s",
            min(len(lines), limit), limit, csv_path,
        )
    else:
        csv_path.write_bytes(body)
        logger.info(
            "[ofac.fetch_to_disk] wrote %d bytes to %s", len(body), csv_path,
        )

    return [csv_path.resolve()]

# OFAC SDN CSV has no header row. Column names assigned positionally.
SDN_COLUMNS = [
    "ent_num",
    "sdn_name",
    "sdn_type",
    "program",
    "title",
    "call_sign",
    "vess_type",
    "tonnage",
    "grt",
    "vess_flag",
    "vess_owner",
    "remarks",
]

# SDN types we care about
SDN_TYPE_INDIVIDUAL = "individual"
SDN_TYPE_ENTITY = "entity"
VALID_SDN_TYPES = {SDN_TYPE_INDIVIDUAL, SDN_TYPE_ENTITY}


def _clean_sdn_type(raw: str) -> str:
    """Normalize SDN_Type field (strip whitespace, dashes, lowercase)."""
    cleaned = raw.strip().strip("-").strip().lower()
    return cleaned


class OfacPipeline(Pipeline):
    """ETL pipeline for OFAC SDN (US Treasury Specially Designated Nationals) data.

    Loads all SDN entries as InternationalSanction nodes.
    Matching to existing Company/Person nodes is done separately
    via entity resolution.
    """

    name = "ofac"
    source_id = "ofac_sdn"

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
        self.sanctions: list[dict[str, Any]] = []

    def extract(self) -> None:
        ofac_dir = Path(self.data_dir) / "ofac"
        csv_path = ofac_dir / "sdn.csv"

        if not csv_path.exists():
            logger.warning("[ofac] sdn.csv not found at %s", csv_path)
            return

        logger.info("[ofac] Reading %s", csv_path)
        self._raw = pd.read_csv(
            csv_path,
            header=None,
            names=SDN_COLUMNS,
            dtype=str,
            encoding="utf-8",
            keep_default_na=False,
            on_bad_lines="skip",
        )

        if self.limit:
            self._raw = self._raw.head(self.limit)

        logger.info("[ofac] Extracted %d rows", len(self._raw))

    def transform(self) -> None:
        sanctions: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            ent_num = str(row["ent_num"]).strip()
            if not ent_num:
                continue

            sdn_type = _clean_sdn_type(str(row["sdn_type"]))
            if sdn_type not in VALID_SDN_TYPES:
                continue

            name_raw = str(row["sdn_name"]).strip()
            if not name_raw:
                continue

            sanctions.append({
                "sanction_id": f"ofac_{ent_num}",
                "name": normalize_name(name_raw),
                "original_name": name_raw,
                "sdn_type": sdn_type,
                "program": str(row["program"]).strip(),
                "title": str(row["title"]).strip(),
                "remarks": str(row["remarks"]).strip(),
                "source": "ofac_sdn",
            })

        self.sanctions = deduplicate_rows(sanctions, ["sanction_id"])

        logger.info(
            "[ofac] Transformed %d InternationalSanction nodes",
            len(self.sanctions),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.sanctions:
            loaded = loader.load_nodes(
                "InternationalSanction", self.sanctions, key_field="sanction_id"
            )
            logger.info("[ofac] Loaded %d InternationalSanction nodes", loaded)

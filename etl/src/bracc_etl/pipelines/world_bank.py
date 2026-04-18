from __future__ import annotations

import csv
import hashlib
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


def _make_debarment_id(firm_name: str, country: str, from_date: str) -> str:
    """Deterministic ID from firm name + country + from_date."""
    raw = f"{firm_name}|{country}|{from_date}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# --------------------------------------------------------------------------
# Module-level fetch_to_disk: World Bank "Debarred Firms & Individuals" API.
# --------------------------------------------------------------------------
#
# The public listing page
# (https://www.worldbank.org/en/projects-operations/procurement/debarred-firms)
# embeds a small jQuery call against the apigwext gateway. The gateway
# requires an ``apikey`` header; the key used here is the same one the
# public page ships with its rendered HTML (look for ``propApiKey``). No
# other authentication is required.
#
# Endpoint:
#   https://apigwext.worldbank.org/dvsvc/v1.0/json/APPLICATION/ADOBE_EXPRNCE_MGR/FIRM/SANCTIONED_FIRM
#
# The response is a single JSON document — ``{"response": {"ZPROCSUPP":[...]}}`` —
# containing the full debarred-firm universe (~900 records). We flatten it
# into the "new API format" column names the pipeline's ``extract()`` already
# recognises (``SUPP_NAME``, ``COUNTRY_NAME``, ``DEBAR_FROM_DATE``,
# ``DEBAR_TO_DATE``, ``DEBAR_REASON``) and write it as UTF-8 CSV at
# ``debarred.csv``.

_WB_API_URL = (
    "https://apigwext.worldbank.org/dvsvc/v1.0/json/APPLICATION/"
    "ADOBE_EXPRNCE_MGR/FIRM/SANCTIONED_FIRM"
)
# Public API key embedded in the World Bank debarred-firms page markup. If
# the upstream rotates it, scrape ``propApiKey`` from the HTML source of
# https://www.worldbank.org/en/projects-operations/procurement/debarred-firms
# and update this constant.
_WB_PUBLIC_API_KEY = "z9duUaFUiEUYSHs97CU38fcZO7ipOPvm"
_WB_HTTP_TIMEOUT = 120.0

# Column order for the output CSV. Matches the "new API format" branch of
# WorldBankPipeline.transform().
_WB_CSV_COLUMNS: list[str] = [
    "SUPP_NAME",
    "COUNTRY_NAME",
    "DEBAR_FROM_DATE",
    "DEBAR_TO_DATE",
    "DEBAR_REASON",
    "SUPP_TYPE_CODE",
    "SUPP_CITY",
    "SUPP_ADDR",
    "INELIGIBLY_STATUS",
]


def fetch_to_disk(
    output_dir: Path | str,
    url: str = _WB_API_URL,
    api_key: str = _WB_PUBLIC_API_KEY,
    limit: int | None = None,
    timeout: float = _WB_HTTP_TIMEOUT,
) -> list[Path]:
    """Download the World Bank debarred-firms list to ``debarred.csv``.

    Hits the public Adobe Experience Manager JSON endpoint used by
    worldbank.org, flattens the payload and writes UTF-8 CSV with column
    names matching the "new API format" branch of
    ``WorldBankPipeline.transform``.

    Args:
        output_dir: Destination directory. Created if missing.
        url: Override for the JSON API URL.
        api_key: Override for the ``apikey`` request header (public key by
            default — see module docstring).
        limit: If set, keep only the first N records.
        timeout: HTTP timeout in seconds.

    Returns:
        List with a single path: ``<output_dir>/debarred.csv``.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "debarred.csv"

    logger.info(
        "[world_bank.fetch_to_disk] GET %s (limit=%s) -> %s",
        url, limit, out_path,
    )

    with httpx.Client(
        timeout=timeout,
        follow_redirects=True,
        headers={
            "apikey": api_key,
            "User-Agent": "br-acc/bracc-etl download_world_bank (httpx)",
            "Accept": "application/json",
        },
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
        payload = resp.json()

    items = (
        payload.get("response", {}).get("ZPROCSUPP")
        or payload.get("ZPROCSUPP")
        or []
    )
    if not isinstance(items, list):
        raise RuntimeError(
            "World Bank API returned unexpected shape: "
            f"{type(items).__name__} (expected list)"
        )
    if limit is not None:
        items = items[:limit]

    logger.info("[world_bank.fetch_to_disk] Received %d records", len(items))

    with open(out_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=_WB_CSV_COLUMNS, extrasaction="ignore",
        )
        writer.writeheader()
        for item in items:
            row = {col: "" for col in _WB_CSV_COLUMNS}
            for col in _WB_CSV_COLUMNS:
                v = item.get(col)
                if v is None:
                    continue
                row[col] = str(v).strip()
            writer.writerow(row)

    logger.info(
        "[world_bank.fetch_to_disk] Wrote %d rows to %s (%.1f KB)",
        len(items), out_path, out_path.stat().st_size / 1024,
    )
    return [out_path.resolve()]


class WorldBankPipeline(Pipeline):
    """ETL pipeline for World Bank Debarred Firms & Individuals.

    Data source: World Bank Group sanctions list (CSV).
    Loads InternationalSanction nodes with source_list='WORLD_BANK'.
    """

    name = "world_bank"
    source_id = "world_bank"

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
        wb_dir = Path(self.data_dir) / "world_bank"
        csv_path = wb_dir / "debarred.csv"

        if not csv_path.exists():
            logger.warning("[world_bank] debarred.csv not found at %s", csv_path)
            return

        self._raw = pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,
        )

        if self.limit:
            self._raw = self._raw.head(self.limit)

        logger.info("[world_bank] Extracted %d rows", len(self._raw))

    def transform(self) -> None:
        sanctions: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            # Support both old column names (Firm Name) and new API format (SUPP_NAME)
            firm_name_raw = str(
                row.get("Firm Name") or row.get("SUPP_NAME") or ""
            ).strip()
            if not firm_name_raw:
                continue

            country = str(
                row.get("Country") or row.get("COUNTRY_NAME") or ""
            ).strip()
            from_date = str(
                row.get("From Date") or row.get("DEBAR_FROM_DATE") or ""
            ).strip()
            to_date = str(
                row.get("To Date") or row.get("DEBAR_TO_DATE") or ""
            ).strip()
            grounds = str(
                row.get("Grounds") or row.get("DEBAR_REASON") or ""
            ).strip()

            sanction_id = _make_debarment_id(firm_name_raw, country, from_date)

            sanctions.append({
                "sanction_id": sanction_id,
                "name": normalize_name(firm_name_raw),
                "original_name": firm_name_raw,
                "country": country,
                "from_date": from_date,
                "to_date": to_date,
                "grounds": grounds,
                "source": "world_bank",
                "source_list": "WORLD_BANK",
            })

        self.sanctions = deduplicate_rows(sanctions, ["sanction_id"])
        logger.info(
            "[world_bank] Transformed %d InternationalSanction nodes",
            len(self.sanctions),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.sanctions:
            loaded = loader.load_nodes(
                "InternationalSanction",
                self.sanctions,
                key_field="sanction_id",
            )
            logger.info("[world_bank] Loaded %d InternationalSanction nodes", loaded)

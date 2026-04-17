#!/usr/bin/env python3
"""Download PNCP federal procurement contracts (``comprasnet`` pipeline input).

The ``comprasnet`` ETL pipeline
(``etl/src/bracc_etl/pipelines/comprasnet.py``) reads one JSON file per year
from ``data/comprasnet/*_contratos.json`` containing raw PNCP contract
records (the ``/contratos`` endpoint, distinct from ``/contratacoes/publicacao``
used by the ``pncp`` / ``pncp_go`` pipelines).

This script fetches those JSON files from the public PNCP consulta API:

    https://pncp.gov.br/api/consulta/v1/contratos

API constraints (verified 2026-04-17):
  * ``dataInicial`` / ``dataFinal`` in ``YYYYMMDD`` format
  * Max window: 365 days (HTTP 422 "Periodo maior que 365 dias" beyond that)
  * ``tamanhoPagina`` up to 500 observed
  * Pre-PNCP years (2019-2020) return HTTP 204 (no content) — still succeeds,
    just yields empty files
  * Response shape:
        {"data": [...], "totalRegistros": N, "totalPaginas": P,
         "numeroPagina": P, "paginasRestantes": R, "empty": false}

Usage (matches ``config/bootstrap_all_contract.yml``):

    python3 scripts/download_comprasnet.py 2019 2020 2021 2022 2023 2024 2025 2026

    # Smoke test:
    python3 scripts/download_comprasnet.py 2024 \\
        --output-dir /tmp/smoke_comprasnet --max-pages 2

Output: one file per year named ``{year}_contratos.json`` containing a JSON
array of raw PNCP contract records — the exact shape
``ComprasnetPipeline.extract`` expects.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

try:  # Prefer httpx if installed; fall back to requests.
    import httpx  # type: ignore[import-not-found]

    _HTTP_LIB = "httpx"
except ImportError:  # pragma: no cover - fallback path
    httpx = None  # type: ignore[assignment]
    _HTTP_LIB = "requests"
    import requests  # noqa: F401  (imported lazily below)


API_URL = "https://pncp.gov.br/api/consulta/v1/contratos"
PAGE_SIZE = 500          # Max observed that the API accepts.
REQUEST_TIMEOUT = 60     # seconds per HTTP call.
MAX_RETRIES = 4
RETRY_BACKOFF = 3.0      # seconds, multiplied by attempt number.
INTER_PAGE_DELAY = 0.15  # polite pause between pages.

logger = logging.getLogger("download_comprasnet")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download PNCP federal procurement contracts as yearly JSON files "
            "for the `comprasnet` ETL pipeline."
        ),
    )
    parser.add_argument(
        "years",
        nargs="+",
        type=int,
        help="One or more years to download, e.g. 2019 2020 ... 2026.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/comprasnet"),
        help="Directory for {year}_contratos.json files (default: data/comprasnet).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap on pages per year (smoke test).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip a year if its output file already exists and is non-empty.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def _http_get_json(url: str, params: dict[str, Any]) -> tuple[int, dict | None]:
    """Issue a GET returning (status_code, parsed_json_or_None).

    Empty-body 2xx responses (HTTP 204) return (status, None).
    Raises on network errors after retry attempts are exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if _HTTP_LIB == "httpx":
                resp = httpx.get(url, params=params, timeout=REQUEST_TIMEOUT)
                status = resp.status_code
                text = resp.text
            else:
                import requests  # local import keeps startup cheap

                resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                status = resp.status_code
                text = resp.text

            if status == 204 or not text.strip():
                return status, None
            if status == 429:
                wait = RETRY_BACKOFF * attempt * 2
                logger.warning(
                    "HTTP 429 rate-limit on attempt %d/%d, sleeping %.1fs",
                    attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            if status >= 500:
                wait = RETRY_BACKOFF * attempt
                logger.warning(
                    "HTTP %d from PNCP (attempt %d/%d); retrying in %.1fs",
                    status, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            if status >= 400:
                # 4xx other than 429 are not worth retrying.
                logger.warning(
                    "HTTP %d from PNCP (non-retryable): params=%s body=%s",
                    status, params, text[:200],
                )
                return status, None
            # PNCP occasionally emits control chars; json.loads with strict=False.
            return status, json.loads(text, strict=False)
        except Exception as exc:  # noqa: BLE001 - we want network robustness
            last_exc = exc
            wait = RETRY_BACKOFF * attempt
            logger.warning(
                "Network error on attempt %d/%d: %s; retrying in %.1fs",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)

    if last_exc is not None:
        raise last_exc
    return 0, None


def _fetch_year(year: int, max_pages: int | None) -> list[dict[str, Any]]:
    """Fetch all contracts for a single year from the PNCP API."""
    date_inicial = f"{year:04d}0101"
    date_final = f"{year:04d}1231"
    all_records: list[dict[str, Any]] = []

    # Fetch page 1 to learn totalPaginas.
    params = {
        "dataInicial": date_inicial,
        "dataFinal": date_final,
        "pagina": 1,
        "tamanhoPagina": PAGE_SIZE,
    }
    status, payload = _http_get_json(API_URL, params)
    if status == 204 or payload is None:
        logger.info("  year=%d: HTTP %d, no data (likely pre-PNCP era).", year, status)
        return []

    first_items = payload.get("data") or []
    all_records.extend(first_items)
    total_pages = int(payload.get("totalPaginas", 1) or 1)
    total_registros = int(payload.get("totalRegistros", 0) or 0)
    logger.info(
        "  year=%d: totalRegistros=%d totalPaginas=%d (pageSize=%d)",
        year, total_registros, total_pages, PAGE_SIZE,
    )

    effective_last_page = total_pages
    if max_pages is not None:
        effective_last_page = min(total_pages, max_pages)

    for page in range(2, effective_last_page + 1):
        params = {
            "dataInicial": date_inicial,
            "dataFinal": date_final,
            "pagina": page,
            "tamanhoPagina": PAGE_SIZE,
        }
        status, payload = _http_get_json(API_URL, params)
        if payload is None:
            logger.warning(
                "  year=%d page=%d: empty response (status=%d); skipping.",
                year, page, status,
            )
            continue
        items = payload.get("data") or []
        all_records.extend(items)
        if page == 2 or page % 25 == 0 or page == effective_last_page:
            logger.info(
                "  year=%d: fetched page %d/%d (+%d records, total=%d)",
                year, page, effective_last_page, len(items), len(all_records),
            )
        if INTER_PAGE_DELAY > 0:
            time.sleep(INTER_PAGE_DELAY)

    if max_pages is not None and effective_last_page < total_pages:
        logger.info(
            "  year=%d: stopped early at page %d/%d (max-pages=%d)",
            year, effective_last_page, total_pages, max_pages,
        )

    return all_records


def _write_year_file(output_dir: Path, year: int, records: list[dict[str, Any]]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{year}_contratos.json"
    out_file.write_text(
        json.dumps(records, ensure_ascii=False),
        encoding="utf-8",
    )
    return out_file


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info("Using HTTP library: %s", _HTTP_LIB)
    logger.info(
        "Downloading PNCP contracts (%s) for years: %s",
        API_URL, args.years,
    )

    exit_code = 0
    total_records = 0
    for year in args.years:
        out_path = args.output_dir / f"{year}_contratos.json"
        if args.skip_existing and out_path.exists() and out_path.stat().st_size > 2:
            logger.info(
                "Skipping year=%d (file already exists: %s, %d bytes)",
                year, out_path, out_path.stat().st_size,
            )
            continue
        try:
            logger.info("=== Fetching year %d ===", year)
            records = _fetch_year(year, args.max_pages)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Fatal error fetching year=%d: %s", year, exc)
            exit_code = 2
            continue

        written = _write_year_file(args.output_dir, year, records)
        size = written.stat().st_size
        logger.info(
            "Wrote %s (%d records, %d bytes)",
            written, len(records), size,
        )
        total_records += len(records)

    logger.info(
        "Done: %d records across %d year(s) under %s",
        total_records, len(args.years), args.output_dir,
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

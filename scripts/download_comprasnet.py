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


def _page_dir(output_dir: Path, year: int) -> Path:
    """Per-page checkpoint directory for ``year``.

    Pages are written incrementally to survive PNCP timeouts (the
    upstream API routinely exhausts retries mid-year; prior versions of
    this script kept records in memory and lost the whole year on any
    fatal timeout — see commit log).
    """
    return output_dir / f"{year}_pages"


def _page_file(page_dir: Path, page: int) -> Path:
    return page_dir / f"p{page:05d}.json"


def _fetch_page(
    year: int,
    page: int,
    date_inicial: str,
    date_final: str,
) -> tuple[int, dict | None]:
    """Fetch a single page. Returns (status, payload) or (status, None)."""
    params = {
        "dataInicial": date_inicial,
        "dataFinal": date_final,
        "pagina": page,
        "tamanhoPagina": PAGE_SIZE,
    }
    try:
        return _http_get_json(API_URL, params)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "  year=%d page=%d: network error after retries (%s); marking missing "
            "and continuing — re-run to fill in.",
            year, page, exc,
        )
        return 0, None


def _fetch_year(year: int, max_pages: int | None, output_dir: Path) -> int:
    """Fetch all contracts for a single year from the PNCP API.

    Writes one JSON file per page under ``{output_dir}/{year}_pages/``
    as the page arrives. Already-present page files are reused (simple
    resume). Page-level network failures are logged and skipped — the
    year loop never raises, so a single flaky page no longer discards
    hundreds of MB of already-downloaded data.

    Returns the number of pages successfully materialized on disk for
    this year (including reused ones).
    """
    date_inicial = f"{year:04d}0101"
    date_final = f"{year:04d}1231"
    pages_dir = _page_dir(output_dir, year)
    pages_dir.mkdir(parents=True, exist_ok=True)

    page1_path = _page_file(pages_dir, 1)
    if page1_path.exists() and page1_path.stat().st_size > 2:
        page1_payload = json.loads(page1_path.read_text(encoding="utf-8"))
    else:
        status, payload = _fetch_page(year, 1, date_inicial, date_final)
        if status == 204 or payload is None:
            logger.info(
                "  year=%d: HTTP %d on page 1, no data (likely pre-PNCP era).",
                year, status,
            )
            return 0
        page1_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        page1_payload = payload

    total_pages = int(page1_payload.get("totalPaginas", 1) or 1)
    total_registros = int(page1_payload.get("totalRegistros", 0) or 0)
    first_items = page1_payload.get("data") or []
    logger.info(
        "  year=%d: totalRegistros=%d totalPaginas=%d (pageSize=%d)",
        year, total_registros, total_pages, PAGE_SIZE,
    )

    effective_last_page = total_pages
    if max_pages is not None:
        effective_last_page = min(total_pages, max_pages)

    pages_ok = 1  # page 1 already accounted for
    for page in range(2, effective_last_page + 1):
        out_path = _page_file(pages_dir, page)
        if out_path.exists() and out_path.stat().st_size > 2:
            pages_ok += 1
            continue

        _status, payload = _fetch_page(year, page, date_inicial, date_final)
        if payload is None:
            # Either HTTP 204/empty body (end of data) or network failure. In
            # the 204 case we still want to stop iterating (no more pages);
            # in the failure case we just continue. We treat both the same:
            # skip the page, don't persist an empty file.
            if _status == 204:
                logger.info(
                    "  year=%d page=%d: HTTP 204 — end of data.",
                    year, page,
                )
                break
            continue

        out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        pages_ok += 1
        items = payload.get("data") or []
        if page == 2 or page % 25 == 0 or page == effective_last_page:
            logger.info(
                "  year=%d: fetched page %d/%d (+%d records, on_disk=%d)",
                year, page, effective_last_page, len(items), pages_ok,
            )
        if INTER_PAGE_DELAY > 0:
            time.sleep(INTER_PAGE_DELAY)

    if max_pages is not None and effective_last_page < total_pages:
        logger.info(
            "  year=%d: stopped early at page %d/%d (max-pages=%d)",
            year, effective_last_page, total_pages, max_pages,
        )
    _ = first_items  # retained for backwards-compat; page1_path is the source of truth.
    return pages_ok


def _consolidate_year(
    output_dir: Path, year: int, *, prune_pages: bool = True,
) -> tuple[Path, int, int]:
    """Merge all ``{year}_pages/p*.json`` into ``{year}_contratos.json``.

    Returns ``(out_path, pages_merged, records_written)``. Missing pages
    (gaps from persistent network failures) are logged as warnings but
    do not prevent consolidation — re-run to fill them in.
    """
    pages_dir = _page_dir(output_dir, year)
    out_file = output_dir / f"{year}_contratos.json"

    if not pages_dir.exists():
        # Nothing fetched — write an empty JSON array so the pipeline's
        # skip-existing path triggers and downstream doesn't crash.
        out_file.write_text("[]", encoding="utf-8")
        return out_file, 0, 0

    page_files = sorted(pages_dir.glob("p*.json"))
    if not page_files:
        out_file.write_text("[]", encoding="utf-8")
        return out_file, 0, 0

    # Detect gaps (for logging only).
    seen = {int(p.stem[1:]) for p in page_files}
    expected_last = max(seen) if seen else 0
    missing = [n for n in range(1, expected_last + 1) if n not in seen]
    if missing:
        logger.warning(
            "  year=%d: %d gap(s) in page sequence (first gaps: %s). Re-run to fill.",
            year, len(missing), missing[:10],
        )

    # Streaming consolidation — writes records to the output file one at a
    # time so we never hold more than one page (~1 MB) in memory. Earlier
    # in-memory implementations OOM-killed on years with 3+ GB of page
    # data (2025 peaked at 14 GB RSS before the kernel killed it).
    merged_count = 0
    with out_file.open("w", encoding="utf-8") as fh:
        fh.write("[")
        first = True
        for pf in page_files:
            try:
                payload = json.loads(pf.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                logger.warning(
                    "  year=%d: bad JSON in %s: %s — skipping.", year, pf.name, exc,
                )
                continue
            data = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                continue
            for record in data:
                if not first:
                    fh.write(",\n")
                json.dump(record, fh, ensure_ascii=False)
                first = False
                merged_count += 1
        fh.write("]\n")

    if prune_pages:
        for pf in page_files:
            pf.unlink(missing_ok=True)
        try:
            pages_dir.rmdir()
        except OSError:
            pass

    return out_file, len(page_files), merged_count


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
                "Skipping year=%d (consolidated file already exists: %s, %d bytes)",
                year, out_path, out_path.stat().st_size,
            )
            continue

        logger.info("=== Fetching year %d ===", year)
        try:
            pages_on_disk = _fetch_year(year, args.max_pages, args.output_dir)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unexpected error in _fetch_year(%d): %s", year, exc)
            exit_code = 2
            # still fall through to consolidate whatever pages did land.
            pages_on_disk = -1

        written, merged_pages, rec_count = _consolidate_year(args.output_dir, year)
        logger.info(
            "Wrote %s (%d pages merged -> %d records, %d bytes; pages_on_disk=%d)",
            written, merged_pages, rec_count, written.stat().st_size, pages_on_disk,
        )
        total_records += rec_count

    logger.info(
        "Done: %d records across %d year(s) under %s",
        total_records, len(args.years), args.output_dir,
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

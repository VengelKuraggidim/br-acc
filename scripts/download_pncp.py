#!/usr/bin/env python3
"""Download PNCP procurement records (national scope) for the ``pncp`` pipeline.

Wraps :func:`bracc_etl.pipelines.pncp.fetch_to_disk` so the bootstrap
contract can list ``pncp`` as ``script_download``.

The ``pncp`` ETL pipeline reads ``pncp_*.json`` from ``data/pncp/``;
``fetch_to_disk`` writes one JSON per (modalidade, year) tuple so re-runs
can resume per shard without re-downloading completed combos.

Source (no auth): https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao

Caveats:
  * Multi-year × all-modalidades fetches can take hours and produce
    thousands of pages. Use --limit for smoke tests; --start-year /
    --end-year for backfills; --modalidades to subset.
  * HTTP 204 responses for empty windows are treated as success (no rows).
  * Modalidades that hit the timeout twice in the same window are
    skipped to avoid hammering a stalled endpoint.

Usage::

    # Smoke test — 1 modalidade × 1 year × cap 100 records:
    uv run --project etl python scripts/download_pncp.py \\
        --start-year 2024 --end-year 2024 --modalidade 6 --limit 100 \\
        --output-dir /tmp/smoke_pncp

    # Default: previous + current year, all modalidades.
    uv run --project etl python scripts/download_pncp.py \\
        --output-dir data/pncp
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.pncp import fetch_to_disk


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download PNCP procurement publications (federal/estadual/"
            "municipal) into the directory the pncp pipeline reads from."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pncp"),
        help="Destination directory (default: data/pncp).",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Inclusive start year (default: previous calendar year).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Inclusive end year (default: current calendar year).",
    )
    parser.add_argument(
        "--modalidade",
        action="append",
        type=int,
        default=None,
        help=(
            "PNCP modalidade code 1..13 (repeat for multiple). "
            "Omit to iterate every modalidade defined by the pipeline."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap total records fetched across the whole run (smoke).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help=(
            "httpx total timeout in seconds. Currently informational — the "
            "pipeline uses a fixed 60s timeout matching the PNCP backend's "
            "own request-budget hints."
        ),
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    written = fetch_to_disk(
        output_dir=args.output_dir,
        start_year=args.start_year,
        end_year=args.end_year,
        modalidades=args.modalidade,
        limit=args.limit,
    )

    if not written:
        print(
            "[download_pncp] no files written — see warnings above.",
            file=sys.stderr,
        )
        return 1

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

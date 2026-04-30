#!/usr/bin/env python3
"""Download Novo CAGED monthly microdata.

Wraps ``bracc_etl.pipelines.caged.fetch_to_disk``. Downloads
``CAGEDMOV<YYYYMM>.7z`` from PDET, extracts via the system ``7z`` binary
(``apt install 7zip`` on Ubuntu 25.10+, ``apt install p7zip-full`` on
older releases — same dependency model as the ``qlik`` scraper) and
writes ``caged_<YYYYMM>.csv`` per month into ``--output-dir``.

CAGED MOV per month: ~50 MB compressed / ~500 MB extracted. Default
behaviour is "give me all 12 months of one year"; use ``--limit`` for
smoke runs.

Usage::

    # Smoke test: 1 month, 100 rows
    uv run --project etl python scripts/download_caged.py \\
        --year 2024 --month 1 --limit 100 \\
        --output-dir /tmp/smoke_caged

    # Production: full year (12 months × ~50MB each = ~600MB on disk)
    uv run --project etl python scripts/download_caged.py \\
        --year 2024 --output-dir data/caged
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.caged import fetch_to_disk


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Novo CAGED monthly microdata from PDET (.7z mensal "
            "→ extract via 7z CLI → remap → write caged_<YYYYMM>.csv)."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year (required, e.g. 2024). No default — full "
        "history is multi-GB.",
    )
    parser.add_argument(
        "--month",
        type=int,
        action="append",
        choices=range(1, 13),
        metavar="{1..12}",
        help=(
            "Month to fetch (1..12). Repeatable: ``--month 1 --month 2``. "
            "Default: every month."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/caged"),
        help="Destination directory (default: data/caged).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Truncate each month's CSV to the first N rows. Smoke-test only.",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download archives even when present in <output-dir>/raw/.",
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
        year=args.year,
        months=args.month,
        limit=args.limit,
        skip_existing=not args.no_skip_existing,
    )

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size / 1e6:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

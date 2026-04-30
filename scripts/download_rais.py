#!/usr/bin/env python3
"""Download RAIS establishment microdata and write aggregated CSV.

Wraps ``bracc_etl.pipelines.rais.fetch_to_disk``. Downloads
``RAIS_ESTAB_PUB.7z`` (~120 MB compressed / ~1 GB extracted) for a given
year, extracts via the system ``7z`` binary (``apt install 7zip`` on
Ubuntu 25.10+, ``apt install p7zip-full`` on older releases — same
dependency model as the ``qlik`` scraper) and writes a pre-aggregated
``rais_<year>_aggregated.csv`` to ``--output-dir``.

Only the establishment file is fetched. ``RAIS_VINC_PUB_*`` (multi-GB
per-region employment record archives) is not — this pipeline emits
sector-level reference data only.

Usage::

    # Smoke test: 1000 raw rows
    uv run --project etl python scripts/download_rais.py \\
        --year 2022 --limit 1000 \\
        --output-dir /tmp/smoke_rais

    # Production: full year
    uv run --project etl python scripts/download_rais.py \\
        --year 2024 --output-dir data/rais

    # Multiple years
    uv run --project etl python scripts/download_rais.py \\
        --year 2022 --year 2023 --year 2024 --output-dir data/rais
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.rais import fetch_to_disk


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download RAIS establishment microdata from PDET, aggregate "
            "by (CNAE subclass, UF), and write rais_<year>_aggregated.csv."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        required=True,
        help=(
            "Reference year (required). Repeatable: ``--year 2022 --year 2023``. "
            "No default — multi-year history is multi-GB on disk."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/rais"),
        help="Destination directory (default: data/rais).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate raw-row processing to the first N rows. Smoke-test only "
            "— skips the bulk of the aggregation."
        ),
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

    written: list[Path] = []
    for year in args.year:
        result = fetch_to_disk(
            output_dir=args.output_dir,
            year=year,
            limit=args.limit,
            skip_existing=not args.no_skip_existing,
        )
        written.extend(result)

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size / 1e6:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

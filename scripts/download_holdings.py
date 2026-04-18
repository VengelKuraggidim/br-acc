#!/usr/bin/env python3
"""Download Brasil.IO ``holding.csv.gz`` for the ``holdings`` pipeline.

Thin CLI wrapper around :func:`bracc_etl.pipelines.holdings.fetch_to_disk` so
the Fiscal Cidadao bootstrap contract can mark ``holdings`` as
``script_download`` instead of ``file_manifest``.

The ``holdings`` ETL pipeline reads ``holding.csv.gz`` (or ``holding.csv``)
from ``data/holdings/`` and turns the rows into ``HOLDING_DE`` Company-Company
relationships. Source: socios-brasil dataset on Brasil.IO, derived from the
Receita Federal partner CSVs that the ``cnpj`` pipeline ingests separately.

Usage::

    # Default — write to data/holdings/, skip if file already present.
    uv run --project etl python scripts/download_holdings.py

    # Smoke test in a temp dir, force re-download:
    uv run --project etl python scripts/download_holdings.py \\
        --output-dir /tmp/smoke_holdings --no-skip-existing
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.holdings import fetch_to_disk


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Brasil.IO holding.csv.gz (CNPJ-to-CNPJ ownership rows) "
            "into the directory the holdings pipeline reads from."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/holdings"),
        help="Destination directory (default: data/holdings, created if missing).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip if holding.csv.gz already exists in output-dir (default).",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Force re-download even when the destination file already exists.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="httpx total timeout in seconds (default: 600).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Accepted for API symmetry with other download_* scripts; "
            "Brasil.IO ships a single rolling snapshot, so this is "
            "informational only and not applied client-side."
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
        skip_existing=args.skip_existing,
        timeout=args.timeout,
    )

    if not written:
        print(
            "[download_holdings] no files written — see warnings above.",
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

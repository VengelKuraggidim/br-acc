#!/usr/bin/env python3
"""Download the OFAC SDN (Specially Designated Nationals) CSV to disk.

Thin CLI wrapper around ``bracc_etl.pipelines.ofac.fetch_to_disk``.
The US Treasury publishes the SDN list as a header-less CSV (~10k rows,
~8-10 MB) at a legacy URL that 302-redirects to the current
sanctionslistservice.ofac.treas.gov export. No authentication required.

Source (public, 302 -> live export):
    https://www.treasury.gov/ofac/downloads/sdn.csv
    -> https://sanctionslistservice.ofac.treas.gov/api/publicationpreview/exports/sdn.csv

OfacPipeline.extract() reads the file positionally (no header row), so
this wrapper preserves the raw bytes byte-for-byte by default; the
``--limit`` flag truncates to the first N data lines for smoke tests.

Usage::

    # Smoke test with 100 rows
    uv run --project etl python scripts/download_ofac.py \\
        --output-dir /tmp/smoke_ofac --limit 100

    # Full public dataset (~10k rows)
    uv run --project etl python scripts/download_ofac.py \\
        --output-dir data/ofac
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.ofac import OFAC_SDN_URL, fetch_to_disk


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the OFAC SDN CSV (US Treasury's Specially Designated "
            "Nationals list) to <output-dir>/sdn.csv. No auth required."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/ofac"),
        help="Destination directory (default: data/ofac, created if missing).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate the downloaded CSV to the first N data lines "
            "(the SDN file has no header row). Useful for smoke tests."
        ),
    )
    parser.add_argument(
        "--url",
        default=OFAC_SDN_URL,
        help=(
            "Override the OFAC SDN source URL. "
            f"Defaults to {OFAC_SDN_URL}."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout in seconds (default: 120).",
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
        limit=args.limit,
        url=args.url,
        timeout=args.timeout,
    )

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for path in written:
        size = path.stat().st_size if path.exists() else 0
        print(f"  {path}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

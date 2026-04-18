#!/usr/bin/env python3
"""Download the UN Security Council Consolidated Sanctions List.

Wraps ``bracc_etl.pipelines.un_sanctions.fetch_to_disk`` so the Fiscal
Cidadão bootstrap contract can treat the UN source as ``script_download``
instead of ``file_manifest``.

Source (public bulk feed, no auth / no token)::

    https://scsanctions.un.org/resources/xml/en/consolidated.xml

The endpoint serves a single XML with <INDIVIDUAL> and <ENTITY> records
spanning every UN sanctions regime (1267, 1988, 1718, 1970, etc.). The
script parses that XML and writes a flat JSON array in the schema
``UnSanctionsPipeline.extract()`` expects, alongside the raw XML for
auditability.

Usage::

    # Smoke test — first 10 records
    uv run --project etl python scripts/download_un_sanctions.py \\
        --output-dir /tmp/smoke_un_sanctions --limit 10

    # Full refresh
    uv run --project etl python scripts/download_un_sanctions.py \\
        --output-dir data/un_sanctions
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.un_sanctions import UN_CONSOLIDATED_URL, fetch_to_disk


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the UN Security Council Consolidated Sanctions List "
            "XML and project it to JSON matching UnSanctionsPipeline's "
            "expected schema. Writes un_sanctions.xml + un_sanctions.json "
            "to <output-dir>/."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/un_sanctions"),
        help="Destination directory (default: data/un_sanctions).",
    )
    parser.add_argument(
        "--url",
        default=UN_CONSOLIDATED_URL,
        help=(
            "Override the upstream XML URL "
            "(default: https://scsanctions.un.org/resources/xml/en/consolidated.xml)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate the JSON to the first N records (applied after "
            "parsing; the raw XML is always written in full). Useful for "
            "smoke tests."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="httpx request timeout in seconds (default: 60).",
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
        url=args.url,
        limit=args.limit,
        timeout=args.timeout,
    )

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

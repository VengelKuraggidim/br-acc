#!/usr/bin/env python3
"""Download the EU Financial Sanctions Database (FSD) consolidated list.

Wraps ``bracc_etl.pipelines.eu_sanctions.fetch_to_disk`` so the Fiscal
Cidadão bootstrap contract can treat the EU source as ``script_download``
instead of ``file_manifest``.

Source (public bulk feed, token-gated)::

    https://webgate.ec.europa.eu/fsd/fsf/public/files/csvFullSanctionsList/content?token=dG9rZW4tMjAxNw
    https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw

The ``token`` query-string parameter is a static public value documented
on the EEAS FSD page; it has been stable since 2017. Requests without it
return HTTP 403.

The CSV format (``;`` delimited, UTF-8-BOM) matches the "new EU
consolidated format" branch of ``EuSanctionsPipeline.transform`` and is
written as ``eu_sanctions.csv``. The XML is downloaded alongside as an
audit artefact.

Usage::

    # Smoke test — skip the ~24 MB XML audit copy
    uv run --project etl python scripts/download_eu_sanctions.py \\
        --output-dir /tmp/smoke_eu_sanctions --no-xml

    # Full refresh
    uv run --project etl python scripts/download_eu_sanctions.py \\
        --output-dir data/eu_sanctions
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.eu_sanctions import (
    EU_FSD_CSV_URL,
    EU_FSD_XML_URL,
    fetch_to_disk,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the EU FSD consolidated sanctions list (CSV + XML) "
            "to <output-dir>/. Writes eu_sanctions.csv (primary input for "
            "EuSanctionsPipeline) and eu_sanctions.xml (audit copy)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/eu_sanctions"),
        help="Destination directory (default: data/eu_sanctions).",
    )
    parser.add_argument(
        "--csv-url",
        default=EU_FSD_CSV_URL,
        help=(
            "Override the upstream CSV URL. Must include the public "
            "'token=' query string or the gateway returns HTTP 403."
        ),
    )
    parser.add_argument(
        "--xml-url",
        default=EU_FSD_XML_URL,
        help="Override the upstream XML URL (same token requirement).",
    )
    parser.add_argument(
        "--no-xml",
        action="store_true",
        help=(
            "Skip the XML download (saves ~24 MB and a second request). "
            "Useful for smoke tests; production refreshes should keep it "
            "for audit."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="httpx request timeout in seconds (default: 120).",
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

    xml_url = None if args.no_xml else args.xml_url
    written = fetch_to_disk(
        output_dir=args.output_dir,
        csv_url=args.csv_url,
        xml_url=xml_url,
        timeout=args.timeout,
    )

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

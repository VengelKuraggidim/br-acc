#!/usr/bin/env python3
"""Download Camara dos Deputados CPI/CPMI metadata + sessions to disk.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.camara_inquiries.fetch_to_disk`. Writes the three
canonical CSV files the :class:`CamaraInquiriesPipeline` reads:

* ``inquiries.csv``    — CPI/CPMI catalog from
  ``https://dadosabertos.camara.leg.br/api/v2/orgaos?sigla=CPI|CPMI``
* ``requirements.csv`` — empty (full requirement coverage needs the
  BigQuery ``basedosdados.br_camara_dados_abertos`` path wired into
  ``etl/scripts/download_camara_inquiries.py --mode bq_first``).
* ``sessions.csv``     — ``/orgaos/{id}/eventos`` session metadata per CPI.

The Camara v2 Open Data API is public (no auth), so this path runs in any
environment without GCP credentials — the tradeoff is empty requirements.

Usage::

    # Full catalog:
    uv run --project etl python scripts/download_camara_inquiries.py \\
        --output-dir data/camara_inquiries

    # Smoke test capped at 5 inquiries:
    uv run --project etl python scripts/download_camara_inquiries.py \\
        --output-dir /tmp/smoke_camara_inquiries --limit 5
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from the repo root without installing the etl package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.camara_inquiries import fetch_to_disk  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Camara dos Deputados CPI/CPMI inquiry metadata and "
            "session logs via the public v2 Open Data API. No credentials "
            "required; requirements.csv is written empty because historical "
            "requirement joins live in BigQuery (see etl/scripts/"
            "download_camara_inquiries.py for that path)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/camara_inquiries"),
        help=(
            "Directory to write CSVs into. Created if missing. "
            "Defaults to data/camara_inquiries (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap on the number of CPI/CPMI commissions probed. "
            "Useful for smoke tests; omit to pull the full catalog."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Per-request HTTP timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    written = fetch_to_disk(
        output_dir=args.output_dir,
        limit=args.limit,
        timeout=args.timeout,
    )

    if not written:
        print(
            "[download_camara_inquiries] no files written — check logs for "
            "HTTP errors or an empty API catalog.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size if path.exists() else 0
        print(f"[download_camara_inquiries] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

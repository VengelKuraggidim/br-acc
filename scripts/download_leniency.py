#!/usr/bin/env python3
"""Download the CGU "Acordos de Leniência" snapshot CSV to disk.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.leniency.fetch_to_disk`.

Data source: Portal da Transparencia "Acordos de Leniência" widget
(open, no auth). The landing page
``/download-de-dados/acordos-leniencia`` embeds the current snapshot
date (``YYYYMMDD``) in an inline ``arquivos.push({...})`` JS block.
Download URL is ``/download-de-dados/acordos-leniencia/<YYYYMMDD>``.
The CSV uses accented uppercase column headers; the wrapper remaps them
to the lowercase ASCII keys ``LeniencyPipeline.extract`` expects.

Usage::

    # Default: today's snapshot
    uv run --project etl python scripts/download_leniency.py \\
        --output-dir data/leniency

    # Pin to a specific published snapshot (YYYYMMDD)
    uv run --project etl python scripts/download_leniency.py \\
        --output-dir /tmp/smoke_leniency --snapshot 20260417
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.leniency import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the CGU Acordos de Leniência snapshot, remap "
            "its accented uppercase columns to the lowercase ASCII "
            "schema LeniencyPipeline.extract expects, and write "
            "leniencia.csv into --output-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/leniency"),
        help="Destination directory (default: data/leniency).",
    )
    parser.add_argument(
        "--snapshot",
        default=None,
        help=(
            "Explicit YYYYMMDD snapshot date. Defaults to the latest "
            "date scraped off the landing page."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout in seconds (default: 120).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if leniencia.csv already exists.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("download_leniency")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        snapshot=args.snapshot,
        skip_existing=not args.no_skip_existing,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_leniency] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_leniency] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

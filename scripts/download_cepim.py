#!/usr/bin/env python3
"""Download the CGU CEPIM snapshot CSV to disk.

Thin CLI wrapper around :func:`bracc_etl.pipelines.cepim.fetch_to_disk`.

Data source: Portal da Transparencia "Empresas Impedidas de Contratar"
widget (open, no auth). The landing page
``/download-de-dados/cepim`` embeds the current snapshot date
(``YYYYMMDD``) in an inline ``arquivos.push({...})`` JS block. Download
URL is ``/download-de-dados/cepim/<YYYYMMDD>``. Widget mode is ``DIA``
-- only the current published snapshot date works; older dates return
403 from the CGU dadosabertos S3 bucket.

Usage::

    # Default: scrape today's snapshot date off the landing page
    uv run --project etl python scripts/download_cepim.py \\
        --output-dir data/cepim

    # Pin to a specific published snapshot (only the current day works
    # in practice; see CGU widget mode DIA caveat above)
    uv run --project etl python scripts/download_cepim.py \\
        --output-dir /tmp/smoke_cepim --date 20260416
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

from bracc_etl.pipelines.cepim import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the CGU CEPIM snapshot ZIP, extract its inner "
            "CSV, and write cepim.csv into --output-dir using the "
            "upstream ;-delimited latin-1 layout the pipeline expects."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/cepim"),
        help="Destination directory (default: data/cepim).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Explicit YYYYMMDD snapshot date. Defaults to the current "
            "snapshot scraped from the landing page. Historical dates "
            "almost always return 403 (widget mode DIA)."
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
        help="Re-download even if cepim.csv already exists.",
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
    logger = logging.getLogger("download_cepim")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        date=args.date,
        skip_existing=not args.no_skip_existing,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_cepim] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_cepim] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

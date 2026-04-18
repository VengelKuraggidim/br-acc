#!/usr/bin/env python3
"""Download the CGU CEAF (Cadastro de Expulsões) snapshot CSV to disk.

Thin CLI wrapper around :func:`bracc_etl.pipelines.ceaf.fetch_to_disk`.

Data source: Portal da Transparencia "Cadastro de Expulsões da
Administração Federal" widget (open, no auth). The landing page
``/download-de-dados/ceaf`` embeds the current snapshot date
(``YYYYMMDD``) in an inline ``arquivos.push({...})`` JS block. The
download URL is ``/download-de-dados/ceaf/<YYYYMMDD>``. The upstream
CSV uses ``;``-delimited latin-1 with accented uppercase headers; the
wrapper remaps them to the ``,``-delimited snake_case schema
``CeafPipeline.extract`` expects. Widget mode is ``DIA`` -- only the
current published snapshot date works; older dates return 403.

Note: CPFs are masked upstream as ``***.NNN.NNN-**`` so 0 person->
expulsion relationships are produced by design (only ``Expulsion``
nodes).

Usage::

    # Default: scrape today's snapshot date off the landing page
    uv run --project etl python scripts/download_ceaf.py \\
        --output-dir data/ceaf

    # Pin to a specific published snapshot
    uv run --project etl python scripts/download_ceaf.py \\
        --output-dir /tmp/smoke_ceaf --date 20260417
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

from bracc_etl.pipelines.ceaf import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the CGU CEAF snapshot ZIP, remap accented "
            "uppercase columns to the snake_case schema "
            "CeafPipeline.extract expects, and write ceaf.csv into "
            "--output-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/ceaf"),
        help="Destination directory (default: data/ceaf).",
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
        help="Re-download even if ceaf.csv already exists.",
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
    logger = logging.getLogger("download_ceaf")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        date=args.date,
        skip_existing=not args.no_skip_existing,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_ceaf] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_ceaf] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

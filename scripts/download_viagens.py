#!/usr/bin/env python3
"""Download Portal da Transparencia "Viagens a Serviço" data to disk.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.viagens.fetch_to_disk` so the Fiscal Cidadao
bootstrap contract can drop the old "pre-place
``data/viagens/<YYYY>_Viagem.csv`` manually" workflow.

Data source: Portal da Transparencia bulk endpoint
``/download-de-dados/viagens/<YYYY>`` (open, no auth) which 302s to a
yearly ZIP on ``dadosabertos-download.cgu.gov.br``. Each ZIP holds four
CSVs; ``ViagensPipeline.extract`` only reads the ``*_Viagem.csv``
grain, so the wrapper keeps just that file and drops the rest.

Usage::

    # Default: last 3 completed calendar years
    uv run --project etl python scripts/download_viagens.py \\
        --output-dir data/viagens

    # Smoke test -- one small historical year
    uv run --project etl python scripts/download_viagens.py \\
        --output-dir /tmp/smoke_viagens --years 2020
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

from bracc_etl.pipelines.viagens import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Portal da Transparencia Viagens a Serviço yearly "
            "ZIPs, extract the main *_Viagem.csv into --output-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/viagens"),
        help="Destination directory (default: data/viagens).",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help=(
            "Years to fetch (space-separated). Defaults to the last 3 "
            "completed calendar years."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="Per-file HTTP timeout in seconds (default: 600).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if the year's CSV already exists.",
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
    logger = logging.getLogger("download_viagens")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        years=args.years,
        skip_existing=not args.no_skip_existing,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_viagens] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_viagens] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

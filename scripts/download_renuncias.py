#!/usr/bin/env python3
"""Download Portal da Transparencia "Renúncias Fiscais" data to disk.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.renuncias.fetch_to_disk`.

Data source: Portal da Transparencia bulk endpoint
``/download-de-dados/renuncias/<YYYY>`` (open, no auth) which 302s to a
yearly ZIP. The slug is ``renuncias`` -- the intuitive
``renuncias-fiscais`` variant returns HTTP 500 upstream.

Usage::

    # Default: last 3 completed calendar years
    uv run --project etl python scripts/download_renuncias.py \\
        --output-dir data/renuncias

    # Smoke test -- single historical year
    uv run --project etl python scripts/download_renuncias.py \\
        --output-dir /tmp/smoke_renuncias --years 2024
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

from bracc_etl.pipelines.renuncias import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Portal da Transparencia Renúncias Fiscais yearly "
            "ZIPs, extract matching CSV(s) into --output-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/renuncias"),
        help="Destination directory (default: data/renuncias).",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help=(
            "Years to fetch (space-separated). Defaults to the last 3 "
            "completed calendar years. Upstream coverage: 2015-current."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="Per-file HTTP timeout in seconds (default: 300).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if the year's CSVs already exist.",
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
    logger = logging.getLogger("download_renuncias")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        years=args.years,
        skip_existing=not args.no_skip_existing,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_renuncias] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_renuncias] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

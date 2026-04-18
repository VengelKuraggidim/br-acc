#!/usr/bin/env python3
"""Download Portal da Transparencia CPGF monthly ZIPs to disk.

Thin CLI wrapper around :func:`bracc_etl.pipelines.cpgf.fetch_to_disk`.

Data source: Portal da Transparencia "Cartão de Pagamento do Governo
Federal" widget (open, no auth). Each
``/download-de-dados/cpgf/<YYYYMM>`` request 302s to a monthly
``<YYYYMM>_CPGF.zip`` on the CGU dadosabertos bucket. Widget mode is
``MES``; the most recent month usually lags 1-2 months. Without
explicit ``--month``/``--start``/``--end``, the wrapper walks back
from today and stops at the first published month.

Note: CPFs are masked upstream as ``***.NNN.NNN-**`` so 0
person->expense relationships are produced by design (only
``GovCardExpense`` nodes).

Usage::

    # Default: walk back from today, fetch the first published month
    uv run --project etl python scripts/download_cpgf.py \\
        --output-dir data/cpgf

    # Pin to a specific month
    uv run --project etl python scripts/download_cpgf.py \\
        --output-dir /tmp/smoke_cpgf --month 202601

    # Bulk range (inclusive)
    uv run --project etl python scripts/download_cpgf.py \\
        --output-dir data/cpgf --start 202501 --end 202512
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

from bracc_etl.pipelines.cpgf import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Portal da Transparencia CPGF monthly ZIPs and "
            "extract their CSV(s) into --output-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/cpgf"),
        help="Destination directory (default: data/cpgf).",
    )
    parser.add_argument(
        "--month",
        action="append",
        default=None,
        metavar="YYYYMM",
        help="Specific month (repeatable). Mutually exclusive with --start/--end.",
    )
    parser.add_argument(
        "--start",
        default=None,
        metavar="YYYYMM",
        help="Start of inclusive range (requires --end).",
    )
    parser.add_argument(
        "--end",
        default=None,
        metavar="YYYYMM",
        help="End of inclusive range (requires --start).",
    )
    parser.add_argument(
        "--walkback",
        type=int,
        default=6,
        help=(
            "Default-mode walkback window in months (default: 6). "
            "Ignored when --month or --start/--end is set."
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
        help="Re-download even if a month's CSV already exists.",
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
    logger = logging.getLogger("download_cpgf")

    if (args.start and not args.end) or (args.end and not args.start):
        logger.error("--start and --end must both be set")
        return 2

    written = fetch_to_disk(
        output_dir=args.output_dir,
        months=args.month,
        start=args.start,
        end=args.end,
        walkback=args.walkback,
        skip_existing=not args.no_skip_existing,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_cpgf] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_cpgf] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

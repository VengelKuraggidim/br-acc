#!/usr/bin/env python3
"""Download Diario Oficial da Uniao (DOU) acts for a date range.

Thin CLI wrapper around :func:`bracc_etl.pipelines.dou.fetch_to_disk` so
the Fiscal Cidadao bootstrap contract can flip the DOU source from
``file_manifest`` to ``script_download`` without requiring BigQuery
credentials or the authenticated INlabs FTP.

Data source: https://www.in.gov.br/leiturajornal (the official public
"Leitura do Jornal" renderer on the Imprensa Nacional portal). Each
(day, section) URL embeds a JSON payload (``<script id="params">``)
listing every act published that day; the wrapper walks the date range
and dumps one ``<YYYY-MM-DD>_<section>.json`` per combination, in the
shape DouPipeline._extract_json already consumes.

Usage::

    # Default (last 30 days, sections DO1/DO2/DO3) into the pipeline dir:
    uv run --project etl python scripts/download_dou.py \\
        --output-dir data/dou

    # Smoke test — one specific weekday, DO1 only:
    uv run --project etl python scripts/download_dou.py \\
        --output-dir /tmp/smoke_dou \\
        --start-date 2026-04-15 --end-date 2026-04-15 --sections do1

    # Backfill a quarter (override the 30-day cap):
    uv run --project etl python scripts/download_dou.py \\
        --output-dir data/dou \\
        --start-date 2026-01-01 --end-date 2026-03-31 --max-days 120
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

from bracc_etl.pipelines.dou import fetch_to_disk  # noqa: E402


def _parse_sections(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    out = [s.strip().lower() for s in raw.split(",") if s.strip()]
    return out or None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download DOU acts from www.in.gov.br/leiturajornal for a date "
            "range and write per-(day,section) JSON files into "
            "<output-dir>/ for DouPipeline to consume."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/dou"),
        help="Destination directory (default: data/dou, created if missing).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help=(
            "First day to fetch (YYYY-MM-DD). Defaults to "
            "end_date - (max_days - 1) so a bare call grabs the trailing "
            "window."
        ),
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Last day to fetch, inclusive (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--sections",
        default="do1,do2,do3",
        help=(
            "Comma-separated DOU section codes to fetch "
            "(default: do1,do2,do3). The 'E' variants (do1e, do2e, do3e) "
            "are valid but rare — pass them explicitly if needed."
        ),
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=30,
        help=(
            "Soft cap on the span between --start-date and --end-date "
            "(default: 30). Set to 0 or a negative value to disable."
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
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("download_dou")

    max_days: int | None = args.max_days if args.max_days and args.max_days > 0 else None

    written = fetch_to_disk(
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        sections=_parse_sections(args.sections),
        timeout=args.timeout,
        max_days=max_days,
    )

    if not written:
        logger.warning(
            "[download_dou] no files written - the date range may have no "
            "publications (weekend/holidays) or upstream returned no acts.",
        )
        return 0

    for path in written:
        size = path.stat().st_size
        logger.info("[download_dou] wrote %s (%d bytes)", path, size)
    logger.info("[download_dou] total: %d file(s)", len(written))
    return 0


if __name__ == "__main__":
    sys.exit(main())

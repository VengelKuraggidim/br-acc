#!/usr/bin/env python3
"""Download BCB (Banco Central do Brasil) PAS penalties data.

Thin CLI wrapper around :func:`bracc_etl.pipelines.bcb.fetch_to_disk` so
the Fiscal Cidadao bootstrap contract can mark the BCB source as
``script_download`` instead of ``file_manifest``.

Data source: Olinda OData public gateway, dataset
"Processo Administrativo Sancionador - Penalidades Aplicadas"
(https://dadosabertos.bcb.gov.br/dataset/processo-administrativo-sancionador---penalidades-aplicadas).

The upstream JSON schema is remapped to the legacy semicolon-separated
Latin-1 CSV ``penalidades.csv`` the pipeline already consumes.

Usage::

    # Full historical fetch (default output path):
    uv run --project etl python scripts/download_bcb.py \\
        --output-dir data/bcb

    # Smoke test — first 100 rows only:
    uv run --project etl python scripts/download_bcb.py \\
        --output-dir /tmp/smoke_bcb --limit 100
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

from bracc_etl.pipelines.bcb import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the BCB Olinda OData 'Penalidades Aplicadas' table "
            "and write it as data/bcb/penalidades.csv (semicolon / Latin-1) "
            "for BcbPipeline to consume."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/bcb"),
        help="Destination directory (default: data/bcb, created if missing).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="OData $top page size per HTTP request (default: 1000).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Truncate download to the first N rows (smoke tests).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Per-request HTTP timeout in seconds (default: 120).",
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
    logger = logging.getLogger("download_bcb")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        page_size=args.page_size,
        limit=args.limit,
        timeout=args.timeout,
    )

    if not written:
        logger.error("[download_bcb] no files written - check logs above.")
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info("[download_bcb] wrote %s (%d bytes)", path, size)
    return 0


if __name__ == "__main__":
    sys.exit(main())

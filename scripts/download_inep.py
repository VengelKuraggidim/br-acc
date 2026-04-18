#!/usr/bin/env python3
"""Download INEP Censo Escolar microdata to disk.

Thin CLI wrapper around :func:`bracc_etl.pipelines.inep.fetch_to_disk`.

Source: https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_<YYYY>.zip

The ZIP is ~26 MB; the unpacked ``microdados_ed_basica_<YYYY>.csv`` is
~190 MB. Use ``--limit`` to truncate the output for smoke tests.

The ``download.inep.gov.br`` certificate chain typically fails default
TLS verification; this script defaults to ``--insecure`` (disabled
verify). The ZIP container's CRC validates the payload independently.
Pass ``--no-insecure`` to opt back into TLS verification.

Usage::

    # Smoke (5000 rows, default GO census year)
    uv run --project etl python scripts/download_inep.py \\
        --output-dir /tmp/smoke_inep --limit 5000

    # Full file (190 MB) into the canonical bootstrap location
    uv run --project etl python scripts/download_inep.py \\
        --output-dir data/inep
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

from bracc_etl.pipelines.inep import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download INEP Censo Escolar microdata ZIP, extract the main "
            "microdados_ed_basica_<year>.csv, and write it into "
            "--output-dir in the same latin-1/semicolon format "
            "InepPipeline.extract consumes."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/inep"),
        help="Destination directory (default: data/inep, created if missing).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="Census year (default: 2022).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate the output CSV to the first N data rows (header "
            "preserved). The full file is ~190 MB; pass e.g. --limit 5000 "
            "for smoke tests."
        ),
    )
    parser.add_argument(
        "--no-insecure",
        action="store_true",
        help=(
            "Enable TLS verification (default: disabled because the INEP "
            "cert chain commonly fails default-bundle verification). The "
            "ZIP CRC validates the payload regardless."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="HTTP timeout in seconds (default: 600).",
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
        stream=sys.stdout,
    )
    logger = logging.getLogger("download_inep")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        year=args.year,
        limit=args.limit,
        insecure=not args.no_insecure,
        timeout=args.timeout,
    )

    if not written:
        logger.error("[download_inep] no files written -- check logs above.")
        return 1

    for path in written:
        size = path.stat().st_size if path.exists() else 0
        logger.info("[download_inep] wrote %s (%d bytes)", path, size)
    return 0


if __name__ == "__main__":
    sys.exit(main())

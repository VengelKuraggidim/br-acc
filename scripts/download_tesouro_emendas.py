#!/usr/bin/env python3
"""Download Tesouro Transparente "emendas parlamentares" data filtered by UF.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.tesouro_emendas.fetch_to_disk` so the Fiscal
Cidadao bootstrap contract can drop the old "pre-place
``data/tesouro_emendas/emendas_tesouro.csv`` manually" workflow in favour
of automated ingestion.

Data source: Tesouro Transparente CKAN
(https://www.tesourotransparente.gov.br/ckan/dataset/emendas-parlamentares).
The dataset is a national dump with a ``UF`` column, so we filter to
``UF=GO`` (default) at download time to keep ``data/tesouro_emendas/``
small and scoped for the GO bootstrap.

Usage::

    # Full historical GO-only fetch (default):
    uv run --project etl python scripts/download_tesouro_emendas.py \\
        --output-dir data/tesouro_emendas

    # Smoke test — single year, GO only:
    uv run --project etl python scripts/download_tesouro_emendas.py \\
        --output-dir /tmp/smoke_tesouro_emendas --uf GO --years 2024

    # National dump (no UF filter):
    uv run --project etl python scripts/download_tesouro_emendas.py \\
        --output-dir data/tesouro_emendas --uf ""
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

from bracc_etl.pipelines.tesouro_emendas import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the Tesouro Transparente emendas-parlamentares CSV, "
            "filter it by UF (default GO) and optional years, and write "
            "emendas_tesouro.csv into a local directory."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/tesouro_emendas"),
        help=(
            "Directory to write emendas_tesouro.csv (and the raw national "
            "dump) into. Created if missing. Defaults to "
            "data/tesouro_emendas (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--uf",
        default="GO",
        help=(
            "UF code to keep (default: GO). Pass an empty string "
            "('--uf \"\"') to skip the UF filter and keep the full "
            "national dataset."
        ),
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help=(
            "Optional space-separated years to keep (e.g. '--years 2023 "
            "2024 2025'). Omit to keep every year present in the source."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout for the CKAN download in seconds (default: 120).",
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
    logger = logging.getLogger("download_tesouro_emendas")

    # argparse hands us a string; normalise empty -> None so fetch_to_disk
    # skips the UF filter entirely when the caller asked for the national
    # dataset.
    uf: str | None = args.uf if args.uf else None

    written = fetch_to_disk(
        output_dir=args.output_dir,
        uf=uf,
        years=args.years,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_tesouro_emendas] no files written - "
            "check logs above for HTTP errors.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_tesouro_emendas] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

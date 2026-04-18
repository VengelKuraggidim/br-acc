#!/usr/bin/env python3
"""Download SICONFI DCA balance-sheet rows for the ``siconfi`` pipeline.

Wraps :func:`bracc_etl.pipelines.siconfi.fetch_to_disk` so the Fiscal
Cidadao bootstrap contract can flip ``siconfi`` from ``file_manifest`` to
``script_download``.

The ``siconfi`` ETL pipeline (``etl/src/bracc_etl/pipelines/siconfi.py``)
reads ``dca_*.csv`` files from ``data/siconfi/``. This script writes one
``dca_<exercicio>.csv`` per requested year.

Source (no auth):
    https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca

Caveat: full-national runs are ~5,570 entities × N years and take hours.
For smoke / day-to-day use, prefer ``--states-only`` (~27 rows per year)
or ``--limit``.

Usage::

    # Smoke test — last year, only state-level entities (fast):
    uv run --project etl python scripts/download_siconfi.py \\
        --output-dir /tmp/smoke_siconfi --states-only

    # Specific exercicios + entity filter (e.g., one Goias municipality):
    uv run --project etl python scripts/download_siconfi.py \\
        --exercicio 2023 --exercicio 2024 --ente 5208707 \\
        --output-dir data/siconfi
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.siconfi import (
    SICONFI_DEFAULT_ANNEX,
    fetch_to_disk,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download SICONFI DCA rows (balance-sheet annex) into the "
            "directory the siconfi pipeline reads from."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/siconfi"),
        help="Destination directory (default: data/siconfi).",
    )
    parser.add_argument(
        "--exercicio",
        action="append",
        type=int,
        default=None,
        help=(
            "Year to fetch (repeat for multiple). Defaults to the previous "
            "calendar year — SICONFI typically lags ~12 months."
        ),
    )
    parser.add_argument(
        "--ente",
        action="append",
        type=str,
        default=None,
        help=(
            "Optional IBGE code to keep (repeat for multiple). "
            "Omit to iterate every entidade returned by /entes."
        ),
    )
    parser.add_argument(
        "--annex",
        default=SICONFI_DEFAULT_ANNEX,
        help=f"DCA annex name (default: {SICONFI_DEFAULT_ANNEX!r}).",
    )
    parser.add_argument(
        "--states-only",
        action="store_true",
        help="Restrict to UF-level entities (esfera == E, ~27 per year).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap number of entidades iterated per year (smoke tests).",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    written = fetch_to_disk(
        output_dir=args.output_dir,
        exercicios=args.exercicio,
        entes=args.ente,
        annex=args.annex,
        limit=args.limit,
        states_only=args.states_only,
    )

    if not written:
        print(
            "[download_siconfi] no files written — see warnings above.",
            file=sys.stderr,
        )
        return 1

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

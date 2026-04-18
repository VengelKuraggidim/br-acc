#!/usr/bin/env python3
"""Download SIOP (parliamentary amendments) data to disk, optionally UF-filtered.

Thin CLI wrapper around
``bracc_etl.pipelines.siop.fetch_to_disk`` so the Fiscal Cidadao
bootstrap contract can drop the old "pre-place files under ``data/siop/``
manually" workflow in favour of automated ingestion.

The underlying source is the Portal da Transparencia bulk endpoint
``/download-de-dados/emendas-parlamentares/<year>`` (open, no auth).
SIOP's own web UI at ``siop.planejamento.gov.br`` is a QlikView dashboard
with no machine-readable bulk export, so the CGU mirror — which holds
the same federal budget-execution numbers for parliamentary amendments —
is the practical automated feed. See the ``fetch_to_disk`` docstring for
how the optional ``--uf`` filter maps onto this dataset.

Usage::

    # Full national historical (last 5 years, default):
    uv run --project etl python scripts/download_siop.py \\
        --output-dir data/siop

    # GO-scoped for the Fiscal Cidadao GO contract:
    uv run --project etl python scripts/download_siop.py \\
        --output-dir data/siop --uf GO --year 2022 --year 2023 \\
        --year 2024 --year 2025

    # Smoke test — one recent year, GO filter:
    uv run --project etl python scripts/download_siop.py \\
        --output-dir /tmp/smoke_siop --uf GO --year 2024
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.siop import fetch_to_disk


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download SIOP parliamentary amendments bulk data from the "
            "Portal da Transparencia CGU endpoint, optionally filtering "
            "by destination/author UF. Writes one emendas_<year>.csv per "
            "year into --output-dir (the path SiopPipeline.extract globs)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/siop"),
        help=(
            "Destination directory. Created if missing. Defaults to "
            "data/siop (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--uf",
        default=None,
        help=(
            "Optional 2-letter UF sigla (e.g. GO) to filter to. When set, "
            "rows are kept only when the destination UF, bancada-author "
            "name, or locality suffix indicates that state. Omit for the "
            "full national dataset."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        default=None,
        help=(
            "Year to download. May be repeated (e.g. --year 2022 --year "
            "2023). Defaults to the last 5 completed years."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Per-file HTTP timeout in seconds (default: 600).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if raw ZIPs are already present.",
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

    written = fetch_to_disk(
        output_dir=args.output_dir,
        uf=args.uf,
        years=args.year,
        skip_existing=not args.no_skip_existing,
        timeout=args.timeout,
    )

    if not written:
        print(
            "[download_siop] no files written — check logs above for "
            "HTTP/extraction errors or UF filter mismatches.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_siop] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

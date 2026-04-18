#!/usr/bin/env python3
"""Download ALEGO (Assembleia Legislativa de Goias) transparency feeds to disk.

Thin CLI wrapper around ``bracc_etl.pipelines.alego.fetch_to_disk``. Hits the
public Angular SPA API at https://transparencia.al.go.leg.br/api/transparencia
(discovered by inspecting the bundle) and writes three CSV files in the shape
``AlegoPipeline.extract`` already expects.

Usage:
    uv run --project etl python scripts/download_alego.py \
        --output-dir data/alego

    # Smoke test with just the 3 first deputies x 1 month:
    uv run --project etl python scripts/download_alego.py \
        --output-dir /tmp/smoke_alego --limit 3 --months 1

Notes on etiquette:
- ``alegodigital.al.go.leg.br`` (SPL) has ``Disallow: /`` in robots.txt and
  is NOT touched here. We only talk to ``transparencia.al.go.leg.br`` whose
  robots.txt only disallows ``quadro-de-remuneracao`` paths.
- The pipeline module enforces a ~1 req/s rate limit.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.alego import fetch_to_disk


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download ALEGO transparency feeds (deputados, cota parlamentar, "
            "proposicoes) to a local directory as ;-delimited CSV files."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/alego"),
        help=(
            "Directory to write CSV files into. Created if missing. "
            "Defaults to data/alego (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap on number of deputies fetched (useful for smoke "
            "tests). Omit to pull every deputy."
        ),
    )
    parser.add_argument(
        "--months",
        type=int,
        default=3,
        help=(
            "How many recent (ano, mes) periods to pull cota-parlamentar "
            "lancamentos for. Default: 3 (roughly one quarter). Pass 0 to "
            "iterate every period the upstream exposes (slow — ~180 pairs)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    max_months = args.months if args.months > 0 else None
    written = fetch_to_disk(
        output_dir=args.output_dir,
        limit=args.limit,
        max_expense_months=max_months,
    )

    if not written:
        print(
            "[download_alego] no files written — check logs above for HTTP errors.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_alego] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

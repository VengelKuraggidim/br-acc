#!/usr/bin/env python3
"""Download Camara Municipal de Goiania JSON feeds to disk.

Thin CLI wrapper around
``bracc_etl.pipelines.camara_goiania.fetch_to_disk`` so the Fiscal Cidadao
bootstrap contract can drop the old "pre-place files under
data/camara_goiania/ manually" workflow.

Usage:
    uv run --project etl python scripts/download_camara_goiania.py \
        --output-dir data/camara_goiania

    # Smoke test with a tiny per-endpoint cap:
    uv run --project etl python scripts/download_camara_goiania.py \
        --limit 5 --output-dir /tmp/smoke_camara_goiania
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.camara_goiania import fetch_to_disk


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the three Camara Municipal de Goiania JSON feeds "
            "(vereadores, transparency, proposicoes) to a local directory."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/camara_goiania"),
        help=(
            "Directory to write JSON files into. Created if missing. "
            "Defaults to data/camara_goiania (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional per-endpoint record cap for smoke tests. "
            "Omit to download the full payloads."
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

    written = fetch_to_disk(output_dir=args.output_dir, limit=args.limit)

    if not written:
        print(
            "[download_camara_goiania] no files written — check logs above "
            "for HTTP errors.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_camara_goiania] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

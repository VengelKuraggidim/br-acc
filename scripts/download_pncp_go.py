#!/usr/bin/env python3
"""Download PNCP (Goias-scoped) procurement records to disk.

Thin CLI wrapper around
``bracc_etl.pipelines.pncp_go.fetch_to_disk`` so the Fiscal Cidadao
bootstrap contract can drop the old "pre-place files under
data/pncp_go/ manually" workflow.

Source: https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao

Usage:
    # Full historical fetch (default ~5 years back, all 14 modalidades):
    uv run --project etl python scripts/download_pncp_go.py \\
        --output-dir data/pncp_go

    # Smoke test with a short window and a single modalidade:
    uv run --project etl python scripts/download_pncp_go.py \\
        --date-start 2025-01-01 --date-end 2025-01-31 \\
        --modalidades 6 --limit 10 \\
        --output-dir /tmp/smoke_pncp_go
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.pncp_go import fetch_to_disk


def _parse_modalidades(raw: str | None) -> list[int] | None:
    """Parse a comma-separated modalidade list (e.g. ``"6,8,9"``)."""
    if raw is None:
        return None
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    if not tokens:
        return None
    try:
        return [int(t) for t in tokens]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--modalidades must be a CSV of integers, got: {raw!r}",
        ) from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download PNCP procurement publications filtered by UF=GO "
            "from the public PNCP API to a local directory."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pncp_go"),
        help=(
            "Directory to write JSON files into. Created if missing. "
            "Defaults to data/pncp_go (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--date-start",
        default=None,
        help=(
            "Inclusive start date (YYYY-MM-DD or YYYYMMDD). Defaults to the "
            "pipeline's built-in historical window (~5 years back, capped "
            "at the PNCP launch in 2021)."
        ),
    )
    parser.add_argument(
        "--date-end",
        default=None,
        help=(
            "Inclusive end date (YYYY-MM-DD or YYYYMMDD). Defaults to today."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional total record cap for smoke tests. "
            "Omit to fetch the full historical payload."
        ),
    )
    parser.add_argument(
        "--modalidades",
        type=str,
        default=None,
        help=(
            "Comma-separated PNCP modalidade codes (e.g. '6,8,9'). "
            "Omit to iterate all 14 codes defined by the pipeline "
            "(1-14, matching PNCP's Manual de Dados Abertos catalog)."
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

    modalidades = _parse_modalidades(args.modalidades)

    written = fetch_to_disk(
        output_dir=args.output_dir,
        date_start=args.date_start,
        date_end=args.date_end,
        limit=args.limit,
        modalidades=modalidades,
    )

    if not written:
        print(
            "[download_pncp_go] no files written — check logs above "
            "for HTTP errors or empty API responses.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_pncp_go] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

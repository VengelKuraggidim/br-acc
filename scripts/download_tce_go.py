#!/usr/bin/env python3
"""Download TCE-GO decisoes (acórdãos, despachos, resoluções) to disk.

Thin CLI wrapper around ``bracc_etl.pipelines.tce_go.fetch_to_disk``, which
hits the non-documented public JSON API at
``https://iago-search-api.tce.go.gov.br/decisions/search``. The endpoint
powers the SPA at ``https://decisoes.tce.go.gov.br/`` and is the only TCE-GO
dataset we found exposed as structured JSON (the other two — contas
irregulares e fiscalizações — ficam num painel Qlik Sense que ainda precisa
de scraper dedicado, rastreado em
``todo-list-prompts/high_priority/debitos/tce-go-qlik-scraper.md``).

Scope: 10k decisões mais recentes (cap do backend Elasticsearch). Sem auth.

Usage:
    # Full dataset (~10k rows, ~15s):
    uv run --project etl python scripts/download_tce_go.py \
        --output-dir data/tce_go

    # Smoke test:
    uv run --project etl python scripts/download_tce_go.py \
        --output-dir /tmp/smoke_tce_go --limit 50
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.tce_go import (
    DECISIONS_SEARCH_URL,
    fetch_to_disk,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download TCE-GO decisoes (acórdãos/despachos/resoluções) "
            "via iago-search-api. Writes data/tce_go/decisoes.csv by default."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/tce_go"),
        help=(
            "Directory to write the CSV into. Created if missing. "
            "Defaults to data/tce_go (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap on the number of data rows. Useful for smoke "
            "tests. Omit to pull everything (~10k, the backend cap)."
        ),
    )
    parser.add_argument(
        "--url",
        default=DECISIONS_SEARCH_URL,
        help=(
            "Override the iago-search-api endpoint. "
            f"Defaults to {DECISIONS_SEARCH_URL}."
        ),
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help=(
            "Rows per request (default: 1000; backend caps effectively "
            "at 2000). Lower values stretch the run; higher get clipped."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds (default: 60).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    written = fetch_to_disk(
        output_dir=args.output_dir,
        limit=args.limit,
        url=args.url,
        page_size=args.page_size,
        timeout=args.timeout,
    )

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for path in written:
        size = path.stat().st_size if path.exists() else 0
        print(f"  {path}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

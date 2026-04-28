#!/usr/bin/env python3
"""Download TCE-GO datasets to disk (decisões + painéis Qlik).

Three datasets:

- **decisoes** (default ON) — REST anônimo em
  ``iago-search-api.tce.go.gov.br/decisions/search`` (cap ~10k linhas).
- **irregulares** (default OFF) — painel Qlik Sense via Selenium + Firefox
  headless. Índice de 8 PDFs com a lista de servidores irregulares por ano.
- **fiscalizacoes** (default OFF) — painel Qlik Sense via Selenium. ~60
  fiscalizações em andamento (número, ano, tipo, status, descrição, relator).

Os dois painéis precisam da extra ``etl[qlik]`` (selenium) + Firefox e
geckodriver instalados no sistema. Detalhes em
``bracc_etl.pipelines.tce_go_qlik``.

Usage:
    # Só decisões (default, back-compat — ~15s pra 10k rows):
    uv run --project etl python scripts/download_tce_go.py \
        --output-dir data/tce_go

    # Tudo via Qlik (decisões + irregulares + fiscalizações):
    uv run --project etl python scripts/download_tce_go.py \
        --output-dir data/tce_go --include-qlik

    # Só painéis Qlik, pular decisões:
    uv run --project etl python scripts/download_tce_go.py \
        --output-dir data/tce_go --no-decisoes --include-qlik

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
            "Download TCE-GO datasets (decisões + painéis Qlik). Writes "
            "data/tce_go/{decisoes,irregulares,fiscalizacoes}.csv."
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
            "Optional cap on the number of data rows for decisões "
            "(useful for smoke tests). Não afeta os painéis Qlik. "
            "Omit to pull everything (~10k, the backend cap)."
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
    parser.add_argument(
        "--no-decisoes",
        dest="include_decisoes",
        action="store_false",
        help="Skip decisões (default: include).",
    )
    parser.add_argument(
        "--include-irregulares",
        action="store_true",
        help=(
            "Scrape Contas Irregulares panel via Selenium + Firefox. "
            "Requires etl[qlik] + Firefox + geckodriver."
        ),
    )
    parser.add_argument(
        "--include-fiscalizacoes",
        action="store_true",
        help=(
            "Scrape Fiscalizações panel via Selenium + Firefox. "
            "Requires etl[qlik] + Firefox + geckodriver."
        ),
    )
    parser.add_argument(
        "--include-qlik",
        action="store_true",
        help="Atalho pra --include-irregulares + --include-fiscalizacoes.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    include_irregulares = args.include_irregulares or args.include_qlik
    include_fiscalizacoes = args.include_fiscalizacoes or args.include_qlik

    written = fetch_to_disk(
        output_dir=args.output_dir,
        limit=args.limit,
        url=args.url,
        page_size=args.page_size,
        timeout=args.timeout,
        include_decisoes=args.include_decisoes,
        include_irregulares=include_irregulares,
        include_fiscalizacoes=include_fiscalizacoes,
    )

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for path in written:
        size = path.stat().st_size if path.exists() else 0
        print(f"  {path}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

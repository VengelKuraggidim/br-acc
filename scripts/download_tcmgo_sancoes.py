#!/usr/bin/env python3
"""Download TCM-GO sanction-flavored lists to disk.

Thin CLI wrapper around ``bracc_etl.pipelines.tcmgo_sancoes.fetch_to_disk``,
which calls the TCM-GO Web Services open-data endpoint for "contas com
parecer previo pela rejeicao ou julgadas irregulares". The endpoint returns
a CSV with pre-masked CPFs; the wrapper normalises headers into the
aliases the ``tcmgo_sancoes`` pipeline already accepts and writes
``impedidos.csv`` under the target directory.

Source (service #31 in the TCM-GO Web Services catalog):
  https://ws.tcm.go.gov.br/api/rest/dados/contas-irregulares

Scope: UF=GO (all 246 municipalities handled by TCM-GO).
No authentication required.

Note: the "impedidos de licitar" list on the TCM-GO portal is currently
rendered only via an embedded Power BI report, with no public CSV/JSON.
This wrapper ships the one automated artifact available today; operators
may continue to drop a manually exported ``impedidos.csv`` (for example,
from a LAI request) alongside the produced file and re-run the pipeline.

Usage:
    # Full public dataset (~1.4k rows as of 2026-04):
    uv run --project etl python scripts/download_tcmgo_sancoes.py \
        --output-dir data/tcmgo_sancoes

    # Smoke test with a small limit:
    uv run --project etl python scripts/download_tcmgo_sancoes.py \
        --output-dir /tmp/smoke_tcmgo_sancoes --limit 10
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.tcmgo_sancoes import (
    CONTAS_IRREGULARES_URL,
    fetch_impedidos_jsf,
    fetch_to_disk,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download TCM-GO contas-irregulares CSV (sanction-flavored "
            "list of agents with accounts judged irregular or rejected "
            "by TCM-GO). Writes data/tcmgo_sancoes/impedidos.csv by default."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/tcmgo_sancoes"),
        help=(
            "Directory to write the CSV into. Created if missing. "
            "Defaults to data/tcmgo_sancoes (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap on the number of data rows to write "
            "(header always preserved). Useful for smoke tests."
        ),
    )
    parser.add_argument(
        "--url",
        default=CONTAS_IRREGULARES_URL,
        help=(
            "Override the public TCM-GO endpoint. "
            f"Defaults to {CONTAS_IRREGULARES_URL}."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--include-impedidos-jsf",
        action="store_true",
        help=(
            "Also scrape the PrimeFaces widget at tcmgo.tc.br/portalwidgets/"
            "xhtml/impedimento/impedimento.jsf and write impedidos_licitar.csv. "
            "Distinct from the REST CSV (contas-irregulares). Respects "
            "--limit and writes under --output-dir."
        ),
    )
    parser.add_argument(
        "--jsf-only",
        action="store_true",
        help=(
            "Skip the REST CSV fetch and scrape only the JSF widget. Useful "
            "for smoke-testing the scraper without re-downloading the REST "
            "data. Implies --include-impedidos-jsf."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    written: list[Path] = []
    if not args.jsf_only:
        written.extend(
            fetch_to_disk(
                output_dir=args.output_dir,
                limit=args.limit,
                url=args.url,
                timeout=args.timeout,
            ),
        )
    if args.jsf_only or args.include_impedidos_jsf:
        jsf_csv = fetch_impedidos_jsf(
            output_dir=args.output_dir,
            limit=args.limit,
            timeout=args.timeout,
        )
        written.append(jsf_csv)

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for path in written:
        size = path.stat().st_size if path.exists() else 0
        print(f"  {path}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Download the MiDES canonical tables (licitacao/contrato/item) from BigQuery.

Wraps :func:`bracc_etl.pipelines.mides.fetch_to_disk`. The MiDES ETL pipeline
reads ``licitacao.csv``, ``contrato.csv``, ``item.csv`` (or ``.parquet``
siblings) from ``data/mides/``; this script materialises them via BigQuery
queries against the World Bank MiDES dataset on Base dos Dados.

Source: ``basedosdados.world_wb_mides`` (default) or ``basedosdados.br_mides``
(legacy schema). Both require BigQuery access.

REQUIREMENTS:
  * The ``[bigquery]`` optional extra (``uv sync --extra bigquery``).
  * ``GOOGLE_APPLICATION_CREDENTIALS`` pointing at a service-account JSON
    with BigQuery user role on the billing project.

Usage::

    # Default — world_wb_mides, 2021..2100, billing icarus-corruptos:
    uv run --project etl python scripts/download_mides.py \\
        --output-dir data/mides

    # Custom year window + billing project:
    uv run --project etl python scripts/download_mides.py \\
        --start-year 2024 --end-year 2024 \\
        --billing-project my-gcp-project \\
        --output-dir data/mides
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.mides import (
    MIDES_LEGACY_DATASET,
    MIDES_WORLD_WB_DATASET,
    fetch_to_disk,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialise the three canonical MiDES CSV tables into the "
            "directory the mides pipeline reads from. Requires "
            "[bigquery] extra + GOOGLE_APPLICATION_CREDENTIALS."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/mides"),
        help="Destination directory (default: data/mides).",
    )
    parser.add_argument(
        "--billing-project",
        default=None,
        help=(
            "GCP project for BigQuery billing. Falls back to "
            "$GCP_BILLING_PROJECT, then to 'icarus-corruptos'."
        ),
    )
    parser.add_argument(
        "--dataset",
        default=MIDES_WORLD_WB_DATASET,
        help=(
            f"BigQuery dataset id. Default: {MIDES_WORLD_WB_DATASET!r}. "
            f"Legacy fallback: {MIDES_LEGACY_DATASET!r}."
        ),
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2021,
        help="Inclusive start year filter (default: 2021).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2100,
        help="Inclusive end year filter (default: 2100).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip target files that already exist with non-zero size (default).",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Re-run BigQuery for every target even if it already exists.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Accepted for API symmetry; BigQuery queries do not currently "
            "wire a row limit, but a future ``LIMIT`` clause would slot in "
            "via the year filter."
        ),
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

    try:
        written = fetch_to_disk(
            output_dir=args.output_dir,
            billing_project=args.billing_project,
            dataset=args.dataset,
            start_year=args.start_year,
            end_year=args.end_year,
            skip_existing=args.skip_existing,
        )
    except RuntimeError as exc:
        # fetch_to_disk raises RuntimeError when the [bigquery] extra is
        # missing or GOOGLE_APPLICATION_CREDENTIALS is unset — surface a
        # clean exit code so the bootstrap orchestrator can interpret it.
        print(f"[download_mides] {exc}", file=sys.stderr)
        return 2

    if not written:
        print(
            "[download_mides] no files written — see warnings above.",
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

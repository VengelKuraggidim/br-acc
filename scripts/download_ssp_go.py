#!/usr/bin/env python3
"""Download SSP-GO (Goias public-security) raw artifacts to disk.

Thin CLI wrapper around
``bracc_etl.pipelines.ssp_go.fetch_to_disk`` so the Fiscal Cidadao
bootstrap contract can replace the old "pre-place files under
data/ssp_go/ manually" workflow with automated ingestion.

Upstream reality (audited 2026-04-17):

- ``goias.gov.br/seguranca/estatisticas/`` publishes yearly PDF
  bulletins (``estatisticas_<YYYY>.pdf`` plus consolidated multi-year
  PDFs). These are the only machine-readable crime statistics SSP-GO
  publishes.
- ``dadosabertos.go.gov.br`` (state CKAN) has no "ocorrencias by
  municipality" dataset. The SSP organization's sole CSV is
  ``doacoes-recebidas-ssp`` (donations received).

This wrapper downloads both categories. The pipeline's ``extract``
still expects an ``ocorrencias.csv`` — a PDF -> CSV extractor is
pending (tracked in the pipeline module's docstring).

Usage:
    # Download every yearly bulletin plus the CKAN donations CSV.
    uv run --project etl python scripts/download_ssp_go.py \
        --output-dir data/ssp_go

    # Smoke test with only the first 5 PDFs plus the donations CSV:
    uv run --project etl python scripts/download_ssp_go.py \
        --limit 5 --output-dir /tmp/smoke_ssp_go
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from the repo root without installing the etl package:
# inject etl/src on sys.path so ``import bracc_etl`` resolves. When run
# via ``uv run --project etl``, this is a no-op.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.ssp_go import fetch_to_disk  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download SSP-GO yearly public-security bulletins (PDF) from "
            "goias.gov.br/seguranca/estatisticas/ plus the SSP "
            "donations CSV from dadosabertos.go.gov.br into a local "
            "directory."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/ssp_go"),
        help=(
            "Directory to write SSP raw files into. Created if missing. "
            "Defaults to data/ssp_go (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap on the number of PDF bulletins to fetch "
            "(applied in page-listing order, for smoke tests). Omit to "
            "download every bulletin. The CKAN donations CSV is a "
            "single file and is always attempted."
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

    written = fetch_to_disk(
        output_dir=args.output_dir,
        limit=args.limit,
    )

    if not written:
        print(
            "[download_ssp_go] no files written — check logs above for "
            "HTTP/CKAN errors.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_ssp_go] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

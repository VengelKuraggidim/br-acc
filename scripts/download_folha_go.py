#!/usr/bin/env python3
"""Download Goias state payroll (``folha_go``) CKAN data to disk.

Thin CLI wrapper around
``bracc_etl.pipelines.folha_go.fetch_to_disk`` so the Fiscal Cidadao
bootstrap contract can drop the old "pre-place files under
data/folha_go/ manually" workflow in favour of automated ingestion.

Usage:
    uv run --project etl python scripts/download_folha_go.py \
        --output-dir data/folha_go

    # Smoke test with a tiny row cap:
    uv run --project etl python scripts/download_folha_go.py \
        --limit 50 --output-dir /tmp/smoke_folha_go

    # Pin a specific CKAN resource id (advanced / historical snapshots):
    uv run --project etl python scripts/download_folha_go.py \
        --resource-id <ckan-resource-uuid> --output-dir data/folha_go
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.folha_go import fetch_to_disk


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the Goias state payroll CKAN dataset "
            "(folha-de-pagamento) to a local directory as servidores.csv."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/folha_go"),
        help=(
            "Directory to write servidores.csv into. Created if missing. "
            "Defaults to data/folha_go (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional row cap for smoke tests. Omit to download the full "
            "CKAN datastore for the latest active monthly resource."
        ),
    )
    parser.add_argument(
        "--resource-id",
        default=None,
        help=(
            "Optional CKAN datastore resource id override. If omitted, the "
            "latest datastore-active CSV resource of the "
            "'folha-de-pagamento' dataset is auto-discovered."
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
        resource_id=args.resource_id,
    )

    if not written:
        print(
            "[download_folha_go] no files written — check logs above "
            "for CKAN discovery/HTTP errors.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_folha_go] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

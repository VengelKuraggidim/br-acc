#!/usr/bin/env python3
"""Download an OpenSanctions FollowTheMoney JSONL dataset to disk.

Thin CLI wrapper around ``bracc_etl.pipelines.opensanctions.fetch_to_disk``.
OpenSanctions publishes each of its collections as an ``entities.ftm.json``
(JSONL) feed under ``https://data.opensanctions.org/datasets/latest/...``.
No authentication required.

Since OpenSanctionsPipeline hard-filters to Brazilian-connected entities,
the br/acc default dataset is ``br_pep`` (~110 MB, ~253k PEPs). Pass
``--dataset default`` for the full aggregate (~2.7 GB) or any other
OpenSanctions collection slug.

Usage::

    # Smoke test — 5k lines of the Brazilian PEPs dataset
    uv run --project etl python scripts/download_opensanctions.py \\
        --output-dir /tmp/smoke_opensanctions --limit 5000

    # Default: br_pep dataset, capped at 50 000 lines
    uv run --project etl python scripts/download_opensanctions.py \\
        --output-dir data/opensanctions

    # Unbounded, full aggregate collection (multi-GB)
    uv run --project etl python scripts/download_opensanctions.py \\
        --output-dir data/opensanctions --dataset default --limit 0
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.opensanctions import (
    OPENSANCTIONS_BASE,
    OPENSANCTIONS_DEFAULT_DATASET,
    OPENSANCTIONS_DEFAULT_LIMIT,
    fetch_to_disk,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download an OpenSanctions FollowTheMoney JSONL dataset into "
            "<output-dir>/entities.ftm.json. Default dataset is br_pep "
            "(the pipeline discards non-Brazilian entities downstream)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/opensanctions"),
        help="Destination directory (default: data/opensanctions).",
    )
    parser.add_argument(
        "--dataset",
        default=OPENSANCTIONS_DEFAULT_DATASET,
        help=(
            "OpenSanctions dataset slug. Common values: br_pep (default), "
            "peps, sanctions, default. See data.opensanctions.org for the "
            f"full catalog. Default: {OPENSANCTIONS_DEFAULT_DATASET}."
        ),
    )
    parser.add_argument(
        "--limit",
        "--max-records",
        dest="limit",
        type=int,
        default=OPENSANCTIONS_DEFAULT_LIMIT,
        help=(
            "Max number of JSONL lines to write. Pass 0 (or a negative "
            "value) for unbounded — required to get the full multi-GB "
            f"default collection. Default: {OPENSANCTIONS_DEFAULT_LIMIT}."
        ),
    )
    parser.add_argument(
        "--url",
        default=None,
        help=(
            "Explicit URL override. When set, supersedes --dataset / --base."
        ),
    )
    parser.add_argument(
        "--base",
        default=OPENSANCTIONS_BASE,
        help=(
            "OpenSanctions base URL. "
            f"Default: {OPENSANCTIONS_BASE}."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="HTTP timeout in seconds (default: 300).",
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

    written = fetch_to_disk(
        output_dir=args.output_dir,
        dataset=args.dataset,
        limit=args.limit,
        url=args.url,
        base=args.base,
        timeout=args.timeout,
    )

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for path in written:
        size = path.stat().st_size if path.exists() else 0
        print(f"  {path}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

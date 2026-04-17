#!/usr/bin/env python3
"""Download Goiás gazettes from the Querido Diário API.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.querido_diario_go.fetch_to_disk`. Writes a
canonical ``gazettes.json`` envelope consumed by ``QueridoDiarioGoPipeline``
via its local-files fallback, so the pipeline no longer requires manually
pre-placed fixtures.

Usage::

    python3 scripts/download_querido_diario_go.py \\
        --output-dir data/querido_diario_go

    # smoke run
    python3 scripts/download_querido_diario_go.py \\
        --limit 20 --output-dir /tmp/smoke_querido_diario_go
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from the repo root without installing the etl package: inject
# etl/src on sys.path so ``import bracc_etl`` resolves. When the script is run
# via ``uv run`` inside ``etl/``, this is a no-op.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.querido_diario_go import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Goiás (territory_ids=52) gazettes from the Querido "
            "Diário public API into a local directory."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="data/querido_diario_go",
        type=Path,
        help="Destination directory (default: data/querido_diario_go)",
    )
    parser.add_argument(
        "--limit",
        default=None,
        type=int,
        help="Maximum number of gazette records to fetch (default: unlimited)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    written = fetch_to_disk(output_dir=args.output_dir, limit=args.limit)

    if not written:
        logging.error(
            "No files written — API returned no data. Check connectivity or limits.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logging.info("wrote %s (%.1f KB)", path, size / 1024)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

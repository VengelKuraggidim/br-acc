#!/usr/bin/env python3
"""Download the ICIJ OffshoreLeaks consolidated bundle.

Thin CLI wrapper around :func:`bracc_etl.pipelines.icij.fetch_to_disk`
so the Fiscal Cidadão bootstrap contract can replace the old
"manually place ICIJ CSVs in data/icij/" file_manifest workflow.

Data source: ICIJ OffshoreLeaks Database
(https://offshoreleaks.icij.org/pages/database). The download URL
``full-oldb.LATEST.zip`` always points at the most recent bundle of
Neo4j bulk-import CSVs (Panama / Paradise / Pandora / Bahamas /
Offshore Leaks combined).

The pipeline only consumes 4 of the 6 CSVs in the bundle (entities,
officers, intermediaries, relationships) — ``fetch_to_disk``
selectively extracts those four to keep disk usage manageable
(~600 MB extracted vs. ~73 MB compressed for the full bundle).

Usage::

    # Smoke test — first 1000 rows of each CSV
    uv run --project etl python scripts/download_icij.py \\
        --output-dir /tmp/smoke_icij --limit 1000

    # Full pull (default)
    uv run --project etl python scripts/download_icij.py \\
        --output-dir data/icij
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.icij import (  # noqa: E402
    _ICIJ_DEFAULT_USER_AGENT,
    fetch_to_disk,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the ICIJ OffshoreLeaks bundle, extract the four CSVs "
            "ICIJPipeline reads (entities, officers, intermediaries, "
            "relationships) into <output-dir>/."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/icij"),
        help="Destination directory (default: data/icij).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Accepted for API symmetry; ICIJ publishes a rolling LATEST "
            "snapshot so this is informational only."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate each CSV to the first N data rows (header preserved). "
            "Use for smoke tests; default extracts every row."
        ),
    )
    parser.add_argument(
        "--user-agent",
        default=_ICIJ_DEFAULT_USER_AGENT,
        help="HTTP User-Agent header to send.",
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
    logger = logging.getLogger("download_icij")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        date=args.date,
        limit=args.limit,
        user_agent=args.user_agent,
        timeout=args.timeout,
    )

    if not written:
        logger.error("[download_icij] no files written - check logs above.")
        return 1

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

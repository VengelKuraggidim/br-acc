#!/usr/bin/env python3
"""Download the World Bank Group "Debarred Firms & Individuals" list.

Thin CLI wrapper around :func:`bracc_etl.pipelines.world_bank.fetch_to_disk`
so the Fiscal Cidadao bootstrap contract can flip this source from
``file_manifest`` to ``script_download``.

Data source: public Adobe Experience Manager JSON endpoint behind the
listing page at
https://www.worldbank.org/en/projects-operations/procurement/debarred-firms
(uses the same ``apikey`` header the page's bundled JavaScript sends).

Usage::

    # Full refresh into the pipeline's default data dir:
    uv run --project etl python scripts/download_world_bank.py \\
        --output-dir data/world_bank

    # Smoke test — first 20 rows:
    uv run --project etl python scripts/download_world_bank.py \\
        --output-dir /tmp/smoke_world_bank --limit 20
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from the repo root without installing the etl package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.world_bank import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the World Bank debarred-firms list (JSON) and write "
            "it as data/world_bank/debarred.csv (UTF-8) for "
            "WorldBankPipeline to consume."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/world_bank"),
        help=(
            "Destination directory (default: data/world_bank, created if "
            "missing)."
        ),
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help=(
            "Override for the 'apikey' header. Defaults to the public key "
            "embedded in worldbank.org's bundled JavaScript — update only if "
            "the upstream rotates the key (look for 'propApiKey' in the page "
            "HTML)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Truncate download to the first N rows (smoke tests).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout in seconds (default: 120).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("download_world_bank")

    kwargs: dict = {
        "output_dir": args.output_dir,
        "limit": args.limit,
        "timeout": args.timeout,
    }
    if args.api_key:
        kwargs["api_key"] = args.api_key

    written = fetch_to_disk(**kwargs)

    if not written:
        logger.error("[download_world_bank] no files written - check logs.")
        return 1

    for path in written:
        size = path.stat().st_size
        logger.info("[download_world_bank] wrote %s (%d bytes)", path, size)
    return 0


if __name__ == "__main__":
    sys.exit(main())

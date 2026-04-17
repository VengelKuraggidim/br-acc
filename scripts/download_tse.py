#!/usr/bin/env python3
"""Download TSE candidates + campaign donations filtered to one UF.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.tse.fetch_to_disk` so the Fiscal Cidadao GO-only
bootstrap contract can drop the ``file_manifest`` requirement for the ``tse``
pipeline. Writes ``candidatos.csv`` and ``doacoes.csv`` scoped to the requested
UF (default GO), avoiding the multi-GB national dump.

Data source: TSE public CDN (https://cdn.tse.jus.br/estatistica/sead/odsele/).
Per-year ZIPs are cached under ``<output-dir>/raw/`` for idempotent re-runs.

Default years cover Marconi Perillo-era elections (governador GO 1998-2006 &
2010-2014, senador 2018-2022): 1998, 2002, 2006, 2010, 2014, 2018, 2022.

Usage::

    python3 scripts/download_tse.py --output-dir data/tse

    # Smoke run — one year only:
    python3 scripts/download_tse.py \
        --years 2002 --uf GO --output-dir /tmp/smoke_tse_go
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

from bracc_etl.pipelines.tse import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download TSE candidates and donations for a single UF (default GO) "
            "from the TSE public CDN into a local directory."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/tse"),
        type=Path,
        help="Destination directory (default: data/tse).",
    )
    parser.add_argument(
        "--uf",
        default="GO",
        help="UF to keep (default: GO). Candidates/donations for other UFs are dropped.",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help=(
            "Election years to download (space-separated). "
            "Default: 1998 2002 2006 2010 2014 2018 2022 "
            "(Marconi Perillo-era)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="Per-file HTTP timeout in seconds (default: 600).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if raw ZIPs are already present.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable DEBUG logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    written = fetch_to_disk(
        output_dir=args.output_dir,
        uf=args.uf,
        years=args.years,
        timeout=args.timeout,
        skip_existing=not args.no_skip_existing,
    )

    if not written:
        logging.error(
            "[download_tse] no files written — check logs for HTTP/zip errors.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logging.info("[download_tse] wrote %s (%.1f KB)", path, size / 1024)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

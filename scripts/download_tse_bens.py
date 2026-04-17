#!/usr/bin/env python3
"""Download TSE Bens Declarados (candidate declared assets) filtered to one UF.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.tse_bens.fetch_to_disk`. Replaces the GCP BigQuery
path used by ``etl/scripts/download_tse_bens.py`` with a public-CDN download
that does NOT require a billing project, making it suitable for the Fiscal
Cidadao bootstrap contract.

Data source: TSE public CDN ``bem_candidato_<year>.zip``. For each year we
also pull ``consulta_cand_<year>.zip`` (same CDN) to join SQ_CANDIDATO →
CPF/name/partido, since the raw bens CSV only carries the sequential id.

The CDN only publishes bens for 2006 onward; 1998/2002 are unavailable there,
so the default year list starts at 2006.

Usage::

    python3 scripts/download_tse_bens.py --output-dir data/tse_bens

    # Smoke run — one year only:
    python3 scripts/download_tse_bens.py \
        --years 2014 --uf GO --output-dir /tmp/smoke_tse_bens_go
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

from bracc_etl.pipelines.tse_bens import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download TSE Bens Declarados for a single UF (default GO) "
            "from the TSE public CDN, joined with consulta_cand to resolve CPF."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/tse_bens"),
        type=Path,
        help="Destination directory (default: data/tse_bens).",
    )
    parser.add_argument(
        "--uf",
        default="GO",
        help="UF to keep (default: GO).",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help=(
            "Election years to download. Default: 2006 2010 2014 2018 2022 "
            "(earliest year on the TSE CDN is 2006)."
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
            "[download_tse_bens] no files written — check logs for HTTP/zip errors.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logging.info("[download_tse_bens] wrote %s (%.1f KB)", path, size / 1024)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

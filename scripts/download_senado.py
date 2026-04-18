#!/usr/bin/env python3
"""Download Senado Federal roster + CEAPS expenses scoped to one UF.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.senado.fetch_to_disk` so the Fiscal Cidadao GO-only
bootstrap contract can drop the ``file_manifest`` requirement for the
``senado`` pipeline. Pulls every senator who held a GO mandate at any
legislature (48 onward), enriches with the detail/mandatos endpoints, and
filters the yearly CEAPS CSV dumps to rows whose SENADOR column matches the
UF roster — keeping Marconi Perillo, Ronaldo Caiado, Jorge Kajuru, et al.
without the multi-GB national dump.

Data sources:
  * https://legis.senado.leg.br/dadosabertos/senador/lista/legislatura/{N}
  * https://legis.senado.leg.br/dadosabertos/senador/{codigo}
  * https://legis.senado.leg.br/dadosabertos/senador/{codigo}/mandatos
  * https://www.senado.leg.br/transparencia/LAI/verba/despesa_ceaps_{YYYY}.csv

Usage::

    python3 scripts/download_senado.py --output-dir data/senado

    # Smoke run — cap roster + skip national CEAPS download:
    python3 scripts/download_senado.py \\
        --uf GO --limit 10 --output-dir /tmp/smoke_senado --no-ceaps
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

from bracc_etl.pipelines.senado import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Senado Federal senators and CEAPS expenses filtered to a "
            "single UF (default GO) from the Senado Dados Abertos API."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/senado"),
        type=Path,
        help="Destination directory (default: data/senado).",
    )
    parser.add_argument(
        "--uf",
        default="GO",
        help="UF to keep (default: GO). Roster rows for other UFs are dropped.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the number of senators enriched (useful for smoke runs).",
    )
    parser.add_argument(
        "--legislaturas",
        type=int,
        nargs="+",
        default=None,
        help=(
            "Legislature numbers to scan (space-separated). "
            "Default: 48 49 50 51 52 53 54 55 56 57 "
            "(1987-present)."
        ),
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help=(
            "CEAPS years to download + filter (space-separated). "
            "Default: 2008..current."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Per-request HTTP timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--no-details",
        action="store_true",
        help="Skip the per-senator /senador/{codigo} detail + mandatos probe.",
    )
    parser.add_argument(
        "--no-ceaps",
        action="store_true",
        help="Skip the CEAPS CSV download step (roster-only run).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download CEAPS CSVs even when cached under raw/.",
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
        limit=args.limit,
        legislaturas=args.legislaturas,
        years=args.years,
        timeout=args.timeout,
        fetch_details=not args.no_details,
        fetch_ceaps=not args.no_ceaps,
        skip_existing=not args.no_skip_existing,
    )

    if not written:
        logging.error(
            "[download_senado] no files written — check logs for HTTP errors.",
        )
        return 1

    for path in written:
        size = path.stat().st_size
        logging.info("[download_senado] wrote %s (%.1f KB)", path, size / 1024)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

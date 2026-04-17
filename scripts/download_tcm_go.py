#!/usr/bin/env python3
"""Download TCM-GO (Goias municipal finance) raw data from SICONFI/Tesouro.

Wraps ``bracc_etl.pipelines.tcm_go.fetch_to_disk``, which is the same HTTP
logic the ETL pipeline already uses in its API-fallback path. Running this
script populates ``data/tcm_go/`` with an ``entes.csv`` plus one
``finbra_rreo_<year>.csv`` per requested year, so the TcmGoPipeline local-CSV
path can ingest them without touching the network a second time.

Source: SICONFI Tesouro Nacional datalake
  https://apidatalake.tesouro.gov.br/ords/siconfi/tt/
Scope: UF=GO (IBGE prefix 52), 246 municipalities.
Anexo: RREO Anexo 01 (6 deg bimestre = annual cumulative).
No authentication required.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.tcm_go import RREO_YEARS, fetch_to_disk


def _parse_years(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    years: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            years.append(int(token))
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"invalid year {token!r} in --years (expected CSV of integers)"
            ) from exc
    return years or None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download TCM-GO (SICONFI RREO Anexo 01) raw CSVs for Goias "
            "municipalities. Generates data/tcm_go/entes.csv plus one "
            "finbra_rreo_<year>.csv per requested year."
        )
    )
    parser.add_argument(
        "--output-dir",
        default="data/tcm_go",
        help="Destination directory (default: data/tcm_go, created if missing)",
    )
    parser.add_argument(
        "--limit-municipios",
        type=int,
        default=None,
        help="Restrict to the first N Goias municipalities (default: all ~246)",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help=(
            "CSV list of exercise years to fetch (e.g. '2023,2024'). "
            f"Defaults to {list(RREO_YEARS)}."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    years = _parse_years(args.years)
    output_dir = Path(args.output_dir)

    written = fetch_to_disk(
        output_dir=output_dir,
        limit_municipios=args.limit_municipios,
        years=years,
    )

    print(f"Wrote {len(written)} file(s) to {output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

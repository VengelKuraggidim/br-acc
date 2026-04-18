#!/usr/bin/env python3
"""Download TCU (Tribunal de Contas da União) sanctions data.

Wraps ``bracc_etl.pipelines.tcu.fetch_to_disk`` so the Fiscal Cidadão
bootstrap contract can treat the TCU source as ``script_download`` instead
of ``file_manifest``. See the module docstring in
``etl/src/bracc_etl/pipelines/tcu.py`` for the *big caveats* — in short,
only two of the four datasets the pipeline consumes are reachable through
TCU's public APEX application, and the inabilitados report is missing
UF/MUNICIPIO columns.

Source (public APEX 18, no auth):
    https://contas.tcu.gov.br/ords/f?p=1660:1  (inabilitados - 704 rows)
    https://contas.tcu.gov.br/ords/f?p=1660:2  (inidôneos    - 104 rows)

The "contas julgadas irregulares" and the electoral variant are not
published publicly; ``fetch_to_disk`` writes header-only stub CSVs for
those so ``TcuPipeline.extract()`` does not FileNotFoundError.

Usage::

    # Smoke test — GO-scoped (affects inidôneos only), 5 rows each
    uv run --project etl python scripts/download_tcu.py \\
        --output-dir /tmp/smoke_tcu --uf GO --limit 5

    # Default: GO scope where applicable, everything else full-national
    uv run --project etl python scripts/download_tcu.py \\
        --output-dir data/tcu

    # Keep every UF in inidôneos (full-national slice)
    uv run --project etl python scripts/download_tcu.py \\
        --output-dir data/tcu --uf ALL
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bracc_etl.pipelines.tcu import fetch_to_disk


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


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download TCU sanctions data (inabilitados + inidôneos) from the "
            "public APEX application and write the 4 CSVs TcuPipeline reads "
            "into <output-dir>/. Two of the four are header-only stubs "
            "because their upstream datasets are not public — see "
            "fetch_to_disk docstring."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/tcu"),
        help="Destination directory (default: data/tcu, created if missing).",
    )
    parser.add_argument(
        "--uf",
        default="GO",
        help=(
            "UF code to keep when filtering the inidôneos rows (default: GO). "
            "Pass 'ALL' or an empty string to keep every UF. Note: the "
            "inabilitados report does not expose a UF column, so this flag "
            "does not affect that dataset."
        ),
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help=(
            "Accepted for API symmetry with other download_* scripts; the TCU "
            "reports are rolling snapshots without a year filter, so this is "
            "informational only."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate each scraped dataset to the first N rows (after UF "
            "filtering). Useful for smoke tests."
        ),
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

    uf_token = (args.uf or "").strip().upper() or None
    if uf_token in {"ALL", "*"}:
        uf_token = None

    years = _parse_years(args.years)
    output_dir: Path = args.output_dir

    written = fetch_to_disk(
        output_dir=output_dir,
        uf=uf_token,
        years=years,
        limit=args.limit,
    )

    print(f"Wrote {len(written)} file(s) to {output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

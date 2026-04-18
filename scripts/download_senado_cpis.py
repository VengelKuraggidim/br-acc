#!/usr/bin/env python3
"""Download Senate CPI/CPMI inquiry metadata + requirements to disk.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.senado_cpis.fetch_to_disk`. Writes the CSV layout
:class:`SenadoCpisPipeline.extract` reads:

* ``inquiries.csv``       — active CPIs/CPMIs from
  ``https://legis.senado.leg.br/dadosabertos/comissao/lista/{CPI|CPMI}``.
* ``requirements.csv``    — per-sigla requirements from
  ``/comissao/cpi/{sigla}/requerimentos``.
* ``sessions.csv``        — empty (no Open Data endpoint for reunioes;
  historical sessions need the PDF archive path).
* ``members.csv``         — empty (members are exposed only via
  BigQuery's ``br_senado_federal_dados_abertos``).
* ``history_sources.csv`` — empty (populated by
  ``etl/scripts/download_senado_cpi_archive.py`` when the operator runs it
  alongside this CLI).

The Senado Dados Abertos API is public (no auth). For 1946-2015 historical
CPI coverage, additionally run ``etl/scripts/download_senado_cpi_archive.py``
— it parses the official PDF tables and writes to the same output dir.

Usage::

    # Active commissions + their requirements:
    uv run --project etl python scripts/download_senado_cpis.py \\
        --output-dir data/senado_cpis

    # Smoke test: first 3 commissions only:
    uv run --project etl python scripts/download_senado_cpis.py \\
        --output-dir /tmp/smoke_senado_cpis --limit 3
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

from bracc_etl.pipelines.senado_cpis import fetch_to_disk  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Senate CPI/CPMI inquiry metadata and requirements via "
            "the public Senado Dados Abertos API. No credentials required; "
            "sessions/members are intentionally empty (see script docstring)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/senado_cpis"),
        help=(
            "Directory to write CSVs into. Created if missing. "
            "Defaults to data/senado_cpis (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap on the number of commissions probed for "
            "requirements. Useful for smoke tests; omit to scan all."
        ),
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Per-sigla requirement page ceiling (page size=20). Default: 20.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Per-request HTTP timeout in seconds (default: 60).",
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
        max_pages=args.max_pages,
        timeout=args.timeout,
    )

    if not written:
        print(
            "[download_senado_cpis] no files written — check logs for HTTP "
            "errors or an empty Open Data response.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size if path.exists() else 0
        print(f"[download_senado_cpis] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

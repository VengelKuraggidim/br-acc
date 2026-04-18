#!/usr/bin/env python3
"""Download Camara dos Deputados (federal) data via the v2 open-data API.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.camara.fetch_to_disk` so the Fiscal Cidadao
bootstrap contract can provision ``data/camara/`` without the legacy
"pre-place CEAP CSV annual dumps" manual step.

Hits the five canonical endpoints from
``https://dadosabertos.camara.leg.br/api/v2``:

* ``/deputados`` (optionally filtered by ``siglaUf``),
* ``/deputados/{id}/despesas`` (CEAP),
* ``/proposicoes?idDeputadoAutor={id}``,
* ``/votacoes``,
* ``/orgaos``.

UF-scoped snapshots are namespaced by filename (``deputados_GO.json``,
``despesas_GO.json`` ...), leaving the annual CEAP CSVs under
``data/camara/`` undisturbed — the ``CamaraPipeline.extract`` CSV glob
ignores ``*.json`` files.

Usage::

    # GO-scoped full fetch (federal deputies of Goias, ~18 seats):
    uv run --project etl python scripts/download_camara.py \\
        --uf GO --output-dir data/camara

    # Smoke test with a tiny per-endpoint cap:
    uv run --project etl python scripts/download_camara.py \\
        --uf GO --limit 20 --output-dir /tmp/smoke_camara
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

from bracc_etl.pipelines.camara import fetch_to_disk  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Camara dos Deputados open-data API v2 snapshots "
            "(deputados, CEAP despesas, proposicoes, votacoes, orgaos) "
            "to a local directory, optionally filtered to a single UF."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/camara"),
        help=(
            "Directory to write JSON files into. Created if missing. "
            "Defaults to data/camara (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--uf",
        default=None,
        help=(
            "Two-letter UF filter applied to /deputados via siglaUf "
            "(e.g. 'GO' for Goias — ~18 federal seats). Omit to pull the "
            "full national roster."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional per-endpoint record cap for smoke tests. "
            "Omit to fetch the full payloads."
        ),
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
        uf=args.uf,
        limit=args.limit,
    )

    if not written:
        print(
            "[download_camara] no files written — check logs above "
            "for HTTP errors or empty API responses.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_camara] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

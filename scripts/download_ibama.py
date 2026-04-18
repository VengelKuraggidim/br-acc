#!/usr/bin/env python3
"""Download IBAMA "Termos de Embargo" bulk CSV (CKAN, no auth).

Thin CLI around :func:`bracc_etl.pipelines.ibama.fetch_to_disk` so the
Fiscal Cidadão bootstrap contract can treat the IBAMA source as
``script_download`` instead of ``file_manifest``.

Source (public CKAN, no auth):
    https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo

The CKAN resource is a ZIP wrapping a single ``termo_embargo.csv`` (~45 MB
zipped, ~160 MB unpacked, ~113k rows national). ``fetch_to_disk`` remaps
the modern schema (``NOME_EMBARGADO``, ``UF``, ``MUNICIPIO``, ``DES_TAD``…)
onto the legacy column names ``IbamaPipeline.extract`` reads and writes
the result as ``areas_embargadas.csv`` (``;`` delim, UTF-8) so the pipeline
picks it up with zero additional changes. Columns without a modern
counterpart (``DES_TIPO_BIOMA``) are written empty.

Usage::

    # Smoke test — GO-scoped, 500 rows:
    uv run --project etl python scripts/download_ibama.py \\
        --output-dir /tmp/smoke_ibama --uf GO --limit 500

    # Full national dump (no filter):
    uv run --project etl python scripts/download_ibama.py \\
        --output-dir data/ibama

    # GO-only default (recommended for the GO bootstrap):
    uv run --project etl python scripts/download_ibama.py \\
        --output-dir data/ibama --uf GO
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

from bracc_etl.pipelines.ibama import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download IBAMA Termos de Embargo CSV from the CKAN open-data "
            "portal, remap its columns onto the legacy "
            "areas_embargadas.csv schema, and write it into <output-dir>/ "
            "so IbamaPipeline.extract() picks it up."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/ibama"),
        help="Destination directory (default: data/ibama, created if missing).",
    )
    parser.add_argument(
        "--uf",
        default=None,
        help=(
            "Optional UF code to keep (e.g. 'GO'). Pass 'ALL' or leave unset "
            "to keep every UF. IBAMA CKAN does not expose a server-side UF "
            "filter; filtering happens row-by-row on the client."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Cap the number of rows written (after UF filtering). Useful "
            "for smoke tests on the ~160 MB unpacked CSV."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="HTTP timeout for the CKAN download (seconds, default 300).",
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
        stream=sys.stdout,
    )
    logger = logging.getLogger("download_ibama")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        uf=args.uf,
        limit=args.limit,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_ibama] no files written — check logs above for errors.",
        )
        return 1
    for path in written:
        size = path.stat().st_size if path.exists() else 0
        logger.info("[download_ibama] wrote %s (%d bytes)", path, size)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Download STJ (Superior Tribunal de Justiça) decisions to disk.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.stj_dados_abertos.fetch_to_disk`.

The STJ "Dados Abertos" portal publishes a mixture of statistics PDFs and
ad-hoc CSV snapshots whose schema shifts between releases. The only
automated, column-consistent open source for the decisions table consumed
by ``StjPipeline`` is Base dos Dados' mirror on Google BigQuery
(``basedosdados.br_stj_corte_aberta.decisoes``), which requires an
authenticated GCP billing project.

Behaviour:
  * With ``--billing-project <proj>``: streams the full ``decisoes`` table
    to ``<output-dir>/decisoes.csv``.
  * Without ``--billing-project``: logs a clear "source needs GCP billing"
    warning and exits 0 without writing (so public-mode bootstrap skips
    gracefully). This is a hard external requirement, not a bypassable
    paywall, so the wrapper fails open rather than improvise.

Usage::

    python3 scripts/download_stj_dados_abertos.py \\
        --output-dir data/stj_dados_abertos \\
        --billing-project my-gcp-project

    # Public-mode skip (no BQ project available):
    python3 scripts/download_stj_dados_abertos.py \\
        --output-dir data/stj_dados_abertos
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

from bracc_etl.pipelines.stj_dados_abertos import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download STJ Corte Aberta decisions from Base dos Dados "
            "(BigQuery) to decisoes.csv."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/stj_dados_abertos"),
        type=Path,
        help="Destination directory (default: data/stj_dados_abertos).",
    )
    parser.add_argument(
        "--billing-project",
        default=None,
        help=(
            "GCP project used for BigQuery billing. REQUIRED for an "
            "actual download. Omit to skip (public-mode bootstrap)."
        ),
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if decisoes.csv already exists.",
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
    logger = logging.getLogger("download_stj_dados_abertos")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        billing_project=args.billing_project,
        skip_existing=not args.no_skip_existing,
    )

    if not written:
        if args.billing_project:
            logger.error(
                "[download_stj_dados_abertos] no rows written despite "
                "--billing-project; check BQ auth / table availability.",
            )
            return 2
        logger.info(
            "[download_stj_dados_abertos] skipped (no --billing-project); "
            "bootstrap will proceed without STJ data.",
        )
        return 0

    for path in written:
        size = path.stat().st_size
        logger.info(
            "[download_stj_dados_abertos] wrote %s (%.1f KB)",
            path,
            size / 1024,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Download TSE filiados (party membership) filtered to one UF.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.tse_filiados.fetch_to_disk`.

Unlike candidatos/bens, TSE filiacao partidaria is NOT published on the public
CDN — ``divulgacandcontas.tse.jus.br/filiados`` requires session auth. The
only open, automated source is Base dos Dados' mirror on BigQuery
(``basedosdados.br_tse_filiacao_partidaria.microdados``), which requires an
authenticated GCP billing project.

Behaviour:
  * With ``--billing-project <proj>`` (or ``GOOGLE_CLOUD_PROJECT`` /
    ``GCP_PROJECT_ID`` env vars): streams GO-only rows from BigQuery to
    ``filiados.csv``. The env-var fallback lets the bootstrap contract leave
    the flag off and still ingest in dev where ``.env`` is loaded.
  * Without any project source: logs a clear "source needs GCP billing"
    warning and exits 0 without writing (so public-mode bootstrap can skip
    gracefully). This is a hard external requirement, not a bypassable
    paywall, so the wrapper fails open rather than improvise.

Usage::

    python3 scripts/download_tse_filiados.py --output-dir data/tse_filiados \
        --billing-project my-gcp-project

    # Public-mode skip (no BQ project available):
    python3 scripts/download_tse_filiados.py --output-dir data/tse_filiados
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.tse_filiados import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download TSE filiados (party membership) for a single UF "
            "(default GO) from Base dos Dados on BigQuery."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/tse_filiados"),
        type=Path,
        help="Destination directory (default: data/tse_filiados).",
    )
    parser.add_argument(
        "--uf",
        default="GO",
        help="UF to keep (default: GO).",
    )
    parser.add_argument(
        "--billing-project",
        default=None,
        help=(
            "GCP project used for BigQuery billing. REQUIRED for an actual "
            "download. Omit to skip (public-mode bootstrap)."
        ),
    )
    parser.add_argument(
        "--all-statuses",
        action="store_true",
        help=(
            "Include all filiacao statuses (default: Regular only, active members)."
        ),
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if filiados.csv already exists.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable DEBUG logging.",
    )
    # Placeholder --years for bootstrap-contract symmetry with the other two
    # wrappers; ignored because the BQ table is not year-partitioned in a way
    # that maps 1:1 to election years.
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help="Ignored (filiados is not election-year partitioned). Accepted "
             "for symmetry with download_tse.py / download_tse_bens.py.",
    )
    return parser.parse_args(argv)


def _resolve_billing_project(cli_value: str | None) -> str | None:
    """Resolve --billing-project, falling back to GOOGLE_CLOUD_PROJECT /
    GCP_PROJECT_ID env vars so the bootstrap contract can leave the flag off
    when running in dev with ``.env`` loaded.
    """
    if cli_value:
        return cli_value
    for env_key in ("GOOGLE_CLOUD_PROJECT", "GCP_PROJECT_ID"):
        val = os.environ.get(env_key)
        if val:
            return val
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    billing_project = _resolve_billing_project(args.billing_project)
    if billing_project and not args.billing_project:
        logging.info(
            "[download_tse_filiados] using billing-project from env: %s",
            billing_project,
        )

    written = fetch_to_disk(
        output_dir=args.output_dir,
        uf=args.uf,
        billing_project=billing_project,
        all_statuses=args.all_statuses,
        skip_existing=not args.no_skip_existing,
    )

    if not written:
        # Not an error when billing_project is missing — the helper logs why.
        if billing_project:
            logging.error(
                "[download_tse_filiados] no rows written despite billing-project; "
                "check BQ auth and UF filter.",
            )
            return 1
        logging.info(
            "[download_tse_filiados] skipped (no billing-project from CLI or env); "
            "bootstrap will proceed without filiados data.",
        )
        return 0

    for path in written:
        size = path.stat().st_size
        logging.info("[download_tse_filiados] wrote %s (%.1f KB)", path, size / 1024)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

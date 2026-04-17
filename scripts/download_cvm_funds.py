#!/usr/bin/env python3
"""Download CVM fund registry (cad_fi.csv) data.

Thin CLI wrapper around the existing ETL downloader at
``etl/scripts/download_cvm_funds.py`` so the Fiscal Cidadao bootstrap contract
can promote the ``cvm_funds`` pipeline from ``file_manifest`` to
``script_download``.

Source: CVM Dados Abertos — Fundos de Investimento
    https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv

The upstream CSV is a single *rolling snapshot* (not year-partitioned), so
``--year`` is accepted for contract-template symmetry but does not change the
download target. It is logged as informational only.

Usage::

    python3 scripts/download_cvm_funds.py --output-dir data/cvm_funds

    # smoke run
    python3 scripts/download_cvm_funds.py --output-dir /tmp/smoke_cvm_funds --year 2024
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from the repo root without installing the etl package: expose
# both etl/src (bracc_etl package) and etl/scripts (shared _download_utils)
# on sys.path so we can reuse the existing HTTP logic without duplication.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
_ETL_SCRIPTS = _REPO_ROOT / "etl" / "scripts"
for _path in (_ETL_SRC, _ETL_SCRIPTS):
    if _path.is_dir() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _download_utils import download_file, validate_csv  # noqa: E402

logger = logging.getLogger(__name__)

# CVM Dados Abertos — Fund registry snapshot (cad_fi.csv).
CAD_FI_URL = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download CVM fund registry data (cad_fi.csv) — the full rolling "
            "snapshot consumed by CvmFundsPipeline."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/cvm_funds"),
        type=Path,
        help="Destination directory (default: data/cvm_funds, created if missing).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help=(
            "Accepted for contract-template symmetry only; CVM fund registry is "
            "a rolling snapshot (not year-partitioned), so this is informational."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Per-file HTTP timeout in seconds (default: 300).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if cad_fi.csv already exists.",
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

    if args.year is not None:
        logger.info(
            "[download_cvm_funds] --year=%s ignored (cad_fi.csv is a rolling snapshot)",
            args.year,
        )

    out: Path = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    dest = out / "cad_fi.csv"
    skip_existing = not args.no_skip_existing
    if skip_existing and dest.exists():
        logger.info(
            "[download_cvm_funds] skipping — %s already present (%.1f KB)",
            dest, dest.stat().st_size / 1024,
        )
        return 0

    logger.info("[download_cvm_funds] fetching %s", CAD_FI_URL)
    if not download_file(CAD_FI_URL, dest, timeout=args.timeout):
        logger.error("[download_cvm_funds] failed to download %s", CAD_FI_URL)
        return 1

    validate_csv(dest, encoding="latin-1", sep=";")
    logger.info("[download_cvm_funds] wrote %s (%.1f KB)", dest, dest.stat().st_size / 1024)
    logger.info("[download_cvm_funds] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Download CVM PAS (Processo Administrativo Sancionador) data.

Thin CLI wrapper around the existing ETL downloader at
``etl/scripts/download_cvm.py`` so the Fiscal Cidadao bootstrap contract can
promote the ``cvm`` pipeline from ``file_manifest`` to ``script_download``.

Source: CVM Dados Abertos — Processo Sancionador
    https://dados.cvm.gov.br/dados/PROCESSO/SANCIONADOR/DADOS/processo_sancionador.zip

The upstream ZIP is a single *rolling snapshot* (not year-partitioned), so
``--year`` is accepted for contract-template symmetry but does not change the
download target. It is logged as informational only.

Usage::

    python3 scripts/download_cvm.py --output-dir data/cvm

    # smoke run
    python3 scripts/download_cvm.py --output-dir /tmp/smoke_cvm --year 2024
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

from _download_utils import download_file, extract_zip, validate_csv  # noqa: E402

logger = logging.getLogger(__name__)

# CVM open data portal — PROCESSO/SANCIONADOR path (rolling ZIP snapshot).
ZIP_URL = (
    "https://dados.cvm.gov.br/dados/PROCESSO/SANCIONADOR/DADOS/processo_sancionador.zip"
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download CVM PAS sanctions data (processo_sancionador.zip) and "
            "extract processo_sancionador.csv + processo_sancionador_acusado.csv "
            "into the output directory. Idempotent when --skip-existing is set."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data/cvm"),
        type=Path,
        help="Destination directory (default: data/cvm, created if missing).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help=(
            "Accepted for contract-template symmetry only; CVM PAS is a rolling "
            "snapshot (not year-partitioned), so this argument is informational."
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
        help="Re-download and re-extract even if CSVs already exist.",
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
            "[download_cvm] --year=%s ignored (CVM PAS is a rolling snapshot)",
            args.year,
        )

    out: Path = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    skip_existing = not args.no_skip_existing
    existing_csvs = sorted(out.glob("*.csv"))
    if skip_existing and existing_csvs:
        logger.info(
            "[download_cvm] skipping — %d CSV(s) already present in %s",
            len(existing_csvs), out,
        )
        for p in existing_csvs:
            logger.info("[download_cvm] existing %s (%.1f KB)", p, p.stat().st_size / 1024)
        return 0

    zip_dest = out / "processo_sancionador.zip"
    logger.info("[download_cvm] fetching %s", ZIP_URL)
    if not download_file(ZIP_URL, zip_dest, timeout=args.timeout):
        logger.error("[download_cvm] failed to download %s", ZIP_URL)
        return 1

    extracted = extract_zip(zip_dest, out)
    if not extracted:
        logger.error("[download_cvm] extraction produced no files from %s", zip_dest)
        return 1

    csv_files = sorted(out.glob("*.csv"))
    for f in csv_files:
        validate_csv(f, encoding="latin-1", sep=";")
        logger.info("[download_cvm] wrote %s (%.1f KB)", f, f.stat().st_size / 1024)

    if not csv_files:
        logger.error("[download_cvm] no CSVs ended up in %s", out)
        return 1

    logger.info("[download_cvm] done — %d CSV(s) in %s", len(csv_files), out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Download the CGU PEP (Pessoas Expostas Politicamente) monthly dump.

Thin CLI around :func:`bracc_etl.pipelines.pep_cgu.fetch_to_disk` so the
Fiscal Cidadão bootstrap contract can treat the PEP CGU source as
``script_download`` instead of ``file_manifest``.

Source (public, no auth):
    https://portaldatransparencia.gov.br/download-de-dados/pep/<YYYYMM>

That URL 302-redirects to a CloudFront-backed ZIP
(``dadosabertos-download.cgu.gov.br/.../YYYYMM_PEP.zip``, ~2 MB) holding a
single ``YYYYMM_PEP.csv`` (latin-1, ``;`` delim, ~16 MB unpacked,
~130k rows nationally). ``fetch_to_disk`` extracts the inner CSV to
``<output-dir>/pep.csv`` — the exact path + dialect the pipeline reads.

When ``--month`` is omitted, the current UTC month is tried first and then
the function walks back month-by-month (up to ``--max-fallback``) until a
published ZIP is found. The Portal is typically 1-2 months behind.

Usage::

    # Smoke test / default — latest available month
    uv run --project etl python scripts/download_pep_cgu.py \\
        --output-dir /tmp/smoke_pep_cgu

    # Pin a specific month
    uv run --project etl python scripts/download_pep_cgu.py \\
        --output-dir data/pep_cgu --month 202602
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

from bracc_etl.pipelines.pep_cgu import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the CGU PEP monthly dump from Portal da Transparência "
            "and write the inner CSV as <output-dir>/pep.csv so "
            "PepCguPipeline.extract() picks it up."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pep_cgu"),
        help="Destination directory (default: data/pep_cgu, created if missing).",
    )
    parser.add_argument(
        "--month",
        default=None,
        help=(
            "Pin a specific month in YYYYMM form (e.g. 202602). When omitted "
            "(the default) the script tries the current UTC month and walks "
            "back up to --max-fallback months until it finds a published ZIP."
        ),
    )
    parser.add_argument(
        "--max-fallback",
        type=int,
        default=12,
        help=(
            "How many months back to try when --month is not given. "
            "Default: 12 (the Portal typically lags 1-2 months, so this is "
            "plenty of slack)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="HTTP timeout per request in seconds (default: 180).",
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
    logger = logging.getLogger("download_pep_cgu")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        month=args.month,
        timeout=args.timeout,
        max_fallback=args.max_fallback,
    )

    if not written:
        logger.error(
            "[download_pep_cgu] no files written — check logs above for errors.",
        )
        return 1
    for path in written:
        size = path.stat().st_size if path.exists() else 0
        logger.info("[download_pep_cgu] wrote %s (%d bytes)", path, size)
    return 0


if __name__ == "__main__":
    sys.exit(main())

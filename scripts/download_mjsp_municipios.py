#!/usr/bin/env python3
"""Download the MJSP/SINESP municipal homicide-doloso XLSX to disk.

Thin CLI wrapper around
``bracc_etl.pipelines.mjsp_municipios.fetch_to_disk``. Resolves the
current download URL via ``dados.mj.gov.br`` CKAN ``package_show`` and
falls back to the pinned URL when CKAN is unreachable.

Usage:
    uv run --project etl python scripts/download_mjsp_municipios.py \\
        --output-dir data/mjsp_municipios

The XLSX is a single ~10 MB file covering every UF; the pipeline reads
only the GO sheet at extract time.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from the repo root without installing the etl package:
# inject etl/src on sys.path so ``import bracc_etl`` resolves. When run
# via ``uv run --project etl``, this is a no-op.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SRC = _REPO_ROOT / "etl" / "src"
if _ETL_SRC.is_dir() and str(_ETL_SRC) not in sys.path:
    sys.path.insert(0, str(_ETL_SRC))

from bracc_etl.pipelines.mjsp_municipios import fetch_to_disk  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the MJSP/SINESP municipal homicide-doloso XLSX "
            "from dados.mj.gov.br into a local directory."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/mjsp_municipios"),
        help=(
            "Directory to write the XLSX into. Created if missing. "
            "Defaults to data/mjsp_municipios (the pipeline's expected path)."
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

    written = fetch_to_disk(output_dir=args.output_dir)

    if not written:
        print(
            "[download_mjsp_municipios] no files written — check logs "
            "above for HTTP/CKAN errors.",
            file=sys.stderr,
        )
        return 1

    for path in written:
        size = path.stat().st_size
        print(f"[download_mjsp_municipios] wrote {path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Download BNDES "Operações não automáticas" CSV to disk.

Thin CLI wrapper around :func:`bracc_etl.pipelines.bndes.fetch_to_disk`
so the Fiscal Cidadão bootstrap contract can drop the old "place
``operacoes-nao-automaticas.csv`` manually" file_manifest workflow in
favour of automated ingestion.

Data source: BNDES Open Data CKAN catalogue
(https://dadosabertos.bndes.gov.br/dataset/operacoes-financiamento).
The "Operações não automáticas" resource is a single semicolon-
separated, latin-1-encoded national CSV (~20 MB) containing every
non-automatic credit operation BNDES has contracted directly or via
analyst-reviewed indirect channels since 2002. Schema matches what
``BndesPipeline.extract`` consumes verbatim, so no remap is needed.

Usage::

    # Default: full national dump (matches pipeline's expected layout)
    uv run --project etl python scripts/download_bndes.py \\
        --output-dir data/bndes

    # GO-only slice
    uv run --project etl python scripts/download_bndes.py \\
        --output-dir data/bndes --uf GO

    # Smoke test — first 1000 rows
    uv run --project etl python scripts/download_bndes.py \\
        --output-dir /tmp/smoke_bndes --limit 1000
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

from bracc_etl.pipelines.bndes import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the BNDES Operações não automáticas CSV from the "
            "open-data CKAN catalogue and write operacoes-nao-automaticas.csv "
            "into <output-dir>/."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/bndes"),
        help="Destination directory (default: data/bndes, created if missing).",
    )
    parser.add_argument(
        "--uf",
        default=None,
        help=(
            "Optional UF code (two-letter) to filter rows on the upstream "
            "'uf' column. Default: keep all UFs (full national dump)."
        ),
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Accepted for API symmetry with other download_* scripts; the "
            "BNDES CKAN feed is a rolling consolidated CSV without a date "
            "snapshot, so this is informational only."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate to the first N rows after UF filtering. Useful for "
            "smoke tests (default: keep every row)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout for the CKAN download in seconds (default: 120).",
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
    logger = logging.getLogger("download_bndes")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        date=args.date,
        uf=args.uf,
        limit=args.limit,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_bndes] no files written - check logs above for HTTP errors.",
        )
        return 1

    print(f"Wrote {len(written)} file(s) to {args.output_dir.resolve()}:")
    for p in written:
        size = p.stat().st_size if p.exists() else 0
        print(f"  {p}  ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

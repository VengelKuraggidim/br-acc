#!/usr/bin/env python3
"""Download Portal-emendas ZIP and split out TransfereGov inputs.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.transferegov.fetch_to_disk`.

IMPORTANT: despite the pipeline name, the upstream feed for
``TransferegovPipeline`` is the Portal da Transparência
``/download-de-dados/emendas-parlamentares/<YYYYMMDD>`` endpoint — NOT
``/transferencias/`` (different schema). The consolidated ZIP contains
three CSVs and transferegov is the only consumer of the
``_Convenios.csv`` and ``_PorFavorecido.csv`` auxiliaries that
``siop`` and ``tesouro_emendas`` skip.

The Portal accepts any date token in the URL (it always serves the
latest consolidated ZIP); the ``--date`` flag is a cache key.

Usage::

    # Smoke (truncate every CSV to 5000 rows after extraction)
    uv run --project etl python scripts/download_transferegov.py \\
        --output-dir /tmp/smoke_transferegov --limit 5000

    # Full download (~234 MB unpacked; the _PorFavorecido.csv alone is 167 MB)
    uv run --project etl python scripts/download_transferegov.py \\
        --output-dir data/transferegov
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

from bracc_etl.pipelines.transferegov import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the consolidated Portal da Transparência "
            "emendas-parlamentares ZIP and write the 3 CSVs "
            "TransferegovPipeline.extract consumes "
            "(EmendasParlamentares.csv, _Convenios.csv, _PorFavorecido.csv) "
            "into --output-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/transferegov"),
        help="Destination directory (default: data/transferegov, created if missing).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Optional YYYYMMDD cache key. The Portal endpoint accepts any "
            "date token syntactically and always serves the latest "
            "consolidated ZIP, so this only affects the raw-zip filename. "
            "Default: today (UTC)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Truncate each output CSV to the first N data rows after "
            "extraction (header preserved). Useful for smoke tests; the "
            "full _PorFavorecido.csv is 167 MB."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="HTTP timeout in seconds (default: 600).",
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
    logger = logging.getLogger("download_transferegov")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        date=args.date,
        limit=args.limit,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_transferegov] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size if path.exists() else 0
        logger.info(
            "[download_transferegov] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

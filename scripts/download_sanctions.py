#!/usr/bin/env python3
"""Download CEIS + CNEP sanctions snapshots to disk.

Thin CLI wrapper around
:func:`bracc_etl.pipelines.sanctions.fetch_to_disk`.

Two CGU "Portal da Transparência" widgets (mode "DIA") are scraped:

* ``/download-de-dados/ceis``  → ``data/sanctions/ceis.csv``
* ``/download-de-dados/cnep``  → ``data/sanctions/cnep.csv``

Each landing page embeds the current snapshot date in an inline
``arquivos.push({...})`` block; the download URL is
``/download-de-dados/<ceis|cnep>/<YYYYMMDD>``. Other dates 403 (widget
DIA only serves the latest published day).

The accented uppercase CGU headers (``"CPF OU CNPJ DO SANCIONADO"``,
``"DATA INÍCIO SANÇÃO"``, ``"FUNDAMENTAÇÃO LEGAL"``, …) are remapped
in-memory to the pipeline's snake_case schema (``cpf_cnpj``, ``nome``,
``data_inicio``, ``data_fim``, ``motivo``).

Usage::

    # Auto-discover the latest snapshot for each widget
    uv run --project etl python scripts/download_sanctions.py \\
        --output-dir data/sanctions

    # Pin both widgets to a specific snapshot date
    uv run --project etl python scripts/download_sanctions.py \\
        --output-dir /tmp/smoke_sanctions --date 20260417
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

from bracc_etl.pipelines.sanctions import fetch_to_disk  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the CGU CEIS + CNEP sanctions snapshots, remap their "
            "accented uppercase headers to the pipeline's snake_case schema, "
            "and write ceis.csv + cnep.csv into --output-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/sanctions"),
        help="Destination directory (default: data/sanctions, created if missing).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Explicit YYYYMMDD snapshot date applied to BOTH widgets. "
            "Defaults to the latest date scraped from each landing page. "
            "Only the currently-published snapshot is reachable — other "
            "dates 403 (widget mode DIA)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="HTTP timeout in seconds (default: 180).",
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
    logger = logging.getLogger("download_sanctions")

    written = fetch_to_disk(
        output_dir=args.output_dir,
        date=args.date,
        timeout=args.timeout,
    )

    if not written:
        logger.error(
            "[download_sanctions] no files written -- check logs above.",
        )
        return 1

    for path in written:
        size = path.stat().st_size if path.exists() else 0
        logger.info(
            "[download_sanctions] wrote %s (%d bytes)", path, size,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

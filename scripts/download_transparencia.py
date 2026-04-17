#!/usr/bin/env python3
"""Download Portal da Transparencia federal spending data to disk.

Thin CLI wrapper around ``etl/scripts/download_transparencia.py`` so the
Fiscal Cidadao bootstrap contract can drop the "pre-place files under
``data/transparencia/`` manually" workflow in favour of automated
ingestion. The underlying downloader fetches the monthly/yearly ZIPs
published by the CGU (https://portaldatransparencia.gov.br/download-de-dados/),
extracts them, and writes normalised ``contratos.csv``/``servidores.csv``/
``emendas.csv`` files that ``TransparenciaPipeline.extract`` consumes.

Datasets (matching the underlying script's ``--datasets`` flag):

* ``compras`` — monthly contract ZIPs (processes ``*_Compras.csv`` only).
* ``servidores`` — monthly SIAPE cadastro + remuneracao, joined on
  ``Id_SERVIDOR_PORTAL``.
* ``emendas`` — yearly parliamentary amendments ZIP.

No credentials are required: the ``/download-de-dados/`` bulk endpoints are
open. ``TRANSPARENCIA_API_KEY`` in ``.env`` is only used by the separate
``/api-de-dados/`` JSON endpoints (not by this path).

Usage::

    # Full historical fetch (all datasets, ~5 recent years):
    uv run --project etl python scripts/download_transparencia.py \\
        --output-dir data/transparencia

    # Smoke test — one year, one dataset (emendas is the smallest):
    uv run --project etl python scripts/download_transparencia.py \\
        --output-dir /tmp/smoke_transparencia --year 2024 --datasets emendas
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ETL_SCRIPT = _REPO_ROOT / "etl" / "scripts" / "download_transparencia.py"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Portal da Transparencia bulk data (compras, "
            "servidores, emendas) into a local directory. Delegates to "
            "etl/scripts/download_transparencia.py so HTTP/extraction "
            "logic is not duplicated."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/transparencia"),
        help=(
            "Destination directory. Created if missing. Defaults to "
            "data/transparencia (the pipeline's expected path)."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        default=None,
        help=(
            "Year to download. May be repeated to fetch multiple years "
            "(e.g. --year 2022 --year 2023). Defaults to the last 5 "
            "completed years relative to today."
        ),
    )
    parser.add_argument(
        "--datasets",
        default="compras,servidores,emendas",
        help=(
            "Comma-separated datasets to pull. Valid values: "
            "'compras', 'servidores', 'emendas'. Defaults to all three."
        ),
    )
    parser.add_argument(
        "--months",
        default=None,
        help=(
            "Optional comma-separated month list (1-12) for monthly "
            "datasets (compras, servidores). Defaults to all 12 months."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Per-file HTTP timeout in seconds (default: 600).",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download even if raw ZIPs are already present.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def _default_years() -> list[int]:
    """Last 5 completed years (inclusive of current year)."""
    from datetime import date
    current = date.today().year
    return list(range(current - 4, current + 1))


def _build_invocation(
    *,
    year: int,
    datasets: str,
    output_dir: Path,
    months: str | None,
    timeout: int,
    skip_existing: bool,
) -> list[str]:
    """Assemble the argv to invoke the underlying etl/scripts downloader."""
    cmd = [
        sys.executable,
        str(_ETL_SCRIPT),
        "--year",
        str(year),
        "--datasets",
        datasets,
        "--output-dir",
        str(output_dir),
        "--timeout",
        str(timeout),
    ]
    if not skip_existing:
        cmd.append("--no-skip-existing")
    if months:
        for token in months.split(","):
            token = token.strip()
            if token:
                cmd.extend(["--months", token])
    return cmd


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("download_transparencia")

    if not _ETL_SCRIPT.is_file():
        logger.error("underlying downloader not found: %s", _ETL_SCRIPT)
        return 1

    years = args.year if args.year else _default_years()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    overall_rc = 0
    for year in years:
        cmd = _build_invocation(
            year=year,
            datasets=args.datasets,
            output_dir=output_dir,
            months=args.months,
            timeout=args.timeout,
            skip_existing=not args.no_skip_existing,
        )
        logger.info("[download_transparencia] year=%s -> %s", year, " ".join(cmd))
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            logger.warning(
                "[download_transparencia] year %s exited with rc=%s",
                year,
                result.returncode,
            )
            overall_rc = result.returncode

    # Report what got written so callers (bootstrap) can see artefacts.
    for name in ("contratos.csv", "servidores.csv", "emendas.csv"):
        path = output_dir / name
        if path.is_file():
            size = path.stat().st_size
            logger.info("[download_transparencia] wrote %s (%d bytes)", path, size)

    return overall_rc


if __name__ == "__main__":
    sys.exit(main())

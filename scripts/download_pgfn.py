#!/usr/bin/env python3
"""Download PGFN "Dívida Ativa da União" quarterly open-data dumps.

Source (no auth, Apache autoindex):
    https://dadosabertos.pgfn.gov.br/

Each quarter exposes three ZIP archives:
    - ``Dados_abertos_FGTS.zip``              (FGTS contributions)
    - ``Dados_abertos_Previdenciario.zip``    (INSS / Previdenciário)
    - ``Dados_abertos_Nao_Previdenciario.zip`` (SIDA — what PgfnPipeline reads)

The ETL pipeline at ``bracc_etl.pipelines.pgfn`` only consumes files matching
``arquivo_lai_SIDA_*_*.csv`` (the non-previdenciário bucket), so this script
defaults to that bucket. The other buckets can be opted in via ``--include``.

CSVs inside the archives are semicolon-delimited, latin-1 encoded, with the
column ``UF_DEVEDOR``. When ``--uf`` is set (default ``GO`` for the
Fiscal Cidadão deployment), rows for other UFs are filtered out while the
archive is being extracted so the final CSVs on disk are already GO-scoped.
Use ``--uf ALL`` (or ``--uf ""``) to keep every row.

Usage::

    # Smoke test: a single quarter, GO-scoped, written under /tmp
    uv run --project etl python scripts/download_pgfn.py \\
        --output-dir /tmp/smoke_pgfn --period 2024-Q4

    # Default historical window (last 3 years, all quarters available, GO)
    uv run --project etl python scripts/download_pgfn.py \\
        --output-dir data/pgfn

    # Keep every UF (equivalent to the raw PGFN dump)
    uv run --project etl python scripts/download_pgfn.py \\
        --output-dir data/pgfn --uf ALL
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterable

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("download_pgfn")

BASE_URL = "https://dadosabertos.pgfn.gov.br"

# Autoindex link pattern: <a href="2024_trimestre_04/"> 2024_trimestre_04/</a>
_PERIOD_HREF_RE = re.compile(r'href="(\d{4}_trimestre_\d{2})/"')
_ZIP_HREF_RE = re.compile(r'href="([^"]+\.zip)"')

BUCKETS: dict[str, str] = {
    "nao_previdenciario": "Dados_abertos_Nao_Previdenciario.zip",
    "previdenciario": "Dados_abertos_Previdenciario.zip",
    "fgts": "Dados_abertos_FGTS.zip",
}

# Column index of UF_DEVEDOR in the SIDA/PREV/FGTS schema (0-based), header:
#   CPF_CNPJ;TIPO_PESSOA;TIPO_DEVEDOR;NOME_DEVEDOR;UF_DEVEDOR;...
_UF_COL_INDEX = 4

# ZIP bomb guard: SIDA uncompresses to ~3-4 GB per quarter, leave generous slack
_MAX_EXTRACT_BYTES = 20 * 1024**3

# --------------------------------------------------------------------------
# Period parsing
# --------------------------------------------------------------------------


def _period_to_path(period: str) -> str:
    """Normalize 'YYYY-Qn' (and a few variants) to 'YYYY_trimestre_0n'.

    Accepts: '2024-Q4', '2024Q4', '2024-4', '2024_trimestre_04'.
    """
    period = period.strip()
    m = re.fullmatch(r"(\d{4})[-_]?[Qq]?(\d)", period)
    if m:
        year, q = m.group(1), int(m.group(2))
        if not 1 <= q <= 4:
            raise argparse.ArgumentTypeError(
                f"--period quarter must be 1-4, got {period!r}"
            )
        return f"{year}_trimestre_{q:02d}"
    m = re.fullmatch(r"(\d{4})_trimestre_(\d{2})", period)
    if m:
        return period
    raise argparse.ArgumentTypeError(
        f"--period must look like 'YYYY-Qn' (e.g. 2024-Q4), got {period!r}"
    )


def _list_remote_periods(client: httpx.Client) -> list[str]:
    """Enumerate every quarterly directory published by PGFN."""
    resp = client.get(BASE_URL + "/", timeout=30.0)
    resp.raise_for_status()
    periods = sorted(set(_PERIOD_HREF_RE.findall(resp.text)))
    if not periods:
        raise RuntimeError(
            f"No quarterly directories found at {BASE_URL}/ — "
            "is the autoindex page still live?"
        )
    return periods


def _list_remote_zips(client: httpx.Client, period_dir: str) -> list[str]:
    url = f"{BASE_URL}/{period_dir}/"
    resp = client.get(url, timeout=30.0)
    resp.raise_for_status()
    return sorted(set(_ZIP_HREF_RE.findall(resp.text)))


def _default_periods(years: int) -> list[str]:
    """Return the last ``years`` worth of quarter tags (YYYY_trimestre_0n)."""
    now = datetime.utcnow()
    out: list[str] = []
    for year in range(now.year - years + 1, now.year + 1):
        for q in range(1, 5):
            out.append(f"{year}_trimestre_{q:02d}")
    return out


# --------------------------------------------------------------------------
# Download + UF-filtering extraction
# --------------------------------------------------------------------------


def _download(client: httpx.Client, url: str, dest: Path, timeout: float = 1200.0) -> bool:
    """Stream ``url`` into ``dest`` with resume support. Returns True on success."""
    partial = dest.with_suffix(dest.suffix + ".partial")
    start = partial.stat().st_size if partial.exists() else 0
    headers: dict[str, str] = {}
    if start > 0:
        headers["Range"] = f"bytes={start}-"
        logger.info("Resuming %s from %.1f MB", dest.name, start / 1e6)

    try:
        with client.stream("GET", url, headers=headers, timeout=timeout) as resp:
            if resp.status_code == 416:
                if partial.exists():
                    partial.rename(dest)
                logger.info("Already complete: %s", dest.name)
                return True
            resp.raise_for_status()
            if start > 0 and resp.status_code != 206:
                logger.warning(
                    "Server ignored Range for %s, restarting", dest.name,
                )
                start = 0
            total = resp.headers.get("content-length")
            total_mb = f"{(int(total) + start) / 1e6:.1f} MB" if total else "unknown size"
            logger.info("Downloading %s (%s)…", dest.name, total_mb)

            mode = "ab" if start > 0 and resp.status_code == 206 else "wb"
            with open(partial, mode) as fh:
                for chunk in resp.iter_bytes(chunk_size=1 << 16):
                    fh.write(chunk)
        partial.rename(dest)
        logger.info("Downloaded %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
        return True
    except httpx.HTTPError as exc:
        logger.error("Download failed for %s: %s", url, exc)
        return False


def _iter_sida_members(zf: zipfile.ZipFile, include_all: bool) -> Iterable[zipfile.ZipInfo]:
    for info in zf.infolist():
        name = Path(info.filename).name
        if not name.lower().endswith(".csv"):
            continue
        # Pipeline consumes arquivo_lai_SIDA_*_*.csv only; other buckets use
        # FGTS/PREV prefixes. ``include_all`` keeps everything so --include
        # previdenciario/fgts still write something useful if requested.
        if not include_all and not name.startswith("arquivo_lai_SIDA_"):
            continue
        yield info


def _extract_with_uf_filter(
    zip_path: Path,
    output_dir: Path,
    uf_filter: str | None,
    include_all: bool,
) -> list[Path]:
    """Extract CSVs from ``zip_path``; optionally keep only rows for ``uf_filter``.

    Writes each CSV directly into ``output_dir`` (flat), re-encoded
    byte-for-byte (latin-1) and preserving the original header. Returns the
    list of written files.

    ZIP-bomb / path-traversal guards applied.
    """
    written: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        # Guards
        resolved_out = output_dir.resolve()
        total = 0
        for info in zf.infolist():
            target = (output_dir / Path(info.filename).name).resolve()
            if not target.is_relative_to(resolved_out):
                raise ValueError(
                    f"Path traversal blocked in {zip_path.name}: {info.filename}"
                )
            total += info.file_size
        if total > _MAX_EXTRACT_BYTES:
            raise ValueError(
                f"ZIP bomb guard tripped on {zip_path.name}: "
                f"would extract {total / 1e9:.1f} GB"
            )

        for info in _iter_sida_members(zf, include_all=include_all):
            out_name = Path(info.filename).name
            dest = output_dir / out_name
            tmp = dest.with_suffix(dest.suffix + ".partial")
            if uf_filter is None:
                # No UF filter: straight copy.
                with zf.open(info) as src, open(tmp, "wb") as dst:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        dst.write(chunk)
                tmp.rename(dest)
                logger.info(
                    "Extracted %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6,
                )
                written.append(dest)
                continue

            # UF filter: read line-by-line in latin-1, keep header + matching rows.
            uf_token = uf_filter.encode("latin-1")
            kept = 0
            total_rows = 0
            with zf.open(info) as src, open(tmp, "wb") as dst:
                header_line = src.readline()
                if not header_line:
                    logger.warning("Empty CSV inside %s: %s", zip_path.name, info.filename)
                    tmp.unlink(missing_ok=True)
                    continue
                dst.write(header_line)
                for raw in src:
                    total_rows += 1
                    # UF_DEVEDOR column; rows are semicolon-separated, never quoted
                    # (confirmed against 2024-Q4 FGTS sample).
                    fields = raw.split(b";", _UF_COL_INDEX + 2)
                    if len(fields) <= _UF_COL_INDEX:
                        continue
                    if fields[_UF_COL_INDEX] == uf_token:
                        dst.write(raw)
                        kept += 1
            tmp.rename(dest)
            logger.info(
                "Extracted %s: kept %d/%d rows matching UF=%s (%.1f MB)",
                dest.name,
                kept,
                total_rows,
                uf_filter,
                dest.stat().st_size / 1e6,
            )
            written.append(dest)
    return written


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download PGFN Dívida Ativa da União quarterly open-data dumps "
            f"from {BASE_URL}/ and drop SIDA CSVs (arquivo_lai_SIDA_*_*.csv) "
            "into <output-dir>/ so PgfnPipeline.extract() picks them up."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pgfn"),
        help="Destination directory (default: data/pgfn).",
    )
    parser.add_argument(
        "--period",
        action="append",
        default=None,
        help=(
            "Quarter tag to fetch (e.g. '2024-Q4', '2024Q4', "
            "'2024_trimestre_04'). Repeat to fetch multiple. "
            "Overrides --years when set."
        ),
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help=(
            "How many years back (inclusive of the current year) to fetch "
            "when --period is not set. Default: 3."
        ),
    )
    parser.add_argument(
        "--include",
        default="nao_previdenciario",
        help=(
            "Comma-separated archive buckets to fetch. Options: "
            f"{', '.join(BUCKETS)}. Default: nao_previdenciario (the only "
            "bucket the PgfnPipeline currently consumes)."
        ),
    )
    parser.add_argument(
        "--uf",
        default="GO",
        help=(
            "UF code to keep when streaming CSV rows out of the ZIPs "
            "(default: GO). Pass 'ALL' (or an empty string) to keep every UF."
        ),
    )
    parser.add_argument(
        "--keep-zips",
        action="store_true",
        help=(
            "Keep the downloaded ZIPs after extraction (default: delete them "
            "to save ~1 GB per quarter once the CSVs are on disk)."
        ),
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip a (period, bucket) pair if its CSVs already exist. Default: on.",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Always re-download/re-extract even if CSVs already exist.",
    )
    return parser.parse_args(argv)


def _resolve_periods(client: httpx.Client, args: argparse.Namespace) -> list[str]:
    remote = _list_remote_periods(client)
    logger.info("Remote publishes %d quarters: %s…%s", len(remote), remote[0], remote[-1])
    if args.period:
        wanted = [_period_to_path(p) for p in args.period]
    else:
        wanted = _default_periods(args.years)

    resolved: list[str] = []
    for w in wanted:
        if w in remote:
            resolved.append(w)
        else:
            logger.warning("Period not published yet (skipping): %s", w)
    return resolved


def _resolve_buckets(include: str) -> list[str]:
    tokens = [t.strip().lower() for t in include.split(",") if t.strip()]
    unknown = [t for t in tokens if t not in BUCKETS]
    if unknown:
        raise SystemExit(
            f"--include has unknown bucket(s): {unknown}. "
            f"Valid: {sorted(BUCKETS)}",
        )
    return tokens or ["nao_previdenciario"]


def _resolve_uf(raw: str) -> str | None:
    token = (raw or "").strip().upper()
    if token in {"", "ALL", "*"}:
        return None
    if not re.fullmatch(r"[A-Z]{2}", token):
        raise SystemExit(
            f"--uf must be a 2-letter UF code, 'ALL', or empty; got {raw!r}",
        )
    return token


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    zips_dir = args.output_dir / "_raw_zips"
    zips_dir.mkdir(parents=True, exist_ok=True)

    buckets = _resolve_buckets(args.include)
    uf = _resolve_uf(args.uf)
    include_all_members = "previdenciario" in buckets or "fgts" in buckets

    written_any = False
    with httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "br-acc/bracc-etl download_pgfn (httpx)"},
    ) as client:
        periods = _resolve_periods(client, args)
        if not periods:
            logger.error("No periods to fetch — aborting.")
            return 1
        logger.info(
            "Fetching %d period(s), buckets=%s, uf=%s, output=%s",
            len(periods), buckets, uf or "ALL", args.output_dir,
        )

        for period_dir in periods:
            remote_zips = _list_remote_zips(client, period_dir)
            for bucket in buckets:
                zip_name = BUCKETS[bucket]
                if zip_name not in remote_zips:
                    logger.warning(
                        "Bucket %s (%s) not published for %s — skipping",
                        bucket, zip_name, period_dir,
                    )
                    continue

                # Skip if we already have the extracted CSVs for this period.
                period_tag = period_dir.replace("_trimestre_", "Q")  # 2024Q04
                marker_prefix = {
                    "nao_previdenciario": "arquivo_lai_SIDA_",
                    "previdenciario": "arquivo_lai_PREV_",
                    "fgts": "arquivo_lai_FGTS_",
                }[bucket]
                # PGFN stamps the YYYYMM of the quarter's last month into the
                # filename (e.g. 202412 for 2024-Q4).
                year = int(period_dir[:4])
                q = int(period_dir[-2:])
                yyyymm = f"{year}{q * 3:02d}"
                existing = list(
                    args.output_dir.glob(f"{marker_prefix}*_{yyyymm}.csv"),
                )
                if args.skip_existing and existing:
                    logger.info(
                        "Skipping %s/%s: %d CSV(s) already on disk",
                        period_dir, bucket, len(existing),
                    )
                    written_any = True
                    continue

                url = f"{BASE_URL}/{period_dir}/{zip_name}"
                zip_path = zips_dir / f"{period_dir}__{zip_name}"
                if not _download(client, url, zip_path):
                    continue

                try:
                    written = _extract_with_uf_filter(
                        zip_path,
                        args.output_dir,
                        uf_filter=uf,
                        include_all=include_all_members,
                    )
                except (zipfile.BadZipFile, ValueError) as exc:
                    logger.error("Extraction failed for %s: %s", zip_path.name, exc)
                    zip_path.unlink(missing_ok=True)
                    continue

                if written:
                    written_any = True
                if not args.keep_zips:
                    zip_path.unlink(missing_ok=True)

    if not args.keep_zips:
        try:
            zips_dir.rmdir()
        except OSError:
            pass

    if not written_any:
        logger.error("No files written. Check logs above.")
        return 1
    logger.info("Done. Output under %s", args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

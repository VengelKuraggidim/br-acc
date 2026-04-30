from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader

logger = logging.getLogger(__name__)

# RAIS upstream FTP. ``RAIS_ESTAB_PUB.7z`` is the establishment-level
# file we aggregate from (~120 MB compressed → ~1 GB plaintext for
# recent years). The much larger ``RAIS_VINC_PUB_*.7z`` per-region files
# (vínculos) are not used — this pipeline emits sector reference data,
# not employment-record-level rows.
_RAIS_FTP_BASE = "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS"

# UF IBGE numeric code -> state abbreviation
UF_CODE_MAP = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
    "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
    "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
    "52": "GO", "53": "DF",
}


# ── fetch_to_disk: download RAIS_ESTAB_PUB.7z, extrair, agregar ──────


_RAIS_ESTAB_COLS = [
    "CNAE 2.0 Subclasse", "Qtd Vínculos Ativos",
    "Qtd Vínculos CLT", "Qtd Vínculos Estatutários", "UF",
]


def _aggregate_estab_txt(
    txt_path: Path, *, chunk_size: int = 50_000, limit: int | None = None,
) -> pd.DataFrame:
    """Aggregate the establishment-level RAIS .txt file by (CNAE, UF).

    Streamed in chunks so even multi-GB inputs don't blow up memory.
    Output schema matches what ``RaisPipeline._from_aggregated`` reads:
    ``cnae_subclass, uf, establishment_count, total_employees,
    total_clt, total_statutory, avg_employees``.
    """
    agg: dict[tuple[str, str], dict[str, int]] = {}
    rows_seen = 0

    chunks = pd.read_csv(
        txt_path,
        sep=";",
        encoding="latin-1",
        dtype=str,
        keep_default_na=False,
        usecols=_RAIS_ESTAB_COLS,
        chunksize=chunk_size,
    )

    for chunk in chunks:
        for _, row in chunk.iterrows():
            cnae = str(row["CNAE 2.0 Subclasse"]).strip()
            uf_code = str(row["UF"]).strip()
            if not cnae or cnae == "0":
                continue
            uf = UF_CODE_MAP.get(uf_code, uf_code)
            vinculos = int(str(row["Qtd Vínculos Ativos"]).strip() or "0")
            vinculos_clt = int(str(row["Qtd Vínculos CLT"]).strip() or "0")
            vinculos_estat = int(
                str(row["Qtd Vínculos Estatutários"]).strip() or "0",
            )
            key = (cnae, uf)
            if key not in agg:
                agg[key] = {
                    "establishment_count": 0,
                    "total_employees": 0,
                    "total_clt": 0,
                    "total_statutory": 0,
                }
            agg[key]["establishment_count"] += 1
            agg[key]["total_employees"] += vinculos
            agg[key]["total_clt"] += vinculos_clt
            agg[key]["total_statutory"] += vinculos_estat
            rows_seen += 1
            if limit is not None and rows_seen >= limit:
                break
        if limit is not None and rows_seen >= limit:
            break

    out_rows: list[dict[str, Any]] = []
    for (cnae, uf), v in agg.items():
        est = v["establishment_count"]
        out_rows.append({
            "cnae_subclass": cnae,
            "uf": uf,
            "establishment_count": est,
            "total_employees": v["total_employees"],
            "total_clt": v["total_clt"],
            "total_statutory": v["total_statutory"],
            "avg_employees": round(v["total_employees"] / est, 1) if est else 0,
        })

    logger.info(
        "[rais.fetch_to_disk] aggregated %d raw rows → %d (CNAE,UF) buckets",
        rows_seen, len(out_rows),
    )
    return pd.DataFrame(out_rows)


def fetch_to_disk(
    output_dir: Path | str,
    *,
    year: int,
    limit: int | None = None,
    skip_existing: bool = True,
    timeout: int = 600,
) -> list[Path]:
    """Download RAIS establishment microdata and write aggregated CSV.

    Pulls ``RAIS_ESTAB_PUB.7z`` from the PDET FTP for the given year,
    extracts the single ``.txt`` inside via the system ``7z`` binary
    (``apt install 7zip``), aggregates by CNAE subclass × UF in-process,
    and writes ``rais_<year>_aggregated.csv`` (the layout
    ``RaisPipeline._from_aggregated`` reads, with an extra ``year``
    column so multi-year corpora coexist in ``data/rais/``).

    Note: only ``RAIS_ESTAB_PUB`` (~120 MB compressed) is fetched. The
    much heavier ``RAIS_VINC_PUB_*`` (per-region employment record) is
    not — this pipeline emits sector-level reference data only.

    Args:
        output_dir: Destination directory (created if missing). Usually
            ``data/rais``.
        year: Reference year (required). Used both to locate the FTP
            directory and to tag the output filename + ``year`` column.
        limit: When set, truncate raw-row processing to the first N rows
            (smoke test).
        skip_existing: When True (default), reuse archives already on
            disk under ``output_dir/raw/`` instead of re-downloading.
        timeout: Per-file HTTP timeout in seconds.

    Returns:
        List with the single aggregated CSV path on success, empty on
        download failure.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    from _download_utils import (  # type: ignore[import-not-found]
        download_ftp_file,
        extract_7z_archive,
    )

    archive_name = "RAIS_ESTAB_PUB.7z"
    url = f"{_RAIS_FTP_BASE}/{year}/{archive_name}"
    archive_path = raw_dir / f"RAIS_ESTAB_PUB_{year}.7z"

    if skip_existing and archive_path.exists() and archive_path.stat().st_size > 0:
        logger.info("[rais.fetch_to_disk] skip existing %s", archive_path.name)
    else:
        logger.info("[rais.fetch_to_disk] downloading %s", url)
        if not download_ftp_file(url, archive_path, timeout=timeout):
            logger.warning(
                "[rais.fetch_to_disk] download failed for year %s — aborting",
                year,
            )
            return []

    with tempfile.TemporaryDirectory(prefix="rais_extract_") as tmp:
        tmp_dir = Path(tmp)
        extracted = extract_7z_archive(archive_path, tmp_dir)
        # PDET ships the establishment file as ``RAIS_ESTAB_PUB.txt``
        # for years where ``.txt`` is the canonical extension. Fall
        # back to "any extracted .txt" for vintages that drift.
        txt_files = [p for p in extracted if p.suffix.lower() == ".txt"]
        if not txt_files:
            logger.warning(
                "[rais.fetch_to_disk] no .txt found inside %s (got: %s)",
                archive_name, [p.name for p in extracted],
            )
            return []
        txt = txt_files[0]
        logger.info("[rais.fetch_to_disk] aggregating %s", txt.name)
        df = _aggregate_estab_txt(txt, limit=limit)

    df["year"] = year
    out_path = output_dir / f"rais_{year}_aggregated.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(
        "[rais.fetch_to_disk] wrote %s (%d rows)", out_path.name, len(df),
    )
    return [out_path.resolve()]


class RaisPipeline(Pipeline):
    """ETL pipeline for RAIS (Relacao Anual de Informacoes Sociais) labor data.

    RAIS public microdata is de-identified (no CNPJ/CPF). This pipeline
    aggregates establishment-level data by CNAE subclass + UF into
    LaborStats nodes. These are sector-level reference data (not entity-level),
    joined to Company nodes at query time via CNAE prefix matching.

    Data source: ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/
    """

    name = "rais"
    source_id = "rais_mte"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.labor_stats: list[dict[str, Any]] = []

    def extract(self) -> None:
        """Read RAIS establishment microdata and aggregate by CNAE + UF.

        If a pre-aggregated CSV exists, use it directly. Otherwise,
        aggregate from the raw .txt file.
        """
        rais_dir = Path(self.data_dir) / "rais"

        # Pre-aggregated CSVs: any ``rais_<year>_aggregated.csv`` works.
        # Sorted lexicographically so multi-year directories load in
        # ascending year order.
        agg_files = sorted(rais_dir.glob("rais_*_aggregated.csv"))
        if agg_files:
            for agg_path in agg_files:
                logger.info(
                    "Reading pre-aggregated RAIS data from %s", agg_path.name,
                )
                df = pd.read_csv(agg_path, dtype=str, keep_default_na=False)
                self._from_aggregated(df, source_path=agg_path)
            return

        # Otherwise aggregate from raw microdata
        raw_files = sorted(rais_dir.glob("RAIS_ESTAB_PUB*.txt*"))
        if not raw_files:
            logger.warning("No RAIS data files found in %s", rais_dir)
            return

        self._aggregate_raw(raw_files[0])

    @staticmethod
    def _year_from_aggregated_path(path: Path) -> int:
        """Extract YYYY from filename like ``rais_2024_aggregated.csv``."""
        stem = path.stem  # e.g. "rais_2024_aggregated"
        parts = stem.split("_")
        for tok in parts:
            if tok.isdigit() and len(tok) == 4:
                return int(tok)
        return 2022  # legacy fallback (matches original hard-coded value)

    def _from_aggregated(
        self, df: pd.DataFrame, *, source_path: Path | None = None,
    ) -> None:
        """Load from pre-aggregated CSV.

        Year resolution order: ``year`` column (per-row, when present) →
        filename token (``rais_<year>_aggregated.csv``) → 2022 (legacy
        fallback that matches the original hard-coded behaviour).
        """
        fallback_year = (
            self._year_from_aggregated_path(source_path)
            if source_path is not None
            else 2022
        )
        rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            cnae = str(row.get("cnae_subclass", "")).strip()
            uf = str(row.get("uf", "")).strip()
            if not cnae or not uf:
                continue
            row_year_raw = str(row.get("year", "")).strip()
            year = int(row_year_raw) if row_year_raw.isdigit() else fallback_year
            rows.append({
                "stats_id": f"rais_{year}_{cnae}_{uf}",
                "cnae_subclass": cnae,
                "uf": uf,
                "year": year,
                "establishment_count": int(row.get("establishment_count", 0)),
                "total_employees": int(row.get("total_employees", 0)),
                "total_clt": int(row.get("total_clt", 0)),
                "total_statutory": int(row.get("total_statutory", 0)),
                "avg_employees": float(row.get("avg_employees", 0)),
                "source": "rais_mte",
            })
        self.labor_stats.extend(rows)
        logger.info("Loaded %d aggregated RAIS records (year=%s)", len(rows), fallback_year)

    def _aggregate_raw(self, raw_path: Path) -> None:
        """Aggregate raw RAIS microdata file by CNAE + UF."""
        logger.info("Aggregating raw RAIS data from %s", raw_path.name)

        agg: dict[tuple[str, str], dict[str, Any]] = {}
        total_rows = 0

        chunks = pd.read_csv(
            raw_path,
            sep=";",
            encoding="latin-1",
            dtype=str,
            keep_default_na=False,
            usecols=[
                "CNAE 2.0 Subclasse", "Qtd Vínculos Ativos",
                "Qtd Vínculos CLT", "Qtd Vínculos Estatutários", "UF",
            ],
            chunksize=self.chunk_size,
        )

        for chunk in chunks:
            total_rows += len(chunk)
            for _, row in chunk.iterrows():
                cnae = str(row["CNAE 2.0 Subclasse"]).strip()
                uf_code = str(row["UF"]).strip()
                if not cnae or cnae == "0":
                    continue
                uf = UF_CODE_MAP.get(uf_code, uf_code)
                vinculos = int(str(row["Qtd Vínculos Ativos"]).strip() or "0")
                vinculos_clt = int(str(row["Qtd Vínculos CLT"]).strip() or "0")
                vinculos_estat = int(
                    str(row["Qtd Vínculos Estatutários"]).strip() or "0"
                )
                key = (cnae, uf)
                if key not in agg:
                    agg[key] = {
                        "cnae_subclass": cnae,
                        "uf": uf,
                        "establishment_count": 0,
                        "total_employees": 0,
                        "total_clt": 0,
                        "total_statutory": 0,
                    }
                agg[key]["establishment_count"] += 1
                agg[key]["total_employees"] += vinculos
                agg[key]["total_clt"] += vinculos_clt
                agg[key]["total_statutory"] += vinculos_estat
            logger.info("  Processed %d rows", total_rows)

        rows: list[dict[str, Any]] = []
        for v in agg.values():
            est_count = v["establishment_count"]
            rows.append({
                "stats_id": f"rais_2022_{v['cnae_subclass']}_{v['uf']}",
                "cnae_subclass": v["cnae_subclass"],
                "uf": v["uf"],
                "year": 2022,
                "establishment_count": est_count,
                "total_employees": v["total_employees"],
                "total_clt": v["total_clt"],
                "total_statutory": v["total_statutory"],
                "avg_employees": round(v["total_employees"] / est_count, 1)
                if est_count > 0
                else 0,
                "source": "rais_mte",
            })

        self.labor_stats = rows
        logger.info(
            "Aggregated %d rows into %d CNAE+UF stats from %d raw records",
            total_rows, len(rows), total_rows,
        )

    def transform(self) -> None:
        """No additional transform needed — aggregation is done in extract."""

    def load(self) -> None:
        """Load LaborStats nodes (sector reference data, no relationships)."""
        loader = Neo4jBatchLoader(self.driver)

        if not self.labor_stats:
            logger.warning("No RAIS labor stats to load")
            return

        # Load LaborStats nodes
        logger.info("Loading %d LaborStats nodes...", len(self.labor_stats))
        loader.load_nodes("LaborStats", self.labor_stats, key_field="stats_id")

        # Create index for efficient matching
        with self.driver.session(database=self.neo4j_database) as session:
            session.run(
                "CREATE INDEX labor_stats_cnae IF NOT EXISTS "
                "FOR (l:LaborStats) ON (l.cnae_subclass)"
            )
            session.run(
                "CREATE INDEX labor_stats_uf IF NOT EXISTS "
                "FOR (l:LaborStats) ON (l.uf)"
            )

        logger.info("LaborStats nodes loaded. Indexes created.")
        logger.info(
            "Total: %d stats covering %d establishments, %d employees",
            len(self.labor_stats),
            sum(s["establishment_count"] for s in self.labor_stats),
            sum(s["total_employees"] for s in self.labor_stats),
        )

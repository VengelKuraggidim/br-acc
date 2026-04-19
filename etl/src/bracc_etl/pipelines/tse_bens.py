from __future__ import annotations

import hashlib
import logging
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    parse_numeric_comma,
    strip_document,
)

logger = logging.getLogger(__name__)


def _make_asset_id(cpf: str, year: str, asset_type: str, value: str, description: str) -> str:
    """Generate deterministic asset_id from key fields."""
    payload = f"{cpf}|{year}|{asset_type}|{value}|{description}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class TseBensPipeline(Pipeline):
    """ETL pipeline for TSE Bens Declarados (candidate declared assets)."""

    name = "tse_bens"
    source_id = "tse_bens"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.assets: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        bens_dir = Path(self.data_dir) / "tse_bens"
        csv_path = bens_dir / "bens.csv"
        if not csv_path.exists():
            msg = f"Data file not found: {csv_path}"
            raise FileNotFoundError(msg)

        self._raw = pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,
        )
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)
        logger.info("[tse_bens] Extracted %d rows", len(self._raw))

    def transform(self) -> None:
        assets: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []

        for _idx, row in self._raw.iterrows():
            cpf_raw = str(row.get("cpf", ""))
            digits = strip_document(cpf_raw)

            if len(digits) != 11:
                continue

            cpf_formatted = format_cpf(cpf_raw)
            nome = normalize_name(str(row.get("nome_candidato", "")))
            year = str(row.get("ano", "")).strip()
            asset_type = str(row.get("tipo_bem", "")).strip()
            description = str(row.get("descricao_bem", "")).strip()
            value_raw = str(row.get("valor_bem", ""))
            value = parse_numeric_comma(value_raw)
            uf = str(row.get("sigla_uf", "")).strip()
            partido = str(row.get("sigla_partido", "")).strip()

            asset_id = _make_asset_id(digits, year, asset_type, value_raw.strip(), description)

            assets.append(self.attach_provenance(
                {
                    "asset_id": asset_id,
                    "candidate_cpf": cpf_formatted,
                    "candidate_name": nome,
                    "asset_type": asset_type,
                    "asset_description": description,
                    "asset_value": value,
                    "election_year": int(year) if year.isdigit() else 0,
                    "uf": uf,
                    "partido": partido,
                    "source": "tse_bens",
                },
                record_id=asset_id,
            ))

            person_rels.append(self.attach_provenance(
                {
                    "source_key": cpf_formatted,
                    "target_key": asset_id,
                    "person_name": nome,
                },
                record_id=asset_id,
            ))

        self.assets = deduplicate_rows(assets, ["asset_id"])
        self.person_rels = person_rels
        logger.info(
            "[tse_bens] Transformed: %d assets, %d person rels",
            len(self.assets),
            len(self.person_rels),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.assets:
            loader.load_nodes("DeclaredAsset", self.assets, key_field="asset_id")

        # Ensure Person nodes exist for each candidate
        persons_seen: set[str] = set()
        unique_persons: list[dict[str, Any]] = []
        for rel in self.person_rels:
            cpf = rel["source_key"]
            if cpf not in persons_seen:
                persons_seen.add(cpf)
                unique_persons.append(self.attach_provenance(
                    {"cpf": cpf, "name": rel["person_name"]},
                    record_id=cpf,
                ))
        if unique_persons:
            loader.load_nodes("Person", unique_persons, key_field="cpf")

        if self.person_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (a:DeclaredAsset {asset_id: row.target_key}) "
                "MERGE (p)-[r:DECLAROU_BEM]->(a) "
                "SET r.source_id = row.source_id, "
                "    r.source_record_id = row.source_record_id, "
                "    r.source_url = row.source_url, "
                "    r.ingested_at = row.ingested_at, "
                "    r.run_id = row.run_id"
            )
            loader.run_query_with_retry(query, self.person_rels)

        self.rows_loaded = len(self.assets)
        logger.info(
            "[tse_bens] Loaded: %d assets, %d persons, %d rels",
            len(self.assets),
            len(persons_seen),
            len(self.person_rels),
        )


# ────────────────────────────────────────────────────────────────────
# Acquisition helper — UF-scoped CDN download for Fiscal Cidadao
# ────────────────────────────────────────────────────────────────────
#
# Source: TSE public CDN ``bem_candidato_<year>.zip`` (per-state CSVs).
# Earliest year available on the CDN is 2006; 1998/2002 are not published there.
# The raw ``bem_candidato`` CSV carries ``SQ_CANDIDATO`` but no CPF/name/partido,
# so we join with ``consulta_cand_<year>_<UF>.csv`` (from the ``consulta_cand``
# ZIPs, already used by the tse pipeline) to populate the columns
# ``TseBensPipeline.extract`` expects: cpf, nome_candidato, ano, sigla_uf,
# sigla_partido, tipo_bem, descricao_bem, valor_bem.

_TSE_CDN_BENS = "https://cdn.tse.jus.br/estatistica/sead/odsele"


def _bens_download_zip(url: str, dest: Path, *, timeout: float = 600.0) -> Path | None:
    import httpx

    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=timeout) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=65_536):
                    fh.write(chunk)
    except httpx.HTTPError as exc:
        logger.warning("[tse_bens] HTTP error %s: %s", url, exc)
        dest.unlink(missing_ok=True)
        return None

    if not zipfile.is_zipfile(dest):
        logger.warning("[tse_bens] %s did not return a valid ZIP", url)
        dest.unlink(missing_ok=True)
        return None
    return dest


def _extract_uf_csv(zip_path: Path, extract_dir: Path, *, prefix: str, uf: str) -> Path | None:
    """Extract the single ``<prefix>_<year>_<UF>.csv`` member from a TSE ZIP."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    uf_suffix = f"_{uf.lower()}.csv"
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            name = Path(info.filename).name.lower()
            if name.startswith(prefix) and name.endswith(uf_suffix):
                zf.extract(info, extract_dir)
                return extract_dir / info.filename
    return None


def _read_tse_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path, sep=";", encoding="latin-1", dtype=str, keep_default_na=False,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[tse_bens] skip %s: %s", path.name, exc)
        return pd.DataFrame()


def fetch_to_disk(
    output_dir: Path,
    *,
    uf: str = "GO",
    years: list[int] | None = None,
    timeout: float = 600.0,
    skip_existing: bool = True,
) -> list[Path]:
    """Download TSE declared-assets filtered to one UF, joined with candidates.

    For each requested year we pull ``bem_candidato_<year>.zip`` (the asset
    rows) and ``consulta_cand_<year>.zip`` (to resolve CPF, name, partido from
    SQ_CANDIDATO). Only the per-UF CSV from each ZIP is extracted.

    Years default to the subset of Marconi-era elections where TSE publishes
    bens on the CDN (2006+ — 1998/2002 are not available on the CDN).
    """
    uf_upper = uf.upper()
    years = years or [2006, 2010, 2014, 2018, 2022]
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    all_frames: list[pd.DataFrame] = []
    for year in years:
        # 1) Download bens ZIP for year.
        bens_zip = raw_dir / f"bem_candidato_{year}.zip"
        if not (skip_existing and bens_zip.exists() and zipfile.is_zipfile(bens_zip)):
            url = f"{_TSE_CDN_BENS}/bem_candidato/bem_candidato_{year}.zip"
            logger.info("[tse_bens] downloading %s", url)
            if _bens_download_zip(url, bens_zip, timeout=timeout) is None:
                continue

        bens_csv = _extract_uf_csv(
            bens_zip, raw_dir / f"bem_candidato_{year}_extracted",
            prefix=f"bem_candidato_{year}", uf=uf_upper,
        )
        if bens_csv is None:
            logger.warning("[tse_bens] no %s CSV inside %s", uf_upper, bens_zip.name)
            continue

        # 2) Download candidates ZIP and extract the same UF slice, for JOIN.
        cand_zip = raw_dir / f"consulta_cand_{year}.zip"
        if not (skip_existing and cand_zip.exists() and zipfile.is_zipfile(cand_zip)):
            url = f"{_TSE_CDN_BENS}/consulta_cand/consulta_cand_{year}.zip"
            logger.info("[tse_bens] downloading %s", url)
            if _bens_download_zip(url, cand_zip, timeout=timeout) is None:
                continue

        cand_csv = _extract_uf_csv(
            cand_zip, raw_dir / f"consulta_cand_{year}_extracted",
            prefix=f"consulta_cand_{year}", uf=uf_upper,
        )
        if cand_csv is None:
            logger.warning("[tse_bens] no %s candidate CSV for year %d", uf_upper, year)
            continue

        bens_df = _read_tse_csv(bens_csv)
        cand_df = _read_tse_csv(cand_csv)
        if bens_df.empty or cand_df.empty:
            continue

        # Deduplicate candidate rows on SQ_CANDIDATO (one row per candidacy).
        cand_small = cand_df[
            [c for c in ("SQ_CANDIDATO", "NR_CPF_CANDIDATO", "NM_CANDIDATO", "SG_PARTIDO")
             if c in cand_df.columns]
        ].drop_duplicates(subset=["SQ_CANDIDATO"])

        merged = bens_df.merge(cand_small, on="SQ_CANDIDATO", how="left")

        out_df = pd.DataFrame({
            "cpf": merged.get("NR_CPF_CANDIDATO", ""),
            "nome_candidato": merged.get("NM_CANDIDATO", ""),
            "ano": merged.get("ANO_ELEICAO", str(year)),
            "sigla_uf": merged.get("SG_UF", uf_upper),
            "sigla_partido": merged.get("SG_PARTIDO", ""),
            "tipo_bem": merged.get("DS_TIPO_BEM_CANDIDATO", ""),
            "descricao_bem": merged.get("DS_BEM_CANDIDATO", ""),
            "valor_bem": merged.get("VR_BEM_CANDIDATO", ""),
        })
        # Hard filter to requested UF (defensive — per-state files should already).
        out_df = out_df[out_df["sigla_uf"].str.upper() == uf_upper]
        all_frames.append(out_df)
        logger.info("[tse_bens] year=%d uf=%s rows=%d", year, uf_upper, len(out_df))

    if not all_frames:
        logger.warning("[tse_bens] no data collected for uf=%s years=%s", uf_upper, years)
        return []

    combined = pd.concat(all_frames, ignore_index=True)
    out_path = output_dir / "bens.csv"
    combined.to_csv(out_path, index=False, encoding="utf-8")
    logger.info("[tse_bens] wrote %d rows → %s", len(combined), out_path)
    return [out_path]

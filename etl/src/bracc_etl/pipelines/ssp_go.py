"""ETL pipeline scaffold for Goias public-security statistics.

SSP-GO (Secretaria de Seguranca Publica de Goias, via goias.gov.br/seguranca)
publishes aggregate monthly/yearly statistics. This scaffold accepts
pre-downloaded CSV files under ``data/ssp_go/`` with the expected shape:

- ``ocorrencias.csv``  -> GoSecurityStat nodes (aggregate counts by
                          municipality / crime type / period)

Human validation required:

1. Confirm whether SSP-GO publishes a machine-readable endpoint or whether
   only PDF bulletins are available (scraping may be required).
2. Decide on the canonical crime-type taxonomy (SSP's own categories vs.
   a unified set used across Brazilian state security agencies).
3. Validate CSV schema once an operator exports a sample.

Data source: https://goias.gov.br/seguranca/
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
    row_pick,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


class SspGoPipeline(Pipeline):
    """Scaffold pipeline for Goias public-security aggregate statistics."""

    name = "ssp_go"
    source_id = "ssp_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_stats: pd.DataFrame = pd.DataFrame()
        self.stats: list[dict[str, Any]] = []

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        for sep in (";", ","):
            try:
                df = pd.read_csv(
                    path, sep=sep, dtype=str, keep_default_na=False,
                    encoding="utf-8", engine="python", on_bad_lines="skip",
                )
                if len(df.columns) > 1:
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        try:
            return pd.read_csv(
                path, sep=";", dtype=str, keep_default_na=False,
                encoding="latin-1", engine="python", on_bad_lines="skip",
            )
        except (OSError, pd.errors.ParserError) as exc:
            logger.warning("[ssp_go] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "ssp_go"
        if not src_dir.exists():
            logger.warning(
                "[ssp_go] expected directory %s missing; "
                "export SSP-GO aggregate CSVs there.",
                src_dir,
            )
            return
        self._raw_stats = self._read_csv_optional(src_dir / "ocorrencias.csv")
        if self.limit:
            self._raw_stats = self._raw_stats.head(self.limit)
        self.rows_in = len(self._raw_stats)

    def transform(self) -> None:
        for _, row in self._raw_stats.iterrows():
            municipio = normalize_name(
                row_pick(row, "municipio", "nome_municipio", "cidade"),
            )
            cod_ibge = row_pick(row, "cod_ibge", "codigo_ibge", "ibge")
            crime_type = normalize_name(
                row_pick(
                    row, "natureza", "tipo_ocorrencia", "crime", "classificacao",
                ),
            )
            periodo = row_pick(row, "periodo", "mes_ano", "data", "ano")
            count_raw = row_pick(row, "quantidade", "total", "count", "ocorrencias")
            try:
                count = int(float(str(count_raw).replace(",", ".")))
            except (TypeError, ValueError):
                count = 0
            if not municipio and not cod_ibge:
                continue
            stat_id = _hash_id(cod_ibge, municipio, crime_type, periodo)
            self.stats.append({
                "stat_id": stat_id,
                "cod_ibge": cod_ibge,
                "municipality": municipio,
                "crime_type": crime_type,
                "period": periodo,
                "count": count,
                "uf": "GO",
                "source": "ssp_go",
            })

        self.stats = deduplicate_rows(self.stats, ["stat_id"])
        self.rows_loaded = len(self.stats)

    def load(self) -> None:
        if not self.stats:
            logger.warning("[ssp_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes("GoSecurityStat", self.stats, key_field="stat_id")

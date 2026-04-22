from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import deduplicate_rows, normalize_name, parse_numeric_comma

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Column mapping: original CSV header -> safe attribute name
_COL_RENAME = {
    "OB": "ob",
    "Data": "data",
    "Ano": "ano",
    "Mês": "mes",
    "Nome Emenda": "nome_emenda",
    "Transferência Especial": "transferencia_especial",
    "Categoria Econômica Despesa": "categoria_economica",
    "Valor": "valor",
    "CNPJ do Favorecido": "cnpj_favorecido",
    "Nome Favorecido": "nome_favorecido",
}

# Tesouro Transparente CKAN resource for "emendas-parlamentares".
# Dataset: https://www.tesourotransparente.gov.br/ckan/dataset/emendas-parlamentares
# Resource: emendas-parlamentares.csv (Latin-1, semicolon-separated, ~60MB,
# national scope — columns include UF and Ano that we use to subset at
# download time so downstream ``TesouroEmendasPipeline.extract`` consumes
# a GO-only ``emendas_tesouro.csv``).
_CKAN_CSV_URL = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "83e419da-1552-46bf-bfc3-05160b2c46c9/resource/"
    "66d69917-a5d8-4500-b4b2-ef1f5d062430/download/"
    "emendas-parlamentares.csv"
)
_HTTP_TIMEOUT = 120.0
# Stream + filter in pandas chunks so we never hold the full national CSV
# (~60MB but can grow) in memory — and so we can cheaply apply
# ``uf`` / ``years`` subsetting row-by-row.
_READ_CHUNK_SIZE = 50_000


def fetch_to_disk(
    output_dir: Path | str,
    uf: str | None = "GO",
    years: list[int] | list[str] | None = None,
    url: str = _CKAN_CSV_URL,
    timeout: float = _HTTP_TIMEOUT,
) -> list[Path]:
    """Download the Tesouro Transparente emendas CSV and filter by UF/years.

    Writes a single ``emendas_tesouro.csv`` under ``output_dir`` using the
    same Latin-1/semicolon encoding the Tesouro publishes, so that
    ``TesouroEmendasPipeline.extract`` reads it without any further
    transformation. Also persists the raw national file as
    ``emendas_parlamentares_raw.csv`` so re-filtering different UFs or
    year windows doesn't require re-downloading ~60MB.

    Args:
        output_dir: Directory to write outputs into. Created if missing.
            Defaults to the pipeline's expected path
            (``data/tesouro_emendas/``).
        uf: UF code to retain (default ``"GO"``). Pass ``None`` (or an
            empty string) to keep all UFs — the national dump.
        years: Optional list of years (``int`` or string) to keep. ``None``
            keeps every year present in the national dump.
        url: Override for the CKAN resource URL (kept public for tests and
            future URL rotations; defaults to the production resource).
        timeout: Per-request HTTP timeout in seconds.

    Returns:
        List of ``Path`` objects for files written (sorted).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "emendas_parlamentares_raw.csv"
    out_path = output_dir / "emendas_tesouro.csv"

    uf_norm = (uf or "").strip().upper() or None
    years_norm: set[str] | None = (
        {str(y).strip() for y in years if str(y).strip()}
        if years
        else None
    )

    logger.info(
        "[tesouro_emendas] Downloading %s (uf=%s, years=%s) -> %s",
        url, uf_norm or "ALL", sorted(years_norm) if years_norm else "ALL",
        out_path,
    )

    # Stream download to ``raw_path`` so we never buffer the full body.
    # ``verify=False`` mirrors the legacy etl/scripts downloader, which
    # was tolerant of the Tesouro's cert chain quirks.
    with (
        httpx.Client(
            timeout=timeout, verify=False, follow_redirects=True,
        ) as client,
        raw_path.open("wb") as fh,
        client.stream("GET", url) as resp,
    ):
        resp.raise_for_status()
        total = 0
        for chunk in resp.iter_bytes(chunk_size=1 << 16):
            if chunk:
                fh.write(chunk)
                total += len(chunk)
    logger.info(
        "[tesouro_emendas] Raw CSV written: %s (%.2f MB)",
        raw_path, raw_path.stat().st_size / 1024 / 1024,
    )

    # Iterate the raw CSV in chunks, keep only rows matching the requested
    # UF/years, and re-serialise with the *pipeline's* expected column
    # subset (so no extra rename step is needed downstream).
    kept_total = 0
    scanned_total = 0
    header_written = False

    pipeline_columns = list(_COL_RENAME.keys())

    with pd.read_csv(
        raw_path,
        dtype=str,
        encoding="latin-1",
        sep=";",
        keep_default_na=False,
        chunksize=_READ_CHUNK_SIZE,
    ) as reader, out_path.open("w", encoding="latin-1", newline="") as out_fh:
        for chunk in reader:
            scanned_total += len(chunk)

            mask = pd.Series(True, index=chunk.index)
            if uf_norm and "UF" in chunk.columns:
                mask &= chunk["UF"].str.upper().str.strip() == uf_norm
            if years_norm and "Ano" in chunk.columns:
                mask &= chunk["Ano"].astype(str).str.strip().isin(years_norm)

            subset = chunk.loc[mask, :]
            if subset.empty:
                continue

            # Keep only the columns the pipeline actually consumes, in the
            # same order and with the same headers. Any missing upstream
            # columns (future schema drift) default to empty strings so the
            # pipeline's ``dtype=str`` read is preserved.
            projected = pd.DataFrame(
                {col: subset.get(col, "") for col in pipeline_columns},
            )

            projected.to_csv(
                out_fh,
                sep=";",
                index=False,
                header=not header_written,
                lineterminator="\r\n",
            )
            header_written = True
            kept_total += len(projected)

    if not header_written:
        # No rows matched: still leave an empty CSV with the right header so
        # contract checks for ``data/tesouro_emendas/*`` pass and downstream
        # extract() can at least read a valid but empty file.
        header_line = ";".join(pipeline_columns) + "\r\n"
        out_path.write_bytes(header_line.encode("latin-1"))
        logger.warning(
            "[tesouro_emendas] 0 rows matched uf=%s years=%s (scanned %d). "
            "Wrote header-only CSV: %s",
            uf_norm or "ALL",
            sorted(years_norm) if years_norm else "ALL",
            scanned_total,
            out_path,
        )
    else:
        logger.info(
            "[tesouro_emendas] Kept %d / %d rows (uf=%s years=%s) -> %s",
            kept_total, scanned_total,
            uf_norm or "ALL",
            sorted(years_norm) if years_norm else "ALL",
            out_path,
        )

    return sorted([raw_path, out_path])


def _parse_excel_date(date_val: str) -> str:
    """Convert Excel serial date (e.g. 42005) to ISO format."""
    if date_val.isdigit():
        with contextlib.suppress(Exception):
            dt = pd.to_datetime(
                int(date_val), unit="D", origin="1899-12-30"
            )
            return dt.strftime("%Y-%m-%d")
    return date_val


class TesouroEmendasPipeline(Pipeline):
    """ETL pipeline for Tesouro Emendas."""

    name = "tesouro_emendas"
    source_id = "tesouro_emendas"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver, data_dir, limit=limit,
            chunk_size=chunk_size, **kwargs,
        )
        self._raw = pd.DataFrame()
        self.transfers: list[dict[str, Any]] = []
        self.companies: list[dict[str, Any]] = []
        self.transfer_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "tesouro_emendas"
        csv_path = src_dir / "emendas_tesouro.csv"
        if not csv_path.exists():
            msg = f"Tesouro Emendas CSV not found: {csv_path}"
            raise FileNotFoundError(msg)

        self._raw = pd.read_csv(
            csv_path,
            dtype=str,
            encoding="latin-1",
            sep=";",
            keep_default_na=False,
        )
        self.rows_in = len(self._raw)
        logger.info(
            "[tesouro_emendas] Extracted %d records", len(self._raw),
        )

    def transform(self) -> None:
        # Rename columns so itertuples() produces valid attributes
        df = self._raw.rename(columns=_COL_RENAME)

        transfers: list[dict[str, Any]] = []
        companies: list[dict[str, Any]] = []
        transfer_rels: list[dict[str, Any]] = []

        for row in df.itertuples(index=False):
            ob = str(getattr(row, "ob", "")).strip()
            if not ob:
                continue

            date_val = str(getattr(row, "data", "")).strip()
            formatted_date = _parse_excel_date(date_val)

            transfer_id = f"transfer_tesouro_{ob}"
            transfers.append({
                "transfer_id": transfer_id,
                "ob": ob,
                "date": formatted_date,
                "year": str(getattr(row, "ano", "")).strip(),
                "month": str(getattr(row, "mes", "")).strip(),
                "amendment_type": str(
                    getattr(row, "nome_emenda", "")
                ).strip(),
                "special_transfer": str(
                    getattr(row, "transferencia_especial", "")
                ).strip(),
                "economic_category": str(
                    getattr(row, "categoria_economica", "")
                ).strip(),
                "value": parse_numeric_comma(getattr(row, "valor", "")),
                "source": self.source_id,
            })

            cnpj_raw = str(
                getattr(row, "cnpj_favorecido", "")
            ).strip()
            nome_fav = normalize_name(
                str(getattr(row, "nome_favorecido", ""))
            )

            cnpj = cnpj_raw.zfill(14) if cnpj_raw else ""
            if len(cnpj) == 14:
                companies.append({
                    "cnpj": cnpj,
                    "razao_social": nome_fav,
                })
                transfer_rels.append({
                    "source_key": transfer_id,
                    "target_key": cnpj,
                })

            if self.limit and len(transfers) >= self.limit:
                break

        self.transfers = deduplicate_rows(transfers, ["transfer_id"])
        self.companies = deduplicate_rows(companies, ["cnpj"])
        self.transfer_rels = transfer_rels

        logger.info(
            "[tesouro_emendas] Transformed %d transfers, %d companies",
            len(self.transfers),
            len(self.companies),
        )

    def _stamp(self, row: dict[str, Any], *, record_id: object) -> dict[str, Any]:
        """Shorthand pra ``attach_provenance`` com URL canonica da emendas CSV
        do Tesouro Transparente.

        CKAN nao expoe deep-link por OB/registro — usamos a landing do dataset.
        """
        return self.attach_provenance(
            row,
            record_id=record_id,
            record_url=(
                "https://www.tesourotransparente.gov.br/ckan/dataset/"
                "emendas-parlamentares"
            ),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.transfers:
            transfer_rows = [
                self._stamp(t, record_id=t["transfer_id"])
                for t in self.transfers
            ]
            loaded = loader.load_nodes(
                "Payment", transfer_rows, key_field="transfer_id",
            )
            self.rows_loaded += loaded

        if self.companies:
            company_rows = [
                self._stamp(c, record_id=c["cnpj"]) for c in self.companies
            ]
            loader.load_nodes(
                "Company", company_rows, key_field="cnpj",
            )

        if self.transfer_rels:
            rel_rows = [
                self._stamp(r, record_id=f"{r['source_key']}->{r['target_key']}")
                for r in self.transfer_rels
            ]
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Payment {transfer_id: row.source_key}) "
                "MATCH (c:Company {cnpj: row.target_key}) "
                "MERGE (p)-[r:PAGO_PARA]->(c) "
                "SET r.source_id = row.source_id, "
                "    r.source_record_id = row.source_record_id, "
                "    r.source_url = row.source_url, "
                "    r.ingested_at = row.ingested_at, "
                "    r.run_id = row.run_id"
            )
            loader.run_query(query, rel_rows)

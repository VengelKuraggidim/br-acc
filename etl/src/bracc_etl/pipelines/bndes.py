from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_brl_amount,
    strip_document,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Module-level download (script_download mode).
# --------------------------------------------------------------------------
#
# BNDES publishes an open-data CKAN catalogue at
# https://dadosabertos.bndes.gov.br/. The pipeline consumes the
# "Operações não automáticas" CSV resource of the
# "Operações de Financiamento" package, which is served as a single
# semicolon-separated, latin-1-encoded national dump (~20 MB at the
# time of writing). All 14 columns ``BndesPipeline.extract`` reads are
# present upstream with the exact same names, so ``fetch_to_disk``
# saves the file verbatim under ``operacoes-nao-automaticas.csv`` —
# the filename ``BndesPipeline.extract`` looks for.
#
# UF filtering is applied opt-in at download time so a GO-only
# bootstrap doesn't have to keep ~250k national rows on disk.

_BNDES_CKAN_PACKAGE = "operacoes-financiamento"
# Stable resource id for the "Operações não automáticas" CSV. The
# direct download URL keeps working even when the dataset's metadata
# layout changes — the resource id is the contract.
_BNDES_NAO_AUTOMATICAS_URL = (
    "https://dadosabertos.bndes.gov.br/dataset/"
    "10e21ad1-568e-45e5-a8af-43f2c05ef1a2/resource/"
    "6f56b78c-510f-44b6-8274-78a5b7e931f4/download/"
    "operacoes-financiamento-operacoes-nao-automaticas.csv"
)
_BNDES_OUTPUT_FILENAME = "operacoes-nao-automaticas.csv"
_BNDES_HTTP_TIMEOUT = 120.0
_BNDES_READ_CHUNK_SIZE = 50_000


def fetch_to_disk(
    output_dir: Path | str,
    *,
    date: str | None = None,
    uf: str | None = None,
    limit: int | None = None,
    url: str = _BNDES_NAO_AUTOMATICAS_URL,
    timeout: float = _BNDES_HTTP_TIMEOUT,
) -> list[Path]:
    """Download the BNDES "Operações não automáticas" CSV.

    Writes ``operacoes-nao-automaticas.csv`` under ``output_dir``
    using the same latin-1/semicolon dialect ``BndesPipeline.extract``
    expects. The 14 columns the pipeline needs are present in the
    upstream feed unchanged, so no schema remap is required.

    Args:
        output_dir: Destination directory (created if missing).
        date: Accepted for API symmetry with other ``fetch_to_disk``
            callers; the BNDES dump is a rolling consolidated file
            without a date-snapshot endpoint, so this is informational
            only and logged.
        uf: Optional UF code (two-letter). When set, keeps only rows
            whose ``uf`` column matches. Default ``None`` keeps the
            full national dump.
        limit: If set, truncate to the first N matching rows after
            UF filtering. Useful for smoke tests.
        url: Override the upstream URL (kept public for tests).
        timeout: Per-request HTTP timeout in seconds.

    Returns:
        List of absolute ``Path`` objects for files written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / _BNDES_OUTPUT_FILENAME

    if date is not None:
        logger.info(
            "[bndes.fetch_to_disk] --date=%s ignored (BNDES open-data "
            "publishes a rolling consolidated CSV without a date filter)",
            date,
        )

    uf_norm = (uf or "").strip().upper() or None

    logger.info(
        "[bndes.fetch_to_disk] Downloading %s (uf=%s, limit=%s) -> %s",
        url, uf_norm or "ALL", limit if limit is not None else "ALL", out_path,
    )

    # Fast path: when no UF/limit filter, stream straight to disk.
    if uf_norm is None and limit is None:
        with (
            httpx.Client(
                timeout=timeout, verify=False, follow_redirects=True,
                headers={"User-Agent": "br-acc/bracc-etl download_bndes (httpx)"},
            ) as client,
            out_path.open("wb") as fh,
            client.stream("GET", url) as resp,
        ):
            resp.raise_for_status()
            total = 0
            for chunk in resp.iter_bytes(chunk_size=1 << 16):
                if chunk:
                    fh.write(chunk)
                    total += len(chunk)
        logger.info(
            "[bndes.fetch_to_disk] Wrote %s (%.2f MB)",
            out_path, out_path.stat().st_size / 1024 / 1024,
        )
        return [out_path.resolve()]

    # Filter path: download to a temp file, then chunk-filter.
    raw_path = output_dir / "operacoes-nao-automaticas-raw.csv"
    with (
        httpx.Client(
            timeout=timeout, verify=False, follow_redirects=True,
            headers={"User-Agent": "br-acc/bracc-etl download_bndes (httpx)"},
        ) as client,
        raw_path.open("wb") as fh,
        client.stream("GET", url) as resp,
    ):
        resp.raise_for_status()
        for chunk in resp.iter_bytes(chunk_size=1 << 16):
            if chunk:
                fh.write(chunk)
    logger.info(
        "[bndes.fetch_to_disk] Raw CSV written: %s (%.2f MB)",
        raw_path, raw_path.stat().st_size / 1024 / 1024,
    )

    kept_total = 0
    scanned_total = 0
    header_written = False
    with pd.read_csv(
        raw_path,
        dtype=str,
        encoding="latin-1",
        sep=";",
        keep_default_na=False,
        chunksize=_BNDES_READ_CHUNK_SIZE,
    ) as reader, out_path.open("w", encoding="latin-1", newline="") as out_fh:
        for chunk in reader:
            scanned_total += len(chunk)
            mask = pd.Series(True, index=chunk.index)
            if uf_norm and "uf" in chunk.columns:
                mask &= chunk["uf"].str.upper().str.strip() == uf_norm
            subset = chunk.loc[mask, :]
            if limit is not None:
                remaining = max(0, limit - kept_total)
                if remaining == 0:
                    break
                if len(subset) > remaining:
                    subset = subset.iloc[:remaining]
            if subset.empty:
                continue
            subset.to_csv(
                out_fh,
                sep=";",
                index=False,
                header=not header_written,
                lineterminator="\r\n",
            )
            header_written = True
            kept_total += len(subset)

    if not header_written:
        # Preserve schema for downstream contract checks even when 0 rows.
        empty_df = pd.read_csv(
            raw_path, dtype=str, encoding="latin-1", sep=";",
            keep_default_na=False, nrows=0,
        )
        cols = list(empty_df.columns)
        out_path.write_bytes((";".join(cols) + "\r\n").encode("latin-1"))
        logger.warning(
            "[bndes.fetch_to_disk] 0 rows matched uf=%s (scanned %d). "
            "Wrote header-only CSV: %s", uf_norm, scanned_total, out_path,
        )
    else:
        logger.info(
            "[bndes.fetch_to_disk] Kept %d / %d rows (uf=%s) -> %s",
            kept_total, scanned_total, uf_norm or "ALL", out_path,
        )

    return sorted([raw_path.resolve(), out_path.resolve()])


class BndesPipeline(Pipeline):
    """ETL pipeline for BNDES financing operations (non-automatic/direct)."""

    name = "bndes"
    source_id = "bndes"

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
        self.finances: list[dict[str, Any]] = []
        self.relationships: list[dict[str, Any]] = []

    def extract(self) -> None:
        bndes_dir = Path(self.data_dir) / "bndes"
        if not bndes_dir.exists():
            logger.warning("[%s] Data directory not found: %s", self.name, bndes_dir)
            return
        csv_path = bndes_dir / "operacoes-nao-automaticas.csv"
        if not csv_path.exists():
            logger.warning("[%s] CSV file not found: %s", self.name, csv_path)
            return
        self._raw = pd.read_csv(
            csv_path,
            dtype=str,
            delimiter=";",
            encoding="latin-1",
            keep_default_na=False,
        )
        logger.info("[bndes] Extracted %d rows from non-automatic operations", len(self._raw))

    def transform(self) -> None:
        finances: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            cnpj_raw = str(row.get("cnpj", "")).strip()
            digits = strip_document(cnpj_raw)
            if len(digits) != 14:
                continue

            cnpj_formatted = format_cnpj(cnpj_raw)
            contrato = str(row.get("numero_do_contrato", "")).strip()
            if not contrato:
                continue

            finance_id = f"bndes_{contrato}"
            valor_contratado = parse_brl_amount(row.get("valor_contratado_reais", ""))
            valor_desembolsado = parse_brl_amount(row.get("valor_desembolsado_reais", ""))
            date = str(row.get("data_da_contratacao", "")).strip()
            description = str(row.get("descricao_do_projeto", "")).strip()
            cliente = normalize_name(str(row.get("cliente", "")))
            produto = str(row.get("produto", "")).strip()
            juros = str(row.get("juros", "")).strip()
            uf = str(row.get("uf", "")).strip()
            municipio = str(row.get("municipio", "")).strip()
            setor = str(row.get("setor_bndes", "")).strip()
            porte = str(row.get("porte_do_cliente", "")).strip()
            situacao = str(row.get("situacao_do_contrato", "")).strip()

            finances.append({
                "finance_id": finance_id,
                "type": "bndes_loan",
                "contract_number": contrato,
                "value": valor_desembolsado or valor_contratado,
                "value_contracted": valor_contratado,
                "value_disbursed": valor_desembolsado,
                "date": date,
                "description": description,
                "product": produto,
                "rate": juros,
                "uf": uf,
                "municipio": municipio,
                "sector": setor,
                "client_size": porte,
                "status": situacao,
                "source": "bndes",
            })

            relationships.append({
                "source_key": cnpj_formatted,
                "target_key": finance_id,
                "value_contracted": valor_contratado,
                "value_disbursed": valor_desembolsado,
                "rate": juros,
                "date": date,
                "client_name": cliente,
            })

        self.finances = deduplicate_rows(finances, ["finance_id"])
        self.relationships = relationships
        logger.info(
            "[bndes] Transformed %d Finance nodes, %d relationships",
            len(self.finances),
            len(self.relationships),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.finances:
            loaded = loader.load_nodes("Finance", self.finances, key_field="finance_id")
            logger.info("[bndes] Loaded %d Finance nodes", loaded)

        if self.relationships:
            query = (
                "UNWIND $rows AS row "
                "MERGE (c:Company {cnpj: row.source_key}) "
                "ON CREATE SET c.razao_social = row.client_name, c.name = row.client_name "
                "WITH c, row "
                "MATCH (f:Finance {finance_id: row.target_key}) "
                "MERGE (c)-[r:RECEBEU_EMPRESTIMO]->(f) "
                "SET r.value_contracted = row.value_contracted, "
                "    r.value_disbursed = row.value_disbursed, "
                "    r.rate = row.rate, "
                "    r.date = row.date"
            )
            loaded = loader.run_query_with_retry(query, self.relationships, batch_size=500)
            logger.info("[bndes] Loaded %d RECEBEU_EMPRESTIMO relationships", loaded)

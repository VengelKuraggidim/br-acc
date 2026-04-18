from __future__ import annotations

import csv
import hashlib
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


def _generate_penalty_id(cnpj_digits: str, process_number: str, penalty_type: str) -> str:
    """Deterministic ID from CNPJ digits + process number + penalty type."""
    raw = f"{cnpj_digits}:{process_number}:{penalty_type}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# --------------------------------------------------------------------------
# Module-level fetch_to_disk: Olinda OData public endpoint (no auth).
# --------------------------------------------------------------------------
#
# BCB publishes "Quadro Geral do Processo Administrativo Sancionador" on its
# Olinda OData gateway. The endpoint is public, paginated via ``$top``/
# ``$skip`` and returns JSON. We translate the upstream column names to the
# semicolon/latin-1 layout the legacy ``penalidades.csv`` dumps used (and
# that ``BcbPipeline.extract()`` consumes verbatim) so the pipeline needs no
# schema change.
#
# Upstream docs:
#   https://dadosabertos.bcb.gov.br/dataset/processo-administrativo-sancionador---penalidades-aplicadas

_BCB_OLINDA_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/Gepad_QuadroPenalidades/"
    "versao/v1/odata/QuadroGeralProcessoAdministrativoSancionador"
)
_BCB_HTTP_TIMEOUT = 120.0
_BCB_PAGE_SIZE = 1000

# Pipeline-side CSV column order. BcbPipeline.extract() reads pt-BR headers
# with accented characters verbatim; keep byte-for-byte fidelity here.
_BCB_CSV_COLUMNS: list[str] = [
    "Número Processo",
    "Nome Instituição",
    "CNPJ",
    "Data Decisão",
    "Tipo Penalidade",
    "Valor Penalidade",
    "Situação",
]

# Olinda JSON fields -> canonical CSV columns. Where upstream exposes both
# 1ª and 2ª instância columns we prefer the 2ª instância (final) value if
# present, else fall back to the 1ª instância.
def _row_to_csv(item: dict[str, Any]) -> dict[str, str]:
    """Project an Olinda record onto the legacy CSV schema."""
    def _pick(*keys: str) -> str:
        for k in keys:
            v = item.get(k)
            if v is None or str(v).lower() == "null":
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    tipo = _pick("Tipo_penalidade_2_instancia", "Tipo_penalidade_1_instancia")
    valor = _pick("Valor_da_multa_2_instancia", "Valor_da_multa_1_instancia")
    # Legacy CSV used pt-BR comma decimal; Olinda returns a JSON number -> str
    # with "." decimal. BcbPipeline.transform() feeds this through
    # parse_brl_amount, which tolerates either separator, so we pass through.
    data_dec = _pick("Data_da_decisao_2_instancia", "Data_da_decisao_1_instancia")
    num_dec = _pick("Numero_decisao_2_instancia", "Numero_decisao_1_instancia")

    return {
        "Número Processo": _pick("PAS") or num_dec,
        "Nome Instituição": _pick("Nome"),
        "CNPJ": _pick("CPF_CNPJ"),
        "Data Decisão": data_dec,
        "Tipo Penalidade": tipo,
        "Valor Penalidade": valor,
        "Situação": _pick("Situacao"),
    }


def fetch_to_disk(
    output_dir: Path | str,
    url: str = _BCB_OLINDA_URL,
    page_size: int = _BCB_PAGE_SIZE,
    limit: int | None = None,
    timeout: float = _BCB_HTTP_TIMEOUT,
) -> list[Path]:
    """Download BCB PAS penalty data to ``output_dir`` as ``penalidades.csv``.

    Paginates the public Olinda OData endpoint with ``$top``/``$skip`` until
    the server returns an empty ``value`` array. Writes a semicolon-separated
    Latin-1 CSV with the legacy column layout so ``BcbPipeline.extract()``
    reads it unchanged.

    Args:
        output_dir: Destination directory. Created if missing.
        url: Override for the OData base URL.
        page_size: Records per HTTP request (default 1000).
        limit: If set, stop fetching once this many rows have been written.
        timeout: Per-request HTTP timeout in seconds.

    Returns:
        List of paths written (single-element list containing
        ``<output_dir>/penalidades.csv``).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "penalidades.csv"

    logger.info(
        "[bcb.fetch_to_disk] Downloading %s (page_size=%d, limit=%s) -> %s",
        url, page_size, limit, out_path,
    )

    total_written = 0
    with (
        httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "br-acc/bracc-etl download_bcb (httpx)"},
        ) as client,
        open(out_path, "w", encoding="latin-1", newline="", errors="replace") as fh,
    ):
        writer = csv.DictWriter(
            fh, fieldnames=_BCB_CSV_COLUMNS, delimiter=";",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()

        skip = 0
        while True:
            params = {
                "$format": "json",
                "$top": str(page_size),
                "$skip": str(skip),
            }
            resp = client.get(url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            items = payload.get("value") or []
            if not items:
                break

            for item in items:
                row = _row_to_csv(item)
                if not strip_document(row["CNPJ"]):
                    continue
                writer.writerow(row)
                total_written += 1
                if limit is not None and total_written >= limit:
                    break

            logger.info(
                "[bcb.fetch_to_disk] paged skip=%d (+%d) total_written=%d",
                skip, len(items), total_written,
            )

            if limit is not None and total_written >= limit:
                break
            if len(items) < page_size:
                break
            skip += len(items)

    logger.info(
        "[bcb.fetch_to_disk] Wrote %d rows to %s (%.1f KB)",
        total_written, out_path, out_path.stat().st_size / 1024,
    )
    return [out_path.resolve()]


class BcbPipeline(Pipeline):
    """ETL pipeline for BCB (Banco Central do Brasil) penalties."""

    name = "bcb"
    source_id = "bcb"

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
        self.penalties: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        bcb_dir = Path(self.data_dir) / "bcb"
        self._raw = pd.read_csv(
            bcb_dir / "penalidades.csv",
            sep=";",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
        )

    def transform(self) -> None:
        penalties: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []

        for _idx, row in self._raw.iterrows():
            cnpj_raw = str(row.get("CNPJ", ""))
            digits = strip_document(cnpj_raw)

            if len(digits) != 14:
                continue

            cnpj_formatted = format_cnpj(cnpj_raw)
            institution_name = normalize_name(str(row.get("Nome Instituição", "")))
            penalty_type = str(row.get("Tipo Penalidade", "")).strip()
            penalty_value_raw = str(row.get("Valor Penalidade", "")).strip()
            process_number = str(row.get("Número Processo", "")).strip()
            decision_date = str(row.get("Data Decisão", "")).strip()

            penalty_value = parse_brl_amount(penalty_value_raw, default=None)

            penalty_id = _generate_penalty_id(digits, process_number, penalty_type)

            penalty: dict[str, Any] = {
                "penalty_id": penalty_id,
                "cnpj": cnpj_formatted,
                "institution_name": institution_name,
                "penalty_type": penalty_type,
                "process_number": process_number,
                "decision_date": decision_date,
                "source": "bcb",
            }
            if penalty_value is not None:
                penalty["penalty_value"] = penalty_value

            penalties.append(penalty)

            company_rels.append({
                "source_key": cnpj_formatted,
                "target_key": penalty_id,
            })

        self.penalties = deduplicate_rows(penalties, ["penalty_id"])
        self.company_rels = company_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.penalties:
            loader.load_nodes("BCBPenalty", self.penalties, key_field="penalty_id")

        # Ensure Company nodes exist for CNPJ linking
        if self.company_rels:
            companies = [
                {"cnpj": rel["source_key"]} for rel in self.company_rels
            ]
            loader.load_nodes("Company", deduplicate_rows(companies, ["cnpj"]), key_field="cnpj")

        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (b:BCBPenalty {penalty_id: row.target_key}) "
                "MERGE (c)-[:BCB_PENALIZADA]->(b)"
            )
            loader.run_query_with_retry(query, self.company_rels)

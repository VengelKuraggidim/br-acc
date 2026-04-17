"""ETL pipeline for PNCP procurement data scoped to Goias (GO).

Ingests state and municipal procurement publications from the PNCP REST API
filtered by UF=GO.  Creates GoProcurement nodes linked to Company nodes
via FORNECEU_GO (supplier) and CONTRATOU_GO (contracting agency) relationships.

Data source: https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    cap_contract_value,
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_date,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_API_BASE = "https://pncp.gov.br/api/consulta/v1/"

# PNCP modalidade IDs to human-readable labels.
# Codes 2 and 14 added 2026-04 after confirming via curl that the PNCP API
# accepts them (HTTP 204 for GO/2024-Q1, while e.g. 20 returns HTTP 422
# "Código da modalidade de contratação inválido"). Codes 15-19 are silently
# accepted too but are undocumented in the PNCP Manual de Dados Abertos and
# returned no GO data in probes, so they are intentionally omitted to avoid
# multiplying empty API calls.
_MODALIDADE_MAP: dict[int, str] = {
    1: "leilao_eletronico",
    2: "dialogo_competitivo",
    3: "concurso",
    4: "dialogo_competitivo",
    5: "concorrencia",
    6: "pregao_eletronico",
    7: "cotacao_eletronica",
    8: "dispensa",
    9: "inexigibilidade",
    10: "manifestacao_interesse",
    11: "pre_qualificacao",
    12: "credenciamento",
    13: "ata_pre_existente",
    14: "inaplicabilidade_licitacao",
}

_RATE_LIMIT_SLEEP = 0.5
_HTTP_TIMEOUT = 30
# API requires tamanhoPagina >= 10
_DEFAULT_PAGE_SIZE = 50
# API requires codigoModalidadeContratacao; iterate all modalidades.
_MODALIDADE_CODES = tuple(_MODALIDADE_MAP.keys())
# API rejects date ranges > 365 days with HTTP 422.
_MAX_WINDOW_DAYS = 365
# Default historical window when caller does not pass an explicit range.
# PNCP went live in 2021 so ~5 years (1826 days) is the realistic upper
# bound: older ranges are silently accepted by the API (HTTP 204) but hold
# no data. Kept in sync with ``PncpGoPipeline.extract`` below.
_DEFAULT_HISTORICAL_DAYS = 1826


def _make_procurement_id(cnpj_digits: str, year: int | str, sequential: int | str) -> str:
    """Create a stable procurement ID by hashing CNPJ + year + sequential."""
    raw = f"{cnpj_digits}:{year}:{sequential}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _split_date_windows(date_start: str, date_end: str) -> list[tuple[str, str]]:
    """Split a date range into API-compatible windows (<= 365 days each).

    Dates are YYYYMMDD strings (same format the PNCP API expects).
    """
    start = datetime.strptime(date_start, "%Y%m%d")  # noqa: DTZ007
    end = datetime.strptime(date_end, "%Y%m%d")  # noqa: DTZ007
    windows: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=_MAX_WINDOW_DAYS - 1), end)
        windows.append((cursor.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")))
        cursor = window_end + timedelta(days=1)
    return windows


def _iter_api_combo(
    client: httpx.Client,
    modalidade_code: int,
    win_start: str,
    win_end: str,
) -> list[dict[str, Any]]:
    """Fetch all pages for a single (modalidade, window) combo from PNCP.

    Returns the aggregated list of raw records (may be empty).  Network/JSON
    errors are logged and treated as early termination for this combo — the
    caller keeps iterating other combos.
    """
    url = f"{_API_BASE}contratacoes/publicacao"
    records: list[dict[str, Any]] = []
    page = 1
    while True:
        params: dict[str, str | int] = {
            "dataInicial": win_start,
            "dataFinal": win_end,
            "uf": "GO",
            "codigoModalidadeContratacao": modalidade_code,
            "pagina": page,
            "tamanhoPagina": _DEFAULT_PAGE_SIZE,
        }
        try:
            resp = client.get(url, params=params)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "PNCP API request failed "
                "(modalidade %d, window %s-%s, page %d): %s",
                modalidade_code, win_start, win_end, page, exc,
            )
            break

        if not resp.content:
            break

        try:
            payload = resp.json()
        except json.JSONDecodeError as exc:
            logger.warning(
                "PNCP API returned non-JSON "
                "(modalidade %d, window %s-%s, page %d): %s",
                modalidade_code, win_start, win_end, page, exc,
            )
            break

        if isinstance(payload, dict) and "data" in payload:
            page_records = payload["data"]
        elif isinstance(payload, list):
            page_records = payload
        else:
            logger.warning(
                "Unexpected API response "
                "(modalidade %d, window %s-%s, page %d)",
                modalidade_code, win_start, win_end, page,
            )
            break

        if not page_records:
            break

        records.extend(page_records)

        pages_remaining = 0
        if isinstance(payload, dict):
            pages_remaining = payload.get("paginasRestantes", 0)
        if pages_remaining <= 0:
            break

        page += 1
        time.sleep(_RATE_LIMIT_SLEEP)

    return records


def fetch_to_disk(
    output_dir: Path,
    date_start: str | None = None,
    date_end: str | None = None,
    limit: int | None = None,
    modalidades: list[int] | None = None,
) -> list[Path]:
    """Download PNCP GO procurement records and persist them as JSON on disk.

    Mirrors the fetch loop used by ``PncpGoPipeline.extract`` so the ETL can
    be fed from local files (the preferred ``script_download`` acquisition
    mode) instead of hitting the API inline during bootstrap.

    Args:
        output_dir: Directory where per-combo JSON files are written. Created
            if missing. One JSON file is produced per (modalidade, window)
            combo that returned at least one record.
        date_start: Inclusive start date in ``YYYY-MM-DD`` or ``YYYYMMDD``
            format. Defaults to the same historical window used by
            ``PncpGoPipeline.extract`` (``_DEFAULT_HISTORICAL_DAYS``, ~5
            years back from today, capped at the PNCP launch in 2021).
        date_end: Inclusive end date in ``YYYY-MM-DD`` or ``YYYYMMDD`` format.
            Defaults to today.
        limit: Optional cap on the total number of records fetched. Useful
            for smoke tests; ``None`` means no cap (full historical).
        modalidades: PNCP modalidade codes to iterate. ``None`` defaults to
            the full set hard-coded in the pipeline
            (``_MODALIDADE_CODES``).

    Returns:
        List of JSON file paths written to disk (sorted).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def _normalize(d: str) -> str:
        return d.replace("-", "")

    today = datetime.now()  # noqa: DTZ005
    date_end_norm = (
        today.strftime("%Y%m%d") if date_end is None else _normalize(date_end)
    )
    date_start_norm = (
        (today - timedelta(days=_DEFAULT_HISTORICAL_DAYS)).strftime("%Y%m%d")
        if date_start is None
        else _normalize(date_start)
    )

    mod_codes: tuple[int, ...] = tuple(modalidades) if modalidades else _MODALIDADE_CODES
    windows = _split_date_windows(date_start_norm, date_end_norm)

    logger.info(
        "Fetching PNCP GO records: %s to %s, modalidades=%s, limit=%s",
        date_start_norm, date_end_norm, list(mod_codes), limit,
    )
    logger.info("Date windows: %d, modalidades: %d", len(windows), len(mod_codes))

    written: list[Path] = []
    total_records = 0

    with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
        for modalidade_code in mod_codes:
            if limit is not None and total_records >= limit:
                break
            for win_start, win_end in windows:
                if limit is not None and total_records >= limit:
                    break

                records = _iter_api_combo(client, modalidade_code, win_start, win_end)
                if not records:
                    continue

                if limit is not None:
                    remaining = limit - total_records
                    if remaining <= 0:
                        break
                    if len(records) > remaining:
                        records = records[:remaining]

                filename = f"pncp_go_mod{modalidade_code:02d}_{win_start}_{win_end}.json"
                out_path = output_dir / filename
                out_path.write_text(
                    json.dumps({"data": records}, ensure_ascii=False),
                    encoding="utf-8",
                )
                written.append(out_path)
                total_records += len(records)
                logger.info(
                    "  wrote %s (%d records, modalidade %d/%s)",
                    out_path.name,
                    len(records),
                    modalidade_code,
                    _MODALIDADE_MAP.get(modalidade_code, "?"),
                )

    logger.info(
        "PNCP GO fetch complete: %d records across %d file(s)",
        total_records,
        len(written),
    )
    return sorted(written)


class PncpGoPipeline(Pipeline):
    """ETL pipeline for Goias (GO) procurement publications from PNCP."""

    name = "pncp_go"
    source_id = "pncp_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_records: list[dict[str, Any]] = []
        self.procurements: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

    @staticmethod
    def _split_date_windows(
        date_start: str, date_end: str,
    ) -> list[tuple[str, str]]:
        """Split a date range into API-compatible windows (<= 365 days each)."""
        return _split_date_windows(date_start, date_end)

    def _fetch_from_api(
        self,
        date_start: str,
        date_end: str,
    ) -> list[dict[str, Any]]:
        """Fetch GO procurements from PNCP API with pagination.

        The endpoint requires `codigoModalidadeContratacao` and rejects
        windows > 365 days. We iterate all modalidades and chunk the
        requested range into yearly windows.
        """
        all_records: list[dict[str, Any]] = []
        windows = _split_date_windows(date_start, date_end)

        with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
            for modalidade_code in _MODALIDADE_CODES:
                mod_count = 0
                for win_start, win_end in windows:
                    records = _iter_api_combo(
                        client, modalidade_code, win_start, win_end,
                    )
                    if records:
                        all_records.extend(records)
                        mod_count += len(records)

                if mod_count:
                    logger.info(
                        "  modalidade %d (%s): %d records",
                        modalidade_code,
                        _MODALIDADE_MAP.get(modalidade_code, "?"),
                        mod_count,
                    )

        return all_records

    def _load_local_files(self) -> list[dict[str, Any]]:
        """Load pre-downloaded PNCP GO JSON files from data/pncp_go/."""
        src_dir = Path(self.data_dir) / "pncp_go"
        if not src_dir.exists():
            return []

        json_files = sorted(src_dir.glob("*.json"))
        if not json_files:
            return []

        all_records: list[dict[str, Any]] = []
        for f in json_files:
            try:
                raw = f.read_text(encoding="utf-8")
                payload = json.loads(raw, strict=False)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Failed to parse JSON from %s: %s", f, exc)
                continue

            if isinstance(payload, dict) and "data" in payload:
                records = payload["data"]
            elif isinstance(payload, list):
                records = payload
            else:
                logger.warning("Unexpected format in %s, skipping", f.name)
                continue

            all_records.extend(records)
            logger.info("  Loaded %d records from %s", len(records), f.name)

        return all_records

    def extract(self) -> None:
        """Load GO procurement data from local files, falling back to the API."""
        records = self._load_local_files()

        if not records:
            logger.info("No local files found; fetching from PNCP API...")
            today = datetime.now()  # noqa: DTZ005
            history_start = today - timedelta(days=_DEFAULT_HISTORICAL_DAYS)
            date_start = history_start.strftime("%Y%m%d")
            date_end = today.strftime("%Y%m%d")
            records = self._fetch_from_api(date_start, date_end)

        logger.info("Total raw GO procurement records: %d", len(records))
        self._raw_records = records

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        """Normalize fields, format CNPJs, create stable IDs, deduplicate."""
        if not self._raw_records:
            return

        procurements: list[dict[str, Any]] = []
        skipped_no_cnpj = 0
        skipped_zero_value = 0

        for rec in self._raw_records:
            org = rec.get("orgaoEntidade") or {}
            cnpj_raw = str(org.get("cnpj", "")).strip()
            cnpj_digits = strip_document(cnpj_raw)

            if len(cnpj_digits) != 14:
                skipped_no_cnpj += 1
                continue

            agency_cnpj = format_cnpj(cnpj_raw)

            # Value: prefer homologado, fallback to estimado
            valor = rec.get("valorTotalHomologado") or rec.get("valorTotalEstimado") or 0
            if not valor or float(valor) <= 0:
                skipped_zero_value += 1
                continue

            year = rec.get("anoCompra", "")
            sequential = rec.get("sequencialCompra", "")

            # Stable ID from hash of CNPJ + year + sequential
            procurement_id = _make_procurement_id(cnpj_digits, year, sequential)

            agency_name = normalize_name(str(org.get("razaoSocial", "")))

            # Location
            unidade = rec.get("unidadeOrgao") or {}
            municipality = str(unidade.get("municipioNome", "")).strip()

            # Modality
            modalidade_id = rec.get("modalidadeId")
            modality = _MODALIDADE_MAP.get(modalidade_id, "") if modalidade_id else ""
            modalidade_nome = str(rec.get("modalidadeNome", "")).strip()

            # Date
            data_pub = str(rec.get("dataPublicacaoPncp", "")).strip()
            published_at = parse_date(data_pub[:10]) if data_pub else ""

            # Description
            objeto = normalize_name(str(rec.get("objetoCompra", "")))

            # Supplier CNPJs (if available in the record)
            fornecedores_raw = rec.get("fornecedores") or []
            fornecedores: list[dict[str, str]] = []
            for forn in fornecedores_raw:
                forn_cnpj_raw = str(forn.get("cnpj", "")).strip()
                forn_digits = strip_document(forn_cnpj_raw)
                if len(forn_digits) == 14:
                    fornecedores.append({
                        "cnpj": format_cnpj(forn_cnpj_raw),
                        "razao_social": normalize_name(str(forn.get("razaoSocial", ""))),
                    })

            procurements.append({
                "procurement_id": procurement_id,
                "cnpj_agency": agency_cnpj,
                "agency_name": agency_name,
                "year": int(year) if year else None,
                "sequential": int(sequential) if sequential else None,
                "object": objeto,
                "modality": modality or modalidade_nome,
                "amount_estimated": cap_contract_value(float(valor)),
                "published_at": published_at,
                "uf": "GO",
                "municipality": municipality,
                "source": "pncp_go",
                "fornecedores": fornecedores,
            })

        self.procurements = deduplicate_rows(procurements, ["procurement_id"])

        logger.info(
            "Transformed: %d GO procurements (skipped %d no-CNPJ, %d zero-value)",
            len(self.procurements),
            skipped_no_cnpj,
            skipped_zero_value,
        )

        if self.limit:
            self.procurements = self.procurements[: self.limit]

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load GoProcurement nodes and relationships into Neo4j."""
        if not self.procurements:
            logger.warning("No GO procurements to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # GoProcurement nodes
        procurement_nodes = [
            {
                "procurement_id": p["procurement_id"],
                "cnpj_agency": p["cnpj_agency"],
                "agency_name": p["agency_name"],
                "year": p["year"],
                "sequential": p["sequential"],
                "object": p["object"],
                "modality": p["modality"],
                "amount_estimated": p["amount_estimated"],
                "published_at": p["published_at"],
                "uf": p["uf"],
                "municipality": p["municipality"],
                "source": p["source"],
            }
            for p in self.procurements
        ]
        count = loader.load_nodes("GoProcurement", procurement_nodes, key_field="procurement_id")
        logger.info("Loaded %d GoProcurement nodes", count)

        # Ensure Company nodes exist for contracting agencies
        agencies = deduplicate_rows(
            [
                {"cnpj": p["cnpj_agency"], "razao_social": p["agency_name"]}
                for p in self.procurements
            ],
            ["cnpj"],
        )
        count = loader.load_nodes("Company", agencies, key_field="cnpj")
        logger.info("Merged %d Company (agency) nodes", count)

        # CONTRATOU_GO: Company (agency) -> GoProcurement
        agency_rels = [
            {"source_key": p["cnpj_agency"], "target_key": p["procurement_id"]}
            for p in self.procurements
        ]
        count = loader.load_relationships(
            rel_type="CONTRATOU_GO",
            rows=agency_rels,
            source_label="Company",
            source_key="cnpj",
            target_label="GoProcurement",
            target_key="procurement_id",
        )
        logger.info("Created %d CONTRATOU_GO relationships", count)

        # FORNECEU_GO: Company (supplier) -> GoProcurement
        supplier_company_rows: list[dict[str, Any]] = []
        supplier_rels: list[dict[str, Any]] = []
        for p in self.procurements:
            for forn in p.get("fornecedores", []):
                supplier_company_rows.append({
                    "cnpj": forn["cnpj"],
                    "razao_social": forn["razao_social"],
                })
                supplier_rels.append({
                    "source_key": forn["cnpj"],
                    "target_key": p["procurement_id"],
                })

        if supplier_company_rows:
            deduped_suppliers = deduplicate_rows(supplier_company_rows, ["cnpj"])
            count = loader.load_nodes("Company", deduped_suppliers, key_field="cnpj")
            logger.info("Merged %d Company (supplier) nodes", count)

            count = loader.load_relationships(
                rel_type="FORNECEU_GO",
                rows=supplier_rels,
                source_label="Company",
                source_key="cnpj",
                target_label="GoProcurement",
                target_key="procurement_id",
            )
            logger.info("Created %d FORNECEU_GO relationships", count)

"""ETL pipeline for PNCP (Portal Nacional de Contratações Públicas) bids.

Ingests procurement publications from all government levels via the PNCP REST API.
Creates Bid nodes linked to Company (agency) nodes via LICITOU relationships.
Distinct from ComprasNet pipeline — this covers the publication/bid stage
(licitações, pregões, dispensas), not the contract execution stage.

Data source: https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao
"""

from __future__ import annotations

import json
import logging
import re
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
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# PNCP public consulta API — same base used by pncp_go but without the
# UF filter so we cover every government level (federal/estadual/municipal).
_PNCP_API_BASE = "https://pncp.gov.br/api/consulta/v1/"
# tamanhoPagina: API rejects values < 10 and caps at ~500 in practice.
# 50 keeps response payloads manageable while still being efficient.
_PNCP_PAGE_SIZE = 50
# API rejects date ranges > 365 days with HTTP 422.
_PNCP_WINDOW_DAYS = 365
_PNCP_HTTP_TIMEOUT = 60
_PNCP_RATE_LIMIT_SLEEP = 0.3
# Two retries — beyond that the modalidade is likely silently failing.
_PNCP_MAX_RETRIES = 2
_PNCP_RETRY_BACKOFF = 3.0
# Skip the rest of a modalidade if the same window times out twice; the
# server tends to stall instead of returning HTTP 0.
_PNCP_SKIP_MODALIDADE_AFTER_TIMEOUT = True

# PNCP modalidade IDs to human-readable labels
_MODALIDADE_MAP: dict[int, str] = {
    1: "leilao_eletronico",
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
}

# PNCP esfera IDs to labels
_ESFERA_MAP: dict[str, str] = {
    "F": "federal",
    "E": "estadual",
    "M": "municipal",
    "D": "distrital",
}


def _split_date_windows(date_start: str, date_end: str) -> list[tuple[str, str]]:
    """Split a date range into PNCP-API-compatible windows (<= 365 days).

    Dates are YYYYMMDD strings — same format the PNCP endpoint expects.
    """
    start = datetime.strptime(date_start, "%Y%m%d")  # noqa: DTZ007
    end = datetime.strptime(date_end, "%Y%m%d")  # noqa: DTZ007
    windows: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=_PNCP_WINDOW_DAYS - 1), end)
        windows.append(
            (cursor.strftime("%Y%m%d"), window_end.strftime("%Y%m%d"))
        )
        cursor = window_end + timedelta(days=1)
    return windows


def _request_with_retry(
    client: httpx.Client,
    url: str,
    params: dict[str, str | int],
) -> httpx.Response | None:
    """GET with retry + exponential backoff on timeouts and 5xx."""
    for attempt in range(1, _PNCP_MAX_RETRIES + 1):
        try:
            resp = client.get(url, params=params)
        except httpx.TimeoutException:
            if attempt == _PNCP_MAX_RETRIES:
                return None
            time.sleep(_PNCP_RETRY_BACKOFF ** attempt)
            continue
        except httpx.HTTPError:
            return None
        if resp.status_code >= 500:
            if attempt == _PNCP_MAX_RETRIES:
                return resp
            time.sleep(_PNCP_RETRY_BACKOFF ** attempt)
            continue
        return resp
    return None


def _iter_window(
    client: httpx.Client,
    modalidade_code: int,
    win_start: str,
    win_end: str,
) -> tuple[list[dict[str, Any]], bool]:
    """Fetch all pages for a (modalidade, window) combo. Returns (records, timed_out)."""
    url = f"{_PNCP_API_BASE}contratacoes/publicacao"
    records: list[dict[str, Any]] = []
    page = 1
    while True:
        params: dict[str, str | int] = {
            "dataInicial": win_start,
            "dataFinal": win_end,
            "codigoModalidadeContratacao": modalidade_code,
            "pagina": page,
            "tamanhoPagina": _PNCP_PAGE_SIZE,
        }
        resp = _request_with_retry(client, url, params)
        if resp is None:
            logger.warning(
                "PNCP timeout after retries (modalidade=%d window=%s-%s page=%d)",
                modalidade_code, win_start, win_end, page,
            )
            return records, True

        # 204 No Content for empty windows is success.
        if resp.status_code == 204 or not resp.content:
            break

        try:
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "PNCP HTTP error (modalidade=%d window=%s-%s page=%d): %s",
                modalidade_code, win_start, win_end, page, exc,
            )
            break

        try:
            payload = resp.json()
        except json.JSONDecodeError as exc:
            logger.warning(
                "PNCP non-JSON response (modalidade=%d window=%s-%s page=%d): %s",
                modalidade_code, win_start, win_end, page, exc,
            )
            break

        if isinstance(payload, dict) and "data" in payload:
            page_records = payload["data"]
        elif isinstance(payload, list):
            page_records = payload
        else:
            logger.warning(
                "Unexpected PNCP response shape (modalidade=%d window=%s-%s page=%d)",
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
        time.sleep(_PNCP_RATE_LIMIT_SLEEP)

    return records, False


def fetch_to_disk(
    output_dir: Path,
    *,
    date: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    modalidades: list[int] | None = None,
    limit: int | None = None,
) -> list[Path]:
    """Download PNCP procurement records (national scope) to disk.

    The :class:`PncpPipeline` extract step globs ``pncp_*.json`` under
    ``data/pncp/``; this writer produces one file per (year, modalidade)
    combination (``pncp_<modalidade>_<year>.json``) when records exist.

    Parameters
    ----------
    output_dir:
        Destination directory. Created if missing.
    date:
        Accepted for API symmetry; if provided as ``YYYY-MM-DD``/``YYYYMMDD``
        and ``start_year``/``end_year`` are unset, the year portion seeds a
        single-year fetch.
    start_year, end_year:
        Inclusive year range to iterate. Defaults to the previous two
        calendar years (PNCP populated steadily since 2021; recent windows
        usually have the freshest data).
    modalidades:
        List of PNCP modalidade codes (1-13) to iterate. Defaults to the
        full set defined by :data:`_MODALIDADE_MAP`.
    limit:
        Optional cap on total records fetched across the whole run. Useful
        for smoke tests; ``None`` means no cap.

    Returns
    -------
    Sorted list of JSON paths written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now()  # noqa: DTZ005
    if start_year is None and end_year is None and date:
        token = date.replace("-", "")
        if len(token) >= 4:
            try:
                year = int(token[:4])
                start_year = year
                end_year = year
            except ValueError:
                pass
    if end_year is None:
        end_year = today.year
    if start_year is None:
        start_year = max(2021, today.year - 1)

    mod_codes: list[int] = list(modalidades) if modalidades else list(_MODALIDADE_MAP.keys())

    written: list[Path] = []
    total_records = 0

    headers = {"User-Agent": "BR-ACC-ETL/1.0 (pncp)"}
    with httpx.Client(timeout=_PNCP_HTTP_TIMEOUT, headers=headers) as client:
        for year in range(start_year, end_year + 1):
            if limit is not None and total_records >= limit:
                break
            win_start = f"{year:04d}0101"
            win_end_full = f"{year:04d}1231"
            # Cap to today for the current year (PNCP rejects future dates
            # silently with empty windows).
            if year == today.year:
                win_end_full = today.strftime("%Y%m%d")
            windows = _split_date_windows(win_start, win_end_full)

            for modalidade_code in mod_codes:
                if limit is not None and total_records >= limit:
                    break
                year_records: list[dict[str, Any]] = []
                skip = False
                for w_start, w_end in windows:
                    if skip:
                        break
                    if limit is not None and total_records + len(year_records) >= limit:
                        break
                    records, timed_out = _iter_window(
                        client, modalidade_code, w_start, w_end,
                    )
                    if records:
                        year_records.extend(records)
                    if timed_out and _PNCP_SKIP_MODALIDADE_AFTER_TIMEOUT:
                        skip = True

                if not year_records:
                    continue

                if limit is not None:
                    remaining = limit - total_records
                    if remaining <= 0:
                        break
                    if len(year_records) > remaining:
                        year_records = year_records[:remaining]

                out_path = output_dir / f"pncp_{modalidade_code:02d}_{year}.json"
                out_path.write_text(
                    json.dumps({"data": year_records}, ensure_ascii=False),
                    encoding="utf-8",
                )
                written.append(out_path)
                total_records += len(year_records)
                logger.info(
                    "[pncp] wrote %s (%d records, modalidade %d/%s)",
                    out_path.name,
                    len(year_records),
                    modalidade_code,
                    _MODALIDADE_MAP.get(modalidade_code, "?"),
                )

    logger.info(
        "[pncp] fetch complete: %d records across %d file(s)",
        total_records, len(written),
    )
    return sorted(written)


class PncpPipeline(Pipeline):
    """ETL pipeline for PNCP procurement bid publications."""

    name = "pncp"
    source_id = "pncp_bids"

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
        self.bids: list[dict[str, Any]] = []
        self.coverage_start: str = ""
        self.coverage_end: str = ""
        self.coverage_complete: bool = False

    def _infer_coverage(
        self,
        src_dir: Path,
        json_files: list[Path],
        records: list[dict[str, Any]],
    ) -> None:
        """Infer PNCP dataset coverage window.

        Priority:
        1. Explicit manifest file (coverage.json) if available.
        2. Min/max from dataPublicacaoPncp in loaded records.
        3. Min/max month inferred from file names.
        """
        manifest_path = src_dir / "coverage.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                self.coverage_start = str(manifest.get("coverage_start", "")).strip()
                self.coverage_end = str(manifest.get("coverage_end", "")).strip()
                self.coverage_complete = bool(manifest.get("coverage_complete", False))
                return
            except Exception as exc:
                logger.warning("Invalid PNCP coverage manifest %s: %s", manifest_path, exc)

        dates: list[str] = []
        for rec in records:
            raw_date = str(rec.get("dataPublicacaoPncp", "")).strip()
            if len(raw_date) >= 10:
                candidate = raw_date[:10]
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
                    dates.append(candidate)
        if dates:
            self.coverage_start = min(dates)
            self.coverage_end = max(dates)
            self.coverage_complete = False
            return

        month_candidates: list[str] = []
        for f in json_files:
            match = re.search(r"(\d{4})(\d{2})", f.stem)
            if match:
                month_candidates.append(f"{match.group(1)}-{match.group(2)}")
        if month_candidates:
            first_month = min(month_candidates)
            last_month = max(month_candidates)
            self.coverage_start = f"{first_month}-01"
            self.coverage_end = f"{last_month}-31"
            self.coverage_complete = False

    def extract(self) -> None:
        """Load pre-downloaded PNCP JSON files from data/pncp/."""
        src_dir = Path(self.data_dir) / "pncp"
        json_files = sorted(src_dir.glob("pncp_*.json"))
        if not json_files:
            logger.warning("No PNCP JSON files found in %s", src_dir)
            return

        all_records: list[dict[str, Any]] = []
        for f in json_files:
            try:
                raw = f.read_text(encoding="utf-8")
                payload = json.loads(raw, strict=False)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Failed to parse JSON from %s: %s", f, exc)
                continue

            # Handle both wrapped (API response) and flat (list) formats
            if isinstance(payload, dict) and "data" in payload:
                records = payload["data"]
            elif isinstance(payload, list):
                records = payload
            else:
                logger.warning("Unexpected format in %s, skipping", f.name)
                continue

            all_records.extend(records)
            logger.info("  Loaded %d records from %s", len(records), f.name)

        logger.info("Total raw records: %d", len(all_records))
        self._raw_records = all_records
        self._infer_coverage(src_dir, json_files, all_records)
        logger.info(
            "PNCP coverage window: start=%s end=%s complete=%s",
            self.coverage_start or "unknown",
            self.coverage_end or "unknown",
            self.coverage_complete,
        )

    def transform(self) -> None:
        """Normalize fields, format CNPJs, deduplicate by bid_id."""
        if not self._raw_records:
            return

        bids: list[dict[str, Any]] = []
        skipped_no_cnpj = 0
        skipped_zero_value = 0

        for rec in self._raw_records:
            # Extract agency CNPJ
            org = rec.get("orgaoEntidade") or {}
            cnpj_raw = str(org.get("cnpj", "")).strip()
            cnpj_digits = strip_document(cnpj_raw)

            if len(cnpj_digits) != 14:
                skipped_no_cnpj += 1
                continue

            agency_cnpj = format_cnpj(cnpj_raw)

            # Extract value (prefer homologado, fallback to estimado)
            valor = rec.get("valorTotalHomologado") or rec.get("valorTotalEstimado") or 0
            if not valor or float(valor) <= 0:
                skipped_zero_value += 1
                continue

            # Build stable bid ID from PNCP control number
            numero_controle = str(rec.get("numeroControlePNCP", "")).strip()
            if not numero_controle:
                # Fallback: compose from org CNPJ + sequence + year
                seq = rec.get("sequencialCompra", "")
                ano = rec.get("anoCompra", "")
                numero_controle = f"{cnpj_digits}-1-{seq:06d}/{ano}" if seq and ano else ""

            if not numero_controle:
                continue

            # Agency info
            agency_name = normalize_name(str(org.get("razaoSocial", "")))

            # Location from unidadeOrgao
            unidade = rec.get("unidadeOrgao") or {}
            municipality = str(unidade.get("municipioNome", "")).strip()
            state = str(unidade.get("ufSigla", "")).strip()

            # Modality
            modalidade_id = rec.get("modalidadeId")
            modality = _MODALIDADE_MAP.get(modalidade_id, "") if modalidade_id else ""
            modalidade_nome = str(rec.get("modalidadeNome", "")).strip()

            # Government sphere
            esfera_id = str(org.get("esferaId", "")).strip()
            esfera = _ESFERA_MAP.get(esfera_id, esfera_id)

            # Dates
            data_pub = str(rec.get("dataPublicacaoPncp", "")).strip()
            date = data_pub[:10] if data_pub else ""

            # Status
            status = str(rec.get("situacaoCompraNome", "")).strip()

            # Description
            objeto = normalize_name(str(rec.get("objetoCompra", "")))

            bids.append({
                "bid_id": numero_controle,
                "description": objeto,
                "modality": modality or modalidade_nome,
                "amount": cap_contract_value(float(valor)),
                "date": date,
                "status": status,
                "agency_name": agency_name,
                "agency_cnpj": agency_cnpj,
                "municipality": municipality,
                "state": state,
                "esfera": esfera,
                "processo": str(rec.get("processo", "")).strip(),
                "srp": bool(rec.get("srp", False)),
                "source": "pncp",
                "coverage_start": self.coverage_start,
                "coverage_end": self.coverage_end,
                "coverage_complete": self.coverage_complete,
            })

        self.bids = deduplicate_rows(bids, ["bid_id"])

        logger.info(
            "Transformed: %d bids (skipped %d no-CNPJ, %d zero-value)",
            len(self.bids),
            skipped_no_cnpj,
            skipped_zero_value,
        )

        if self.limit:
            self.bids = self.bids[: self.limit]

    def load(self) -> None:
        """Load Bid nodes and LICITOU relationships into Neo4j."""
        if not self.bids:
            logger.warning("No bids to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # Load Bid nodes (MERGE on bid_id)
        bid_nodes = [
            {
                "bid_id": b["bid_id"],
                "description": b["description"],
                "modality": b["modality"],
                "amount": b["amount"],
                "date": b["date"],
                "status": b["status"],
                "agency_name": b["agency_name"],
                "municipality": b["municipality"],
                "state": b["state"],
                "esfera": b["esfera"],
                "processo": b["processo"],
                "srp": b["srp"],
                "source": b["source"],
                "coverage_start": b["coverage_start"],
                "coverage_end": b["coverage_end"],
                "coverage_complete": b["coverage_complete"],
            }
            for b in self.bids
        ]
        count = loader.load_nodes("Bid", bid_nodes, key_field="bid_id")
        logger.info("Loaded %d Bid nodes", count)

        # Ensure Company nodes exist for agencies
        agencies = deduplicate_rows(
            [
                {"cnpj": b["agency_cnpj"], "razao_social": b["agency_name"]}
                for b in self.bids
            ],
            ["cnpj"],
        )
        count = loader.load_nodes("Company", agencies, key_field="cnpj")
        logger.info("Merged %d Company (agency) nodes", count)

        # LICITOU: Company (agency) -> Bid
        rels = [
            {"source_key": b["agency_cnpj"], "target_key": b["bid_id"]}
            for b in self.bids
        ]
        count = loader.load_relationships(
            rel_type="LICITOU",
            rows=rels,
            source_label="Company",
            source_key="cnpj",
            target_label="Bid",
            target_key="bid_id",
        )
        logger.info("Created %d LICITOU relationships", count)

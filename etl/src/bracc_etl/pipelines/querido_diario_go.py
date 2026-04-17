from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_date,
    stable_id as _stable_id,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# NOTE: ``queridodiario.ok.org.br/api/`` serves the SPA frontend (HTML), not
# JSON. The canonical Querido Diário REST API is hosted on the ``api.``
# subdomain, as already used by ``etl/scripts/download_querido_diario.py``.
_API_BASE = "https://api.queridodiario.ok.org.br/"
_GAZETTE_ENDPOINT = "gazettes"
_CITIES_ENDPOINT = "cities"
_PAGE_SIZE = 100
_TIMEOUT = 30
# Polite throttle between per-territory requests when we have to fall back to
# looping (the public API is free and community-run).
_REQUEST_SLEEP_SECONDS = 0.3
# Goiás UF code — kept for reference / logging. Filtering is now done via
# per-municipality IBGE codes fetched from the ``/cities`` endpoint.
_GOIAS_STATE_CODE = "GO"

_CNPJ_RE = re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}")
_CPF_RE = re.compile(r"\d{3}\.\d{3}\.\d{3}-\d{2}")

_APPOINTMENT_KEYWORDS = ["nomear", "nomeação", "cargo comissionado", "exoneração"]
_APPOINTMENT_SEARCH = "|".join(_APPOINTMENT_KEYWORDS)

_ACT_TYPE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("nomeacao", re.compile(r"nomea[rç]|nomeação", re.IGNORECASE)),
    ("exoneracao", re.compile(r"exonera[rç]|exoneração", re.IGNORECASE)),
    ("contrato", re.compile(r"contrat[oa]|licitação|pregão", re.IGNORECASE)),
    ("licitacao", re.compile(r"licita[çc]|edital|pregão", re.IGNORECASE)),
]

_APPOINTMENT_NAME_RE = re.compile(
    r"(?:nomear|exonerar)\s+(?:(?:o|a|o\(a\))\s+(?:servidor|servidora)\s+)?"
    r"([A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ýa-zà-öø-ý]+(?:\s+[A-ZÀ-ÖØ-Ýa-zà-öø-ý]+){1,6})"
    r"\s+para\s+o\s+cargo\s+de\s+"
    r"([A-ZÀ-ÖØ-Ýa-zà-öø-ý /\-]+?)(?:\.|,|;|\n|$)",
    re.IGNORECASE,
)


def _classify_act(text: str) -> str:
    """Classify act type based on keyword matching."""
    for act_type, pattern in _ACT_TYPE_PATTERNS:
        if pattern.search(text):
            return act_type
    return "outro"


def _extract_cnpjs(text: str) -> list[tuple[str, str]]:
    """Extract CNPJ patterns from text, returning (formatted_cnpj, span)."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in _CNPJ_RE.finditer(text):
        raw = match.group(0)
        digits = strip_document(raw)
        if len(digits) != 14 or digits in seen:
            continue
        seen.add(digits)
        cnpj = format_cnpj(digits)
        span = f"{match.start()}:{match.end()}"
        out.append((cnpj, span))
    return out


def _extract_appointments(text: str) -> list[dict[str, str]]:
    """Extract appointment data (person name + role) from gazette text."""
    results: list[dict[str, str]] = []
    for match in _APPOINTMENT_NAME_RE.finditer(text):
        person_name = normalize_name(match.group(1).strip())
        role = match.group(2).strip()
        results.append({"person_name": person_name, "role": role})
    return results


def _fetch_goias_territory_ids(
    client: httpx.Client | None = None,
) -> list[str]:
    """Enumerate IBGE codes of Goiás municipalities from the Querido Diário API.

    The ``/cities`` endpoint returns the full nationwide registry (≈5570
    municipalities); ``state_code=GO`` is accepted as a query parameter but is
    **not** applied server-side, so filtering is performed client-side on the
    ``state_code`` field of each record.

    Returns a list of 7-digit IBGE codes (as strings) for use in
    ``territory_ids`` gazette queries. Raises ``httpx.HTTPError`` if the
    endpoint is unreachable or returns a non-2xx status — there is no silent
    fallback: callers must handle the exception explicitly.
    """
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=_TIMEOUT)
    try:
        resp = client.get(
            f"{_API_BASE}{_CITIES_ENDPOINT}",
            params={"state_code": _GOIAS_STATE_CODE},
        )
        resp.raise_for_status()
        payload = resp.json()
    finally:
        if owns_client:
            client.close()

    cities: list[dict[str, Any]]
    if isinstance(payload, dict):
        raw = payload.get("cities", [])
        cities = [c for c in raw if isinstance(c, dict)]
    elif isinstance(payload, list):
        cities = [c for c in payload if isinstance(c, dict)]
    else:
        cities = []

    ids: list[str] = []
    seen: set[str] = set()
    for city in cities:
        if city.get("state_code") != _GOIAS_STATE_CODE:
            continue
        ibge = str(city.get("territory_id", "")).strip()
        if not ibge or ibge in seen:
            continue
        seen.add(ibge)
        ids.append(ibge)

    if not ids:
        raise RuntimeError(
            "[querido_diario_go] /cities returned 0 Goiás municipalities — "
            "API schema may have changed; refusing to proceed.",
        )

    logger.info(
        "[querido_diario_go] discovered %d Goiás IBGE territory_ids via /cities",
        len(ids),
    )
    return ids


def _fetch_gazettes_for_territories(
    client: httpx.Client,
    territory_ids: list[str],
    keyword: str,
    remaining: int | None,
) -> list[dict[str, Any]]:
    """Page through gazettes for one keyword across the given territory_ids.

    The Querido Diário API accepts ``territory_ids`` as a **repeated** query
    parameter (CSV form returns 0 results — confirmed empirically). Results
    are merged server-side across all supplied territories.
    """
    collected: list[dict[str, Any]] = []
    offset = 0
    while remaining is None or len(collected) < remaining:
        # httpx serialises a list value as repeated ?k=v1&k=v2, which is what
        # the API expects.
        params: list[tuple[str, Any]] = [
            ("querystring", keyword),
            ("offset", offset),
            ("size", _PAGE_SIZE),
        ]
        params.extend(("territory_ids", tid) for tid in territory_ids)

        try:
            resp = client.get(
                f"{_API_BASE}{_GAZETTE_ENDPOINT}",
                params=params,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "[querido_diario_go] API error for keyword=%s offset=%d: %s",
                keyword,
                offset,
                exc,
            )
            break

        data = resp.json()
        gazettes = data.get("gazettes", [])
        if not gazettes:
            break

        collected.extend(gazettes)
        offset += len(gazettes)

        if len(gazettes) < _PAGE_SIZE:
            break

        time.sleep(_REQUEST_SLEEP_SECONDS)

    return collected


def fetch_gazettes(limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch Goiás gazette entries from the Querido Diário public API.

    First enumerates every Goiás IBGE municipality code via ``/cities``, then
    issues one paginated query per appointment keyword passing **all** GO
    territory IDs as repeated ``territory_ids`` params (the API merges the
    results server-side). Results are capped by ``limit`` if provided and
    de-duplicated on ``(territory_id, date, edition, url)``. Pure network
    operation — no filesystem side-effects.
    """
    total_limit = limit
    records: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str, str]] = set()

    with httpx.Client(timeout=_TIMEOUT) as client:
        territory_ids = _fetch_goias_territory_ids(client=client)

        for keyword in _APPOINTMENT_KEYWORDS:
            remaining = (
                None if total_limit is None else max(0, total_limit - len(records))
            )
            if remaining == 0:
                break

            batch = _fetch_gazettes_for_territories(
                client=client,
                territory_ids=territory_ids,
                keyword=keyword,
                remaining=remaining,
            )

            for gazette in batch:
                key = (
                    str(gazette.get("territory_id", "")),
                    str(gazette.get("date", "")),
                    str(gazette.get("edition", "")),
                    str(gazette.get("url", "")),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                records.append(gazette)
                if total_limit is not None and len(records) >= total_limit:
                    break

            time.sleep(_REQUEST_SLEEP_SECONDS)

    return records[: int(total_limit)] if total_limit is not None else records


def fetch_to_disk(output_dir: Path, limit: int | None = None) -> list[Path]:
    """Fetch Goiás gazettes from Querido Diário and persist to ``output_dir``.

    Writes one JSON file per keyword batch using the canonical envelope
    ``{"gazettes": [...]}`` consumed by :meth:`QueridoDiarioGoPipeline._read_local_files`.
    The resulting layout matches what the ``file_manifest`` acquisition mode
    expects under ``data/querido_diario_go/``.

    Returns the list of files written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    records = fetch_gazettes(limit=limit)
    if not records:
        logger.warning("[querido_diario_go] API returned no records")
        return []

    # Group by keyword heuristic: the API response does not echo the query, so
    # we just write a single consolidated file. The loader accepts both list
    # and {"gazettes": [...]} envelopes.
    out_path = output_dir / "gazettes.json"
    out_path.write_text(
        json.dumps({"gazettes": records}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(
        "[querido_diario_go] wrote %d gazette records to %s", len(records), out_path,
    )
    return [out_path]


class QueridoDiarioGoPipeline(Pipeline):
    """ETL pipeline for Goiás municipal gazette data from Querido Diário API."""

    name = "querido_diario_go"
    source_id = "querido_diario_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_gazettes: list[dict[str, Any]] = []
        self.acts: list[dict[str, Any]] = []
        self.appointments: list[dict[str, Any]] = []
        self.company_mentions: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

    def _fetch_from_api(self) -> list[dict[str, Any]]:
        """Fetch gazette entries from Querido Diário API for Goiás municipalities.

        Thin wrapper around :func:`fetch_gazettes` so the logic stays reusable
        from ``scripts/download_querido_diario_go.py``.
        """
        return fetch_gazettes(limit=self.limit)

    def _read_local_files(self) -> list[dict[str, Any]]:
        """Read gazette data from local JSON files as fallback."""
        src_dir = Path(self.data_dir) / "querido_diario_go"
        if not src_dir.exists():
            return []

        records: list[dict[str, Any]] = []

        for json_file in sorted(src_dir.glob("*.json")):
            try:
                payload = json.loads(json_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.warning("[querido_diario_go] invalid JSON: %s", json_file)
                continue

            if isinstance(payload, list):
                records.extend(r for r in payload if isinstance(r, dict))
            elif isinstance(payload, dict):
                gazettes = payload.get("gazettes", payload.get("results", []))
                if isinstance(gazettes, list):
                    records.extend(r for r in gazettes if isinstance(r, dict))

        for jsonl_file in sorted(src_dir.glob("*.jsonl")):
            for line in jsonl_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    records.append(row)

        return records

    def extract(self) -> None:
        # Try local files first (offline / cached mode)
        records = self._read_local_files()

        if not records:
            try:
                records = self._fetch_from_api()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[querido_diario_go] API fetch failed: %s", exc)

        if self.limit:
            records = records[: self.limit]

        self._raw_gazettes = records
        self.rows_in = len(records)
        logger.info("[querido_diario_go] extracted %d gazette entries", len(records))

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        if not self._raw_gazettes:
            return

        acts: list[dict[str, Any]] = []
        appointments: list[dict[str, Any]] = []
        mentions: list[dict[str, Any]] = []

        for row in self._raw_gazettes:
            territory_id = str(row.get("territory_id", "")).strip()
            territory_name = str(row.get("territory_name", "")).strip()
            date = parse_date(str(row.get("date", "")))
            edition = str(row.get("edition", "")).strip()
            is_extra = bool(row.get("is_extra_edition", False))
            text = str(row.get("excerpts", "") or row.get("excerto", "") or "").strip()
            url = str(row.get("url", "")).strip()

            if not text and not territory_id:
                continue

            act_type = _classify_act(text)
            excerpt = text[:500] if text else ""

            act_id = _stable_id(territory_id, date, edition, text[:180])

            acts.append({
                "act_id": act_id,
                "territory_id": territory_id,
                "territory_name": territory_name,
                "date": date,
                "edition": edition,
                "is_extra_edition": is_extra,
                "excerpt": excerpt,
                "url": url,
                "act_type": act_type,
                "uf": "GO",
                "source": "querido_diario_go",
            })

            # Extract CNPJ mentions
            for cnpj, span in _extract_cnpjs(text):
                mentions.append({
                    "cnpj": cnpj,
                    "target_key": act_id,
                    "method": "text_cnpj_extract",
                    "confidence": 0.75,
                    "source_ref": url or act_id,
                    "extract_span": span,
                    "run_id": self.run_id,
                })

            # Extract appointment data
            if act_type in ("nomeacao", "exoneracao"):
                appointment_type = act_type
            elif re.search(r"exonera", text, re.IGNORECASE):
                appointment_type = "exoneracao"
            elif re.search(r"nomea", text, re.IGNORECASE):
                appointment_type = "nomeacao"
            else:
                appointment_type = None

            if appointment_type:
                extracted = _extract_appointments(text)
                for appt in extracted:
                    appt_id = _stable_id(
                        territory_id,
                        date,
                        appt["person_name"],
                        appt["role"],
                    )
                    appointments.append({
                        "appointment_id": appt_id,
                        "person_name": appt["person_name"],
                        "role": appt["role"],
                        "agency": "",
                        "act_date": date,
                        "appointment_type": appointment_type,
                        "territory_id": territory_id,
                        "territory_name": territory_name,
                        "uf": "GO",
                        "source": "querido_diario_go",
                        "act_id": act_id,
                    })

        self.acts = deduplicate_rows(acts, ["act_id"])
        self.appointments = deduplicate_rows(appointments, ["appointment_id"])
        self.company_mentions = deduplicate_rows(
            mentions,
            ["cnpj", "target_key", "method", "extract_span"],
        )

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.acts:
            loader.load_nodes("GoGazetteAct", self.acts, key_field="act_id")

        if self.appointments:
            # Load appointment nodes (without the act_id FK — that goes in the rel)
            appt_nodes = [
                {k: v for k, v in row.items() if k != "act_id"}
                for row in self.appointments
            ]
            loader.load_nodes("GoAppointment", appt_nodes, key_field="appointment_id")

            # Create PUBLICADO_EM relationships
            appt_rels = [
                {"appointment_id": row["appointment_id"], "act_id": row["act_id"]}
                for row in self.appointments
            ]
            rel_query = (
                "UNWIND $rows AS row "
                "MATCH (a:GoAppointment {appointment_id: row.appointment_id}) "
                "MATCH (g:GoGazetteAct {act_id: row.act_id}) "
                "MERGE (a)-[:PUBLICADO_EM]->(g)"
            )
            loader.run_query_with_retry(rel_query, appt_rels)

        if self.company_mentions:
            companies = deduplicate_rows(
                [
                    {"cnpj": row["cnpj"], "razao_social": row["cnpj"]}
                    for row in self.company_mentions
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", companies, key_field="cnpj")

            mention_query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.cnpj}) "
                "MATCH (a:GoGazetteAct {act_id: row.target_key}) "
                "MERGE (c)-[m:MENCIONADA_EM_GO]->(a) "
                "SET m.method = row.method, "
                "m.confidence = row.confidence, "
                "m.source_ref = row.source_ref, "
                "m.extract_span = row.extract_span, "
                "m.run_id = row.run_id"
            )
            loader.run_query_with_retry(mention_query, self.company_mentions)

        self.rows_loaded = len(self.acts) + len(self.appointments)

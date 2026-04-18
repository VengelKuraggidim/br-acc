from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    extract_cnpjs,
    format_cnpj,
    format_cpf,
    normalize_name,
    parse_date,
    row_pick,
    strip_document,
)
from bracc_etl.transforms import (
    stable_id as _stable_id,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# fetch_to_disk — public Camara API v2 path (no credentials required).
# --------------------------------------------------------------------------
# The ``/api/v2/orgaos?sigla=CPI|CPMI`` endpoints enumerate every CPI/CPMI
# the Camara dos Deputados has ever registered. Per-orgao ``/eventos`` gives
# historical session metadata. Full requirement (proposicoes) coverage
# still depends on Base dos Dados BigQuery tables ``br_camara_dados_abertos``
# (``proposicao_autor``/``proposicao_microdados``/``evento_requerimento``),
# so the API-only path returns an empty requirements list — the ETL
# pipeline already tolerates that and ``transform`` remains a no-op for
# requirements when the CSV is empty. Operators with a GCP billing project
# can still run ``etl/scripts/download_camara_inquiries.py --mode bq_first``
# for the fuller historical extract; this module-level helper is the
# credential-free default wired into the bootstrap contract.

_CAMARA_API_BASE = "https://dadosabertos.camara.leg.br/api/v2"
_CAMARA_HTTP_TIMEOUT = 60.0
_CAMARA_DEFAULT_HEADERS = {"Accept": "application/json"}
_CAMARA_EVENT_PAGE_SIZE = 200


def _camara_request_json(
    client: httpx.Client,
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        resp = client.get(url, params=params, timeout=_CAMARA_HTTP_TIMEOUT)
    except httpx.HTTPError as exc:
        logger.warning("[camara_inquiries] HTTP error on %s: %s", url, exc)
        return {}
    if resp.status_code != 200:
        logger.warning(
            "[camara_inquiries] non-200 (%d) on %s", resp.status_code, url,
        )
        return {}
    try:
        payload = resp.json()
    except ValueError as exc:
        logger.warning("[camara_inquiries] non-JSON on %s: %s", url, exc)
        return {}
    return payload if isinstance(payload, dict) else {}


def _camara_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    dados = payload.get("dados")
    if isinstance(dados, list):
        return [x for x in dados if isinstance(x, dict)]
    return []


def _camara_parse_date(raw: Any) -> str:
    if raw is None:
        return ""
    text = str(raw).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return ""


def _camara_dedupe(
    rows: list[dict[str, Any]], key: str,
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        value = str(row.get(key, "")).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(row)
    return out


def _camara_write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    """Write ``rows`` to ``path`` matching the shape CamaraInquiriesPipeline reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return path
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _fetch_camara_inquiries_api(
    client: httpx.Client,
    limit: int | None = None,
    max_events_per_orgao: int = _CAMARA_EVENT_PAGE_SIZE,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect inquiries + sessions from the public Camara v2 API.

    Returns (inquiries, sessions). Requirements stay empty — the v2 API does
    not expose event<->proposicao joins that BigQuery does.
    """
    inquiries: list[dict[str, Any]] = []
    sessions: list[dict[str, Any]] = []

    orgaos: list[dict[str, Any]] = []
    for sigla in ("CPI", "CPMI"):
        payload = _camara_request_json(
            client,
            f"{_CAMARA_API_BASE}/orgaos",
            {"sigla": sigla, "itens": 100},
        )
        orgaos.extend(_camara_items(payload))

    logger.info(
        "[camara_inquiries] /orgaos CPI+CPMI candidates: %d", len(orgaos),
    )

    seen: set[str] = set()
    for orgao in orgaos:
        orgao_id = str(orgao.get("id", "")).strip()
        if not orgao_id or orgao_id in seen:
            continue
        seen.add(orgao_id)

        sigla = str(orgao.get("sigla", "")).strip()
        nome = str(
            orgao.get("nomePublicacao") or orgao.get("nome") or "",
        ).strip()
        if "CPI" not in sigla.upper() and "CPI" not in nome.upper():
            continue

        inquiry_id = f"camara-{orgao_id}"
        inquiry_url = f"{_CAMARA_API_BASE}/orgaos/{orgao_id}"
        kind = "CPMI" if "CPMI" in (sigla or nome).upper() else "CPI"

        detail = _camara_request_json(client, inquiry_url)
        dado_block = detail.get("dados") if isinstance(detail, dict) else {}
        dado = dado_block if isinstance(dado_block, dict) else {}
        inquiries.append({
            "inquiry_id": inquiry_id,
            "inquiry_code": sigla,
            "name": nome,
            "kind": kind,
            "house": "congresso" if kind == "CPMI" else "camara",
            "status": str(dado.get("situacao") or "").strip(),
            "subject": str(dado.get("descricao") or "").strip(),
            "date_start": _camara_parse_date(dado.get("dataInicio")),
            "date_end": _camara_parse_date(dado.get("dataFim")),
            "source_url": inquiry_url,
            "source_system": "camara_api",
            "extraction_method": "orgaos_sigla",
        })

        if limit is not None and len(inquiries) >= limit:
            break

        eventos_payload = _camara_request_json(
            client,
            f"{_CAMARA_API_BASE}/orgaos/{orgao_id}/eventos",
            {"itens": max_events_per_orgao},
        )
        for event in _camara_items(eventos_payload):
            event_id = str(event.get("id", "")).strip()
            if not event_id:
                continue
            sessions.append({
                "session_id": f"camara-event-{event_id}",
                "inquiry_id": inquiry_id,
                "date": _camara_parse_date(event.get("dataHoraInicio")),
                "topic": str(
                    event.get("descricaoTipo") or event.get("titulo") or "",
                ).strip(),
                "source_url": str(event.get("uri") or inquiry_url),
                "source_system": "camara_api",
                "extraction_method": "orgaos_eventos",
            })

    return (
        _camara_dedupe(inquiries, "inquiry_id"),
        _camara_dedupe(sessions, "session_id"),
    )


def fetch_to_disk(
    output_dir: Path | str,
    *,
    date: str | None = None,  # noqa: ARG001 (unused — kept for registry signature)
    limit: int | None = None,
    timeout: float = _CAMARA_HTTP_TIMEOUT,
) -> list[Path]:
    """Download Camara CPI/CPMI metadata + sessions to ``output_dir``.

    Writes the three canonical files the ETL pipeline reads:

    * ``inquiries.csv`` — one row per CPI/CPMI in the
      ``/api/v2/orgaos?sigla=CPI|CPMI`` catalog.
    * ``requirements.csv`` — intentionally empty from this path (requires
      BigQuery to join ``evento_requerimento`` -> ``proposicao_*``).
      ``CamaraInquiriesPipeline.extract`` treats missing/empty files as
      "no rows" and short-circuits the requirements transform.
    * ``sessions.csv`` — one row per ``/orgaos/{id}/eventos`` entry for
      each detected CPI/CPMI.

    Args:
        output_dir: Directory to write the three CSVs into. Created if
            missing.
        date: Unused — accepted for contract-signature parity with
            time-scoped sources (Camara's registry is cumulative).
        limit: Optional cap on the number of inquiries probed (useful for
            smoke tests). ``None`` pulls the full catalog.
        timeout: Per-request HTTP timeout in seconds.

    Returns:
        Sorted list of files written (always includes ``inquiries.csv``
        plus ``requirements.csv`` and ``sessions.csv`` — empty files are
        still returned so downstream checks can assert presence).
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    with httpx.Client(
        headers=_CAMARA_DEFAULT_HEADERS,
        follow_redirects=True,
        timeout=timeout,
    ) as client:
        inquiries, sessions = _fetch_camara_inquiries_api(
            client=client, limit=limit,
        )

    written = [
        _camara_write_csv(output_path / "inquiries.csv", inquiries),
        _camara_write_csv(output_path / "requirements.csv", []),
        _camara_write_csv(output_path / "sessions.csv", sessions),
    ]

    logger.info(
        "[camara_inquiries] fetch_to_disk wrote %d inquiries, %d sessions "
        "(requirements left empty — BQ path needed for historical reqs)",
        len(inquiries), len(sessions),
    )
    return sorted(written)


class CamaraInquiriesPipeline(Pipeline):
    """ETL pipeline for Câmara CPI/CPMI inquiry metadata and requirements."""

    name = "camara_inquiries"
    source_id = "camara_inquiries"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)

        self._raw_inquiries: pd.DataFrame = pd.DataFrame()
        self._raw_requirements: pd.DataFrame = pd.DataFrame()
        self._raw_sessions: pd.DataFrame = pd.DataFrame()

        self.inquiries: list[dict[str, Any]] = []
        self.requirements: list[dict[str, Any]] = []
        self.sessions: list[dict[str, Any]] = []
        self.inquiry_requirement_rels: list[dict[str, Any]] = []
        self.inquiry_session_rels: list[dict[str, Any]] = []
        self.requirement_author_cpf_rels: list[dict[str, Any]] = []
        self.requirement_author_name_rels: list[dict[str, Any]] = []
        self.requirement_company_mentions: list[dict[str, Any]] = []

        self.run_id = f"{self.name}_{pd.Timestamp.utcnow().strftime('%Y%m%d%H%M%S')}"

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False)
        except pd.errors.EmptyDataError:
            logger.info("[camara_inquiries] empty file (treated as no data): %s", path.name)
            return pd.DataFrame()


    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "camara_inquiries"
        self._raw_inquiries = self._read_csv_optional(src_dir / "inquiries.csv")
        self._raw_requirements = self._read_csv_optional(src_dir / "requirements.csv")
        self._raw_sessions = self._read_csv_optional(src_dir / "sessions.csv")

        if self._raw_inquiries.empty:
            logger.warning("[camara_inquiries] inquiries.csv not found/empty in %s", src_dir)
            return

        if self.limit:
            self._raw_inquiries = self._raw_inquiries.head(self.limit)

        logger.info(
            "[camara_inquiries] extracted inquiries=%d requirements=%d sessions=%d",
            len(self._raw_inquiries),
            len(self._raw_requirements),
            len(self._raw_sessions),
        )

    def transform(self) -> None:
        if self._raw_inquiries.empty:
            return

        self._transform_inquiries()
        self._transform_requirements()
        self._transform_sessions()

    def _transform_inquiries(self) -> None:
        rows: list[dict[str, Any]] = []

        for _, row in self._raw_inquiries.iterrows():
            inquiry_id = row_pick(row, "inquiry_id", "id")
            code = row_pick(row, "inquiry_code", "codigo")
            name = row_pick(row, "name", "titulo", "nome")
            if not name:
                continue

            if not inquiry_id:
                inquiry_id = _stable_id(code, name, length=20)

            kind = row_pick(row, "kind", "tipo").upper()
            if not kind:
                kind = "CPMI" if "CPMI" in name.upper() else "CPI"
            status = row_pick(row, "status", "situacao")
            subject = row_pick(row, "subject", "objeto")
            source_url = row_pick(row, "source_url", "url")
            source_system = row_pick(row, "source_system")
            extraction_method = row_pick(row, "extraction_method")
            date_start = parse_date(row_pick(row, "date_start", "data_inicio"))
            date_end = parse_date(row_pick(row, "date_end", "data_fim"))

            rows.append({
                "inquiry_id": inquiry_id,
                "code": code,
                "name": name,
                "kind": kind,
                "house": "camara",
                "status": status,
                "subject": subject,
                "date_start": date_start,
                "date_end": date_end,
                "source_url": source_url,
                "source": "camara_inquiries",
                "source_system": source_system,
                "extraction_method": extraction_method,
            })

        self.inquiries = deduplicate_rows(rows, ["inquiry_id"])

    def _transform_requirements(self) -> None:
        if self._raw_requirements.empty:
            return

        requirements: list[dict[str, Any]] = []
        inquiry_rels: list[dict[str, Any]] = []
        author_cpf_rels: list[dict[str, Any]] = []
        author_name_rels: list[dict[str, Any]] = []
        mentions: list[dict[str, Any]] = []

        for _, row in self._raw_requirements.iterrows():
            inquiry_id = row_pick(row, "inquiry_id")
            if not inquiry_id:
                continue

            requirement_id = row_pick(row, "requirement_id", "id", "codigo")
            req_type = row_pick(row, "type", "tipo")
            text = row_pick(row, "text", "texto", "ementa")
            status = row_pick(row, "status", "situacao")
            source_url = row_pick(row, "source_url", "url")
            source_system = row_pick(row, "source_system")
            extraction_method = row_pick(row, "extraction_method")
            date = parse_date(row_pick(row, "date", "data"))

            if not requirement_id:
                requirement_id = _stable_id(inquiry_id, req_type, text[:200], length=20)

            requirements.append({
                "requirement_id": requirement_id,
                "type": req_type,
                "date": date,
                "text": text,
                "status": status,
                "source_url": source_url,
                "source": "camara_inquiries",
                "source_system": source_system,
                "extraction_method": extraction_method,
            })

            inquiry_rels.append({"source_key": inquiry_id, "target_key": requirement_id})

            author_name = normalize_name(row_pick(row, "author_name", "autor"))
            author_cpf_raw = row_pick(row, "author_cpf", "cpf_autor")
            author_digits = strip_document(author_cpf_raw)
            if len(author_digits) == 11:
                author_cpf_rels.append(
                    {"source_key": format_cpf(author_digits), "target_key": requirement_id},
                )
            elif author_name:
                author_name_rels.append(
                    {"person_name": author_name, "target_key": requirement_id},
                )

            explicit_mentioned = row_pick(row, "mentioned_cnpj", "cnpj")
            explicit_digits = strip_document(explicit_mentioned)
            if len(explicit_digits) == 14:
                mentions.append({
                    "cnpj": format_cnpj(explicit_digits),
                    "target_key": requirement_id,
                    "method": "cnpj_explicit",
                    "confidence": 1.0,
                    "source_ref": source_url or requirement_id,
                    "run_id": self.run_id,
                })

            for cnpj in extract_cnpjs(text):
                mentions.append({
                    "cnpj": cnpj,
                    "target_key": requirement_id,
                    "method": "text_cnpj_extract",
                    "confidence": 0.8,
                    "source_ref": source_url or requirement_id,
                    "run_id": self.run_id,
                })

        self.requirements = deduplicate_rows(requirements, ["requirement_id"])
        self.inquiry_requirement_rels = inquiry_rels
        self.requirement_author_cpf_rels = author_cpf_rels
        self.requirement_author_name_rels = author_name_rels
        self.requirement_company_mentions = deduplicate_rows(
            mentions,
            ["cnpj", "target_key", "method"],
        )

    def _transform_sessions(self) -> None:
        if self._raw_sessions.empty:
            return

        sessions: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        for _, row in self._raw_sessions.iterrows():
            inquiry_id = row_pick(row, "inquiry_id")
            if not inquiry_id:
                continue

            session_id = row_pick(row, "session_id", "id")
            date = parse_date(row_pick(row, "date", "data"))
            topic = row_pick(row, "topic", "assunto")
            source_url = row_pick(row, "source_url", "url")
            source_system = row_pick(row, "source_system")
            extraction_method = row_pick(row, "extraction_method")

            if not session_id:
                session_id = _stable_id(inquiry_id, date, topic[:200], length=20)

            sessions.append({
                "session_id": session_id,
                "date": date,
                "topic": topic,
                "source_url": source_url,
                "source": "camara_inquiries",
                "source_system": source_system,
                "extraction_method": extraction_method,
            })
            rels.append({"source_key": inquiry_id, "target_key": session_id})

        self.sessions = deduplicate_rows(sessions, ["session_id"])
        self.inquiry_session_rels = rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.inquiries:
            loader.load_nodes("Inquiry", self.inquiries, key_field="inquiry_id")

        if self.requirements:
            loader.load_nodes("InquiryRequirement", self.requirements, key_field="requirement_id")

        if self.sessions:
            loader.load_nodes("InquirySession", self.sessions, key_field="session_id")

        if self.inquiry_requirement_rels:
            loader.load_relationships(
                rel_type="TEM_REQUERIMENTO",
                rows=self.inquiry_requirement_rels,
                source_label="Inquiry",
                source_key="inquiry_id",
                target_label="InquiryRequirement",
                target_key="requirement_id",
            )

        if self.inquiry_session_rels:
            loader.load_relationships(
                rel_type="REALIZOU_SESSAO",
                rows=self.inquiry_session_rels,
                source_label="Inquiry",
                source_key="inquiry_id",
                target_label="InquirySession",
                target_key="session_id",
            )

        if self.requirement_author_cpf_rels:
            loader.load_relationships(
                rel_type="PROPOS_REQUERIMENTO",
                rows=self.requirement_author_cpf_rels,
                source_label="Person",
                source_key="cpf",
                target_label="InquiryRequirement",
                target_key="requirement_id",
            )

        if self.requirement_author_name_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person) WHERE p.name = row.person_name "
                "MATCH (r:InquiryRequirement {requirement_id: row.target_key}) "
                "MERGE (p)-[:PROPOS_REQUERIMENTO]->(r)"
            )
            loader.run_query_with_retry(query, self.requirement_author_name_rels)

        if self.requirement_company_mentions:
            companies = deduplicate_rows(
                [
                    {"cnpj": row["cnpj"], "razao_social": row["cnpj"]}
                    for row in self.requirement_company_mentions
                ],
                ["cnpj"],
            )
            loader.load_nodes("Company", companies, key_field="cnpj")

            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.cnpj}) "
                "MATCH (r:InquiryRequirement {requirement_id: row.target_key}) "
                "MERGE (c)-[m:MENCIONADA_EM]->(r) "
                "SET m.method = row.method, "
                "m.confidence = row.confidence, "
                "m.source_ref = row.source_ref, "
                "m.run_id = row.run_id"
            )
            loader.run_query_with_retry(query, self.requirement_company_mentions)

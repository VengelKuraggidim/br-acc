from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import defusedxml.ElementTree as ET  # type: ignore[import-untyped]  # noqa: N817
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

_TEMPORAL_RULE = (
    "event_date>=inquiry.date_start and "
    "(inquiry.date_end is null or event_date<=inquiry.date_end)"
)


def _make_cpi_id(code: str, name: str) -> str:
    """Deterministic CPI ID for backward compatibility."""
    return _stable_id(code, name, length=16)


def _infer_kind(name: str, explicit_kind: str = "") -> str:
    kind = explicit_kind.strip().upper()
    if kind in {"CPI", "CPMI"}:
        return kind
    if "CPMI" in name.upper():
        return "CPMI"
    return "CPI"


def _temporal_status(event_date: str, start_date: str, end_date: str) -> str:
    if not event_date or not start_date:
        return "unknown"
    if event_date < start_date:
        return "invalid"
    if end_date and event_date > end_date:
        return "invalid"
    return "valid"


# --------------------------------------------------------------------------
# fetch_to_disk — Senado Open Data (unauthenticated XML endpoints).
# --------------------------------------------------------------------------
# Active-commission coverage comes from
# ``https://legis.senado.leg.br/dadosabertos/comissao/lista/{CPI|CPMI}``
# (returns XML). For each commission, per-sigla requirements live at
# ``/comissao/cpi/{sigla}/requerimentos`` (returns JSON). Historical
# (pre-Open-Data) commissions need the PDF archive path implemented in
# ``etl/scripts/download_senado_cpi_archive.py`` — that remains a separate
# CLI because it requires optional PDF-parsing deps and is brittle enough
# that a smoke run shouldn't depend on it. This helper covers the
# always-available, credential-free slice; operators who want the 1946-2015
# archive run the archive CLI in addition.

_SENADO_OPEN_DATA = "https://legis.senado.leg.br/dadosabertos"
_SENADO_HTTP_TIMEOUT = 60.0
_SENADO_REQ_PAGE_SIZE = 20  # endpoint rejects larger values


def _senado_slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "unknown"


def _senado_make_inquiry_id(kind: str, code: str, sigla: str, name: str) -> str:
    anchor = sigla or code or name
    return f"senado-{_senado_slugify(kind)}-{_senado_slugify(anchor)}"


def _senado_parse_date(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]
    if len(raw) >= 10 and raw[2] == "/" and raw[5] == "/":
        try:
            import datetime as _dt
            return _dt.datetime.strptime(raw[:10], "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def _senado_text(node: Any) -> str:
    if node is None:
        return ""
    text = getattr(node, "text", "")
    return (text or "").strip()


def _senado_dedupe(
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


def _senado_fetch_active_inquiries(
    client: httpx.Client,
    timeout: float,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Parse the CPI/CPMI active-commission XML endpoint.

    Returns (inquiry rows, sigla -> inquiry_id map).
    """
    inquiries: list[dict[str, Any]] = []
    sigla_to_inquiry_id: dict[str, str] = {}

    for kind in ("CPI", "CPMI"):
        url = f"{_SENADO_OPEN_DATA}/comissao/lista/{kind}"
        try:
            resp = client.get(url, timeout=timeout)
            resp.raise_for_status()
            raw = resp.content
            xml_start = raw.find(b"<")
            if xml_start > 0:
                raw = raw[xml_start:]
            root = ET.fromstring(raw)
        except (httpx.HTTPError, ET.ParseError) as exc:
            logger.warning(
                "[senado_cpis] active-inquiries endpoint failed for kind=%s: %s",
                kind, exc,
            )
            continue

        colegiados = root.findall(".//Colegiado") + root.findall(".//colegiado")
        for com in colegiados:
            code = _senado_text(com.find("CodigoColegiado")) or _senado_text(
                com.find("Codigo"),
            )
            sigla = _senado_text(com.find("SiglaColegiado")) or _senado_text(
                com.find("Sigla"),
            )
            name = _senado_text(com.find("NomeColegiado")) or _senado_text(
                com.find("Nome"),
            )
            if not name:
                continue

            inquiry_id = _senado_make_inquiry_id(kind, code, sigla, name)
            if sigla:
                sigla_to_inquiry_id[sigla.upper()] = inquiry_id

            inquiries.append({
                "inquiry_id": inquiry_id,
                "inquiry_code": code or sigla,
                "name": name,
                "kind": kind,
                "house": "congresso" if kind == "CPMI" else "senado",
                "status": "em atividade",
                "subject": (
                    _senado_text(com.find("TextoFinalidade"))
                    or _senado_text(com.find("DescricaoSubtitulo"))
                ),
                "date_start": _senado_parse_date(
                    _senado_text(com.find("DataInicio")),
                ),
                "date_end": _senado_parse_date(_senado_text(com.find("DataFim"))),
                "source_url": url,
                "source_system": "senado_open_data",
                "extraction_method": "comissao_lista_tipo",
                "source_ref": sigla or code,
                "date_precision": "day",
            })

    return _senado_dedupe(inquiries, "inquiry_id"), sigla_to_inquiry_id


def _senado_fetch_requirements_for_sigla(
    client: httpx.Client,
    sigla: str,
    inquiry_id: str,
    max_pages: int,
    run_id: str,
    timeout: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    endpoint = f"{_SENADO_OPEN_DATA}/comissao/cpi/{sigla}/requerimentos"

    for page in range(max_pages):
        params = {"pagina": page, "tamanho": _SENADO_REQ_PAGE_SIZE}
        try:
            resp = client.get(endpoint, params=params, timeout=timeout)
        except httpx.HTTPError as exc:
            logger.warning(
                "[senado_cpis] requerimentos endpoint failed sigla=%s: %s",
                sigla, exc,
            )
            break
        if resp.status_code in (400, 404):
            break
        if resp.status_code != 200 or not resp.content.strip():
            break
        try:
            payload = resp.json()
        except ValueError:
            logger.warning(
                "[senado_cpis] non-JSON requirements page sigla=%s page=%d",
                sigla, page,
            )
            break
        if not isinstance(payload, list) or not payload:
            break

        for req in payload:
            if not isinstance(req, dict):
                continue
            code = str(req.get("codigo", "")).strip()
            number = str(req.get("numero", "")).strip()
            year = str(req.get("ano", "")).strip()
            requirement_id = (
                f"senado-req-{_senado_slugify(sigla)}-{_senado_slugify(number)}-"
                f"{year or 'na'}-{code or 'na'}"
            )
            author_raw = req.get("autor")
            author_obj: dict[str, Any] = author_raw if isinstance(author_raw, dict) else {}
            author_name = (
                str(author_obj.get("nomeParlamentar", "")).strip()
                or str(author_obj.get("nome", "")).strip()
                or str(req.get("autoria", "")).strip()
            )
            doc_raw = req.get("documento")
            doc_obj: dict[str, Any] = doc_raw if isinstance(doc_raw, dict) else {}
            source_ref = str(doc_obj.get("linkDownload", "")).strip() if doc_obj else ""
            date_value = _senado_parse_date(
                str(req.get("dataApresentacao", "")).strip()
                or str(req.get("dataApreciacao", "")).strip(),
            )
            rows.append({
                "requirement_id": requirement_id,
                "inquiry_id": inquiry_id,
                "type": str(req.get("tipoRequerimento", "")).strip() or "REQUERIMENTO",
                "date": date_value,
                "text": (
                    str(req.get("ementa", "")).strip()
                    or str(req.get("assunto", "")).strip()
                ),
                "status": str(req.get("situacao", "")).strip(),
                "author_name": author_name,
                "author_cpf": "",
                "source_url": source_ref or endpoint,
                "source_system": "senado_open_data",
                "extraction_method": "comissao_cpi_requerimentos",
                "source_ref": code or number or sigla,
                "date_precision": "day" if date_value else "unknown",
                "run_id": run_id,
            })

        if len(payload) < _SENADO_REQ_PAGE_SIZE:
            break

    return rows


def _senado_write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    """Write ``rows`` to ``path`` matching SenadoCpisPipeline reader layout."""
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


def fetch_to_disk(
    output_dir: Path | str,
    *,
    date: str | None = None,  # noqa: ARG001 (unused — contract signature parity)
    limit: int | None = None,
    max_pages: int = 20,
    timeout: float = _SENADO_HTTP_TIMEOUT,
) -> list[Path]:
    """Download Senate CPI/CPMI metadata + requirements to ``output_dir``.

    Writes the canonical files consumed by ``SenadoCpisPipeline.extract``:

    * ``inquiries.csv`` — active CPIs/CPMIs from
      ``/dadosabertos/comissao/lista/{CPI|CPMI}``.
    * ``requirements.csv`` — per-sigla requirements from
      ``/comissao/cpi/{sigla}/requerimentos``.
    * ``sessions.csv`` — empty from this endpoint set (no reunião metadata
      exposed; historical sessions require the archive PDF path).
    * ``members.csv`` — empty (members appear in the richer BigQuery
      ``br_senado_federal_dados_abertos`` dataset, not Open Data).
    * ``history_sources.csv`` — empty (populated by the archive CLI when
      the operator runs it in addition).

    The empty CSVs are still written so the pipeline's ``_read_csv_optional``
    sees consistent paths and downstream presence checks pass.

    Args:
        output_dir: Directory to write the CSVs into. Created if missing.
        date: Accepted for signature parity; the Senado registry is
            cumulative (no date-window filter).
        limit: Optional cap on the number of commissions probed for
            requirements (useful for smoke tests). ``None`` = all.
        max_pages: Per-sigla pagination ceiling for requirements
            (``REQ_PAGE_SIZE``=20 per page).
        timeout: Per-request HTTP timeout in seconds.

    Returns:
        Sorted list of files written (5 entries, some may be empty).
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    run_id = f"senado_cpis_{pd.Timestamp.utcnow().strftime('%Y%m%d%H%M%S')}"

    inquiries: list[dict[str, Any]] = []
    requirements: list[dict[str, Any]] = []

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        inquiries, sigla_map = _senado_fetch_active_inquiries(
            client=client, timeout=timeout,
        )

        iterator: list[tuple[str, str]] = sorted(sigla_map.items())
        if limit is not None:
            iterator = iterator[:limit]

        for sigla, inquiry_id in iterator:
            requirements.extend(
                _senado_fetch_requirements_for_sigla(
                    client=client,
                    sigla=sigla,
                    inquiry_id=inquiry_id,
                    max_pages=max_pages,
                    run_id=run_id,
                    timeout=timeout,
                ),
            )

    requirements = _senado_dedupe(requirements, "requirement_id")

    written = [
        _senado_write_csv(output_path / "inquiries.csv", inquiries),
        _senado_write_csv(output_path / "requirements.csv", requirements),
        _senado_write_csv(output_path / "sessions.csv", []),
        _senado_write_csv(output_path / "members.csv", []),
        _senado_write_csv(output_path / "history_sources.csv", []),
    ]

    logger.info(
        "[senado_cpis] fetch_to_disk wrote %d inquiries, %d requirements "
        "(sessions/members/history_sources empty — archive CLI needed for "
        "historical PDF coverage)",
        len(inquiries), len(requirements),
    )
    return sorted(written)


class SenadoCpisPipeline(Pipeline):
    """ETL pipeline for Senate inquiries (CPI/CPMI), v2.

    Input directory: data/senado_cpis/

    Supported files:
    - inquiries.csv (preferred v2)
    - cpis.csv (legacy fallback)
    - requirements.csv
    - sessions.csv
    - members.csv

    Compatibility:
    - Still creates :CPI nodes for existing consumers.
    - Adds richer model: :Inquiry, :InquiryRequirement, :InquirySession.
    """

    name = "senado_cpis"
    source_id = "senado_cpis"

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
        self._raw: pd.DataFrame = pd.DataFrame()
        self._raw_requirements: pd.DataFrame = pd.DataFrame()
        self._raw_sessions: pd.DataFrame = pd.DataFrame()
        self._raw_members: pd.DataFrame = pd.DataFrame()
        self._raw_history_sources: pd.DataFrame = pd.DataFrame()

        # Backward-compatible outputs
        self.cpis: list[dict[str, Any]] = []
        self.senator_rels: list[dict[str, Any]] = []

        # New model outputs
        self.inquiries: list[dict[str, Any]] = []
        self.inquiry_requirements: list[dict[str, Any]] = []
        self.inquiry_sessions: list[dict[str, Any]] = []
        self.inquiry_requirement_rels: list[dict[str, Any]] = []
        self.inquiry_session_rels: list[dict[str, Any]] = []
        self.inquiry_member_rels: list[dict[str, Any]] = []
        self.requirement_author_cpf_rels: list[dict[str, Any]] = []
        self.requirement_author_name_rels: list[dict[str, Any]] = []
        self.requirement_company_mentions: list[dict[str, Any]] = []
        self.temporal_violations: list[dict[str, Any]] = []
        self.source_documents: list[dict[str, Any]] = []
        self._inquiry_date_lookup: dict[str, tuple[str, str]] = {}

        self.run_id = f"{self.name}_{pd.Timestamp.utcnow().strftime('%Y%m%d%H%M%S')}"

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False)
        except pd.errors.EmptyDataError:
            logger.info("[senado_cpis] empty file (treated as no data): %s", path.name)
            return pd.DataFrame()

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "senado_cpis"
        if not src_dir.exists():
            logger.warning("[senado_cpis] data dir not found: %s", src_dir)
            return

        inquiries_csv = src_dir / "inquiries.csv"
        legacy_csv = src_dir / "cpis.csv"

        if inquiries_csv.exists():
            self._raw_inquiries = self._read_csv_optional(inquiries_csv)
        elif legacy_csv.exists():
            self._raw_inquiries = self._read_csv_optional(legacy_csv)
        else:
            logger.warning("[senado_cpis] inquiries.csv/cpis.csv not found in %s", src_dir)
            return

        self._raw_requirements = self._read_csv_optional(src_dir / "requirements.csv")
        self._raw_sessions = self._read_csv_optional(src_dir / "sessions.csv")
        self._raw_members = self._read_csv_optional(src_dir / "members.csv")
        self._raw_history_sources = self._read_csv_optional(src_dir / "history_sources.csv")

        if self.limit:
            self._raw_inquiries = self._raw_inquiries.head(self.limit)
        self._raw = self._raw_inquiries

        logger.info(
            "[senado_cpis] extracted inquiries=%d requirements=%d sessions=%d members=%d",
            len(self._raw_inquiries),
            len(self._raw_requirements),
            len(self._raw_sessions),
            len(self._raw_members),
        )

    def transform(self) -> None:
        if self._raw_inquiries.empty and not self._raw.empty:
            # Legacy compatibility for tests/callers that pre-fill self._raw.
            self._raw_inquiries = self._raw

        if self._raw_inquiries.empty:
            return

        self._transform_inquiries()
        self._transform_members()
        self._transform_requirements()
        self._transform_sessions()
        self._transform_source_documents()


    def _transform_inquiries(self) -> None:
        inquiries: list[dict[str, Any]] = []
        cpis: list[dict[str, Any]] = []

        for _, row in self._raw_inquiries.iterrows():
            code = row_pick(row, "inquiry_code", "codigo", "codigo_cpi")
            name = row_pick(row, "name", "nome", "nome_cpi")
            if not name:
                continue

            kind = _infer_kind(name, row_pick(row, "kind", "tipo"))
            house = row_pick(row, "house", "casa") or "senado"
            status = row_pick(row, "status", "situacao")
            subject = row_pick(row, "subject", "objeto")
            source_url = row_pick(row, "source_url", "url")
            source_system = row_pick(row, "source_system")
            extraction_method = row_pick(row, "extraction_method")
            source_ref = row_pick(row, "source_ref")
            date_precision = row_pick(row, "date_precision") or "unknown"
            date_start = parse_date(row_pick(row, "date_start", "data_inicio"))
            date_end = parse_date(row_pick(row, "date_end", "data_fim"))

            inquiry_id = row_pick(row, "inquiry_id")
            if not inquiry_id:
                inquiry_id = _stable_id(code, name, length=20)

            inquiry = {
                "inquiry_id": inquiry_id,
                "code": code,
                "name": name,
                "kind": kind,
                "house": house,
                "status": status,
                "subject": subject,
                "date_start": date_start,
                "date_end": date_end,
                "source_url": source_url,
                "source": "senado_cpis",
                "source_system": source_system,
                "extraction_method": extraction_method,
                "source_ref": source_ref,
                "date_precision": date_precision,
                "run_id": self.run_id,
            }
            inquiries.append(inquiry)

            cpi_id = _make_cpi_id(code or inquiry_id, name)
            cpis.append({
                "cpi_id": cpi_id,
                "code": code,
                "name": name,
                "date_start": date_start,
                "date_end": date_end,
                "subject": subject,
                "source": "senado_cpis",
                "inquiry_id": inquiry_id,
                "kind": kind,
                "house": house,
            })

        self.inquiries = deduplicate_rows(inquiries, ["inquiry_id"])
        self.cpis = deduplicate_rows(cpis, ["cpi_id"])
        self._inquiry_date_lookup = {
            str(row.get("inquiry_id", "")): (
                str(row.get("date_start", "")).strip(),
                str(row.get("date_end", "")).strip(),
            )
            for row in self.inquiries
        }

    def _transform_members(self) -> None:
        rows: list[dict[str, Any]] = []

        # Legacy fallback: member info embedded in cpis.csv row.
        source = self._raw_members if not self._raw_members.empty else self._raw_inquiries

        for _, row in source.iterrows():
            inquiry_id = row_pick(row, "inquiry_id")
            if not inquiry_id:
                code = row_pick(row, "inquiry_code", "codigo", "codigo_cpi")
                name = row_pick(row, "name", "nome", "nome_cpi")
                if not name:
                    continue
                inquiry_id = _stable_id(code, name, length=20)

            person_name = normalize_name(
                row_pick(row, "member_name", "nome_parlamentar", "name")
            )
            if not person_name:
                continue

            role = row_pick(row, "role", "papel")

            rows.append({
                "inquiry_id": inquiry_id,
                "person_name": person_name,
                "role": role,
            })

        self.inquiry_member_rels = rows

        cpi_lookup = {c["inquiry_id"]: c["cpi_id"] for c in self.cpis}
        self.senator_rels = [
            {
                "senator_name": r["person_name"],
                "cpi_id": cpi_lookup.get(r["inquiry_id"], ""),
                "role": r["role"],
            }
            for r in rows
            if cpi_lookup.get(r["inquiry_id"])
        ]

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

            requirement_id = row_pick(row, "requirement_id", "codigo", "id")
            req_type = row_pick(row, "type", "tipo")
            text = row_pick(row, "text", "texto", "ementa")
            status = row_pick(row, "status", "situacao")
            source_url = row_pick(row, "source_url", "url")
            source_system = row_pick(row, "source_system")
            extraction_method = row_pick(row, "extraction_method")
            source_ref = row_pick(row, "source_ref")
            date_precision = row_pick(row, "date_precision") or "unknown"
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
                "source": "senado_cpis",
                "source_system": source_system,
                "extraction_method": extraction_method,
                "source_ref": source_ref,
                "date_precision": date_precision,
                "run_id": self.run_id,
            })
            start_date, end_date = self._inquiry_date_lookup.get(inquiry_id, ("", ""))
            temporal_status = _temporal_status(date, start_date, end_date)

            inquiry_rels.append({
                "source_key": inquiry_id,
                "target_key": requirement_id,
                "event_date": date,
                "temporal_status": temporal_status,
                "temporal_rule": _TEMPORAL_RULE,
            })
            if temporal_status == "invalid":
                self.temporal_violations.append({
                    "violation_id": _stable_id("req", inquiry_id, requirement_id, date, length=20),
                    "edge_type": "TEM_REQUERIMENTO",
                    "rule": _TEMPORAL_RULE,
                    "event_date": date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "source_id": self.source_id,
                    "run_id": self.run_id,
                })

            author_cpf_raw = row_pick(row, "author_cpf", "cpf_autor")
            author_digits = strip_document(author_cpf_raw)
            if len(author_digits) == 11:
                author_cpf_rels.append({
                    "source_key": format_cpf(author_digits),
                    "target_key": requirement_id,
                })
            # Do not infer factual author->requirement edges from name-only rows.
            # Name is preserved on InquiryRequirement for exploratory analysis.

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

        self.inquiry_requirements = deduplicate_rows(requirements, ["requirement_id"])
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
        session_rels: list[dict[str, Any]] = []

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
            source_ref = row_pick(row, "source_ref")
            date_precision = row_pick(row, "date_precision") or "unknown"

            if not session_id:
                session_id = _stable_id(inquiry_id, date, topic[:200], length=20)

            sessions.append({
                "session_id": session_id,
                "date": date,
                "topic": topic,
                "source_url": source_url,
                "source": "senado_cpis",
                "source_system": source_system,
                "extraction_method": extraction_method,
                "source_ref": source_ref,
                "date_precision": date_precision,
                "run_id": self.run_id,
            })
            start_date, end_date = self._inquiry_date_lookup.get(inquiry_id, ("", ""))
            temporal_status = _temporal_status(date, start_date, end_date)

            session_rels.append({
                "source_key": inquiry_id,
                "target_key": session_id,
                "event_date": date,
                "temporal_status": temporal_status,
                "temporal_rule": _TEMPORAL_RULE,
            })
            if temporal_status == "invalid":
                self.temporal_violations.append({
                    "violation_id": _stable_id("sess", inquiry_id, session_id, date, length=20),
                    "edge_type": "REALIZOU_SESSAO",
                    "rule": _TEMPORAL_RULE,
                    "event_date": date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "source_id": self.source_id,
                    "run_id": self.run_id,
                })

        self.inquiry_sessions = deduplicate_rows(sessions, ["session_id"])
        self.inquiry_session_rels = session_rels

    def _transform_source_documents(self) -> None:
        if self._raw_history_sources.empty:
            return

        documents: list[dict[str, Any]] = []
        for _, row in self._raw_history_sources.iterrows():
            url = row_pick(row, "source_url", "url")
            checksum = row_pick(row, "checksum")
            if not url:
                continue
            doc_id = _stable_id(url, checksum or "", length=24)
            documents.append({
                "doc_id": doc_id,
                "url": url,
                "checksum": checksum,
                "published_at": row_pick(row, "period_end"),
                "retrieved_at": row_pick(row, "retrieved_at_utc"),
                "content_type": row_pick(row, "doc_type") or "application/pdf",
                "source_id": self.source_id,
                "run_id": self.run_id,
            })

        self.source_documents = deduplicate_rows(documents, ["doc_id"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.inquiries:
            count = loader.load_nodes("Inquiry", self.inquiries, key_field="inquiry_id")
            logger.info("[senado_cpis] loaded %d Inquiry nodes", count)

        if self.cpis:
            count = loader.load_nodes("CPI", self.cpis, key_field="cpi_id")
            logger.info("[senado_cpis] loaded %d CPI nodes", count)

            # Explicit compatibility bridge between old and new labels.
            bridge_rows = [
                {"source_key": row["cpi_id"], "target_key": row["inquiry_id"]}
                for row in self.cpis
                if row.get("inquiry_id")
            ]
            if bridge_rows:
                loader.load_relationships(
                    rel_type="EH_INQUIRY",
                    rows=bridge_rows,
                    source_label="CPI",
                    source_key="cpi_id",
                    target_label="Inquiry",
                    target_key="inquiry_id",
                )

        if self.inquiry_requirements:
            count = loader.load_nodes(
                "InquiryRequirement",
                self.inquiry_requirements,
                key_field="requirement_id",
            )
            logger.info("[senado_cpis] loaded %d InquiryRequirement nodes", count)

        if self.inquiry_sessions:
            count = loader.load_nodes(
                "InquirySession",
                self.inquiry_sessions,
                key_field="session_id",
            )
            logger.info("[senado_cpis] loaded %d InquirySession nodes", count)

        if self.inquiry_requirement_rels:
            loader.load_relationships(
                rel_type="TEM_REQUERIMENTO",
                rows=self.inquiry_requirement_rels,
                source_label="Inquiry",
                source_key="inquiry_id",
                target_label="InquiryRequirement",
                target_key="requirement_id",
                properties=["event_date", "temporal_status", "temporal_rule"],
            )

        if self.inquiry_session_rels:
            loader.load_relationships(
                rel_type="REALIZOU_SESSAO",
                rows=self.inquiry_session_rels,
                source_label="Inquiry",
                source_key="inquiry_id",
                target_label="InquirySession",
                target_key="session_id",
                properties=["event_date", "temporal_status", "temporal_rule"],
            )

        if self.inquiry_member_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person) WHERE p.name = row.person_name "
                "MATCH (i:Inquiry {inquiry_id: row.inquiry_id}) "
                "MERGE (p)-[r:PARTICIPA_INQUIRY]->(i) "
                "SET r.role = row.role"
            )
            loader.run_query_with_retry(query, self.inquiry_member_rels)

        if self.senator_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person) WHERE p.name = row.senator_name "
                "MATCH (c:CPI {cpi_id: row.cpi_id}) "
                "MERGE (p)-[r:PARTICIPOU_CPI]->(c) "
                "SET r.role = row.role"
            )
            loader.run_query_with_retry(query, self.senator_rels)

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
                    {
                        "cnpj": row["cnpj"],
                        "razao_social": row.get("cnpj", ""),
                    }
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

        if self.temporal_violations:
            count = loader.load_nodes(
                "TemporalViolation",
                deduplicate_rows(self.temporal_violations, ["violation_id"]),
                key_field="violation_id",
            )
            logger.info("[senado_cpis] loaded %d TemporalViolation nodes", count)

        if self.source_documents:
            count = loader.load_nodes(
                "SourceDocument",
                self.source_documents,
                key_field="doc_id",
            )
            logger.info("[senado_cpis] loaded %d SourceDocument nodes", count)

"""ETL pipeline for DOU (Diario Oficial da Uniao) gazette acts.

Ingests structured act data from the official Imprensa Nacional portal
(in.gov.br). Creates DOUAct nodes linked to Person (by CPF) via PUBLICOU
and to Company (by CNPJ) via MENCIONOU.

Data source: Imprensa Nacional XML dumps (preferred) or pre-downloaded
JSON files in data/dou/. See scripts/download_dou.py for acquisition.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from defusedxml.ElementTree import ParseError as _XmlParseError  # type: ignore[import-untyped]
from defusedxml.ElementTree import (
    parse as _safe_xml_parse,  # type: ignore[import-untyped,unused-ignore]
)

from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    extract_cnpjs,
    extract_cpfs,
    parse_date,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# DOU sections
_SECTION_MAP: dict[str, str] = {
    "DO1": "secao_1",
    "DO2": "secao_2",
    "DO3": "secao_3",
    "DOE": "secao_extra",
}

# Act-type keywords for classification
_NOMINATION_KEYWORDS = (
    "nomear", "nomeacao", "nomeação", "designar", "designacao", "designação",
)
_EXONERATION_KEYWORDS = (
    "exonerar", "exoneracao", "exoneração", "dispensar",
)
_CONTRACT_KEYWORDS = (
    "contrato", "extrato de contrato", "contratada", "contratante",
)
_PENALTY_KEYWORDS = (
    "penalidade", "suspensao", "suspensão", "impedimento",
    "inidoneidade", "advertencia", "advertência",
)

def _classify_act(title: str, abstract: str) -> str:
    """Classify a DOU act by type based on title and abstract text."""
    combined = f"{title} {abstract}".lower()

    if any(kw in combined for kw in _NOMINATION_KEYWORDS):
        return "nomeacao"
    if any(kw in combined for kw in _EXONERATION_KEYWORDS):
        return "exoneracao"
    if any(kw in combined for kw in _CONTRACT_KEYWORDS):
        return "contrato"
    if any(kw in combined for kw in _PENALTY_KEYWORDS):
        return "penalidade"
    return "outro"


def _make_act_id(url_title: str, date: str) -> str:
    """Generate a stable act ID from URL title and date."""
    raw = f"dou_{url_title}_{date}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# --------------------------------------------------------------------------
# Module-level fetch_to_disk: Imprensa Nacional public leiturajornal pages.
# --------------------------------------------------------------------------
#
# The in.gov.br "Leitura do Jornal" page renders a per-day, per-secao HTML
# page and embeds a ``<script id="params" type="application/json">``
# payload containing a ``jsonArray`` field — one element per published act
# for that day/section, with fields: ``urlTitle, title, pubDate, pubName,
# hierarchyStr, content`` plus a few others. The sections published are
# ``DO1`` (executive/legislative), ``DO2`` (personnel), ``DO3`` (contracts)
# and — rarely — ``DO1E`` / ``DO2E`` / ``DO3E`` (extra editions).
#
# ``fetch_to_disk`` walks a date range, issues one GET per (day, section),
# extracts the embedded JSON and writes ``<YYYY-MM-DD>_<section>.json``
# files in the shape the pipeline's ``_extract_json`` already consumes
# (``{"jsonArray": [...]}``). The upstream ``content`` field is remapped to
# ``abstract`` so downstream CPF/CNPJ extraction finds the full text.
#
# Upstream hierarchy: no auth, no API key. The page sits behind a
# TLS-terminating edge that sometimes 403s requests without a User-Agent;
# we always send one.

_DOU_LEITURAJORNAL_URL = "https://www.in.gov.br/leiturajornal"
_DOU_SECTIONS_DEFAULT: tuple[str, ...] = ("do1", "do2", "do3")
_DOU_PARAMS_RE = re.compile(
    r'<script id="params" type="application/json">\s*(\{.*?\})\s*</script>',
    re.DOTALL,
)
_DOU_HTTP_TIMEOUT = 60.0


def _parse_date(value: str) -> _dt.date:
    """Parse YYYY-MM-DD or DD-MM-YYYY into a date."""
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return _dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unparseable date {value!r} (expected YYYY-MM-DD)")


def _fetch_dou_day_section(
    client: httpx.Client, day: _dt.date, section: str,
) -> list[dict[str, Any]]:
    """Fetch one DOU day+section page and return its ``jsonArray`` items.

    Returns an empty list when the day has no edition (weekends/holidays or
    sections without a publication that day).
    """
    params = {"data": day.strftime("%d-%m-%Y"), "secao": section}
    resp = client.get(_DOU_LEITURAJORNAL_URL, params=params)
    resp.raise_for_status()
    match = _DOU_PARAMS_RE.search(resp.text)
    if not match:
        logger.debug(
            "[dou.fetch_to_disk] %s %s: no params script (unexpected layout)",
            day, section,
        )
        return []
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        logger.warning(
            "[dou.fetch_to_disk] %s %s: failed to parse params JSON (%s)",
            day, section, exc,
        )
        return []
    return list(payload.get("jsonArray", []) or [])


def _remap_item(raw: dict[str, Any]) -> dict[str, str]:
    """Map an Imprensa Nacional item to the pipeline's JSON schema."""
    # Prefer 'title' over 'titulo'; 'content' is the full text body and
    # serves as the pipeline's ``abstract`` (downstream extracts CPFs/CNPJs).
    return {
        "urlTitle": str(raw.get("urlTitle", "") or ""),
        "title": str(raw.get("title") or raw.get("titulo") or ""),
        "abstract": str(raw.get("content") or raw.get("abstract") or ""),
        "pubDate": str(raw.get("pubDate") or ""),
        "pubName": str(raw.get("pubName") or ""),
        "artCategory": str(raw.get("artType") or raw.get("artCategory") or ""),
        "hierarchyStr": str(raw.get("hierarchyStr") or ""),
    }


def fetch_to_disk(
    output_dir: Path | str,
    start_date: str | _dt.date | None = None,
    end_date: str | _dt.date | None = None,
    sections: list[str] | tuple[str, ...] | None = None,
    timeout: float = _DOU_HTTP_TIMEOUT,
    max_days: int | None = 30,
) -> list[Path]:
    """Download DOU acts for a date range into ``output_dir`` as JSON files.

    One file per (day, section) combination is written with the shape
    ``{"jsonArray": [...]}``, matching the pipeline's legacy JSON loader.
    The upstream ``content`` string is remapped to ``abstract`` so the
    pipeline's CPF/CNPJ extractors see the full act text.

    Args:
        output_dir: Destination directory. Created if missing.
        start_date: First day to fetch (``YYYY-MM-DD`` or ``date``).
            Defaults to ``end_date - (max_days - 1)`` so a bare call grabs
            the trailing window.
        end_date: Last day to fetch (inclusive). Defaults to today (UTC).
        sections: Sections to request (default ``('do1','do2','do3')``).
            Case-insensitive; lowercased on the wire.
        timeout: Per-request HTTP timeout.
        max_days: Soft cap on the span to prevent runaway loops. ``None``
            disables the cap.

    Returns:
        Sorted list of paths to every non-empty JSON file written. Empty
        (day, section) pairs are skipped silently (no file is produced).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    today = _dt.date.today()
    end = (
        _parse_date(end_date) if isinstance(end_date, str)
        else (end_date or today)
    )
    if start_date is None:
        span = (max_days or 30) - 1
        start = end - _dt.timedelta(days=max(span, 0))
    elif isinstance(start_date, str):
        start = _parse_date(start_date)
    else:
        start = start_date
    if start > end:
        raise ValueError(f"start_date {start} is after end_date {end}")
    if max_days is not None and (end - start).days + 1 > max_days:
        raise ValueError(
            f"date range {start}..{end} spans {(end - start).days + 1} days, "
            f"exceeds max_days={max_days}. Raise --max-days to proceed."
        )

    secs = tuple(s.lower() for s in (sections or _DOU_SECTIONS_DEFAULT))

    logger.info(
        "[dou.fetch_to_disk] Fetching DOU %s..%s sections=%s -> %s",
        start, end, secs, output_dir,
    )

    written: list[Path] = []
    with httpx.Client(
        timeout=timeout,
        follow_redirects=True,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (br-acc/bracc-etl download_dou; httpx)"
            ),
            "Accept": "text/html,application/xhtml+xml",
        },
    ) as client:
        day = start
        while day <= end:
            for section in secs:
                try:
                    items = _fetch_dou_day_section(client, day, section)
                except httpx.HTTPError as exc:
                    logger.warning(
                        "[dou.fetch_to_disk] %s %s: HTTP error (%s) -- skipping",
                        day, section, exc,
                    )
                    continue
                if not items:
                    logger.debug(
                        "[dou.fetch_to_disk] %s %s: no acts", day, section,
                    )
                    continue
                mapped = [_remap_item(it) for it in items]
                out_path = (
                    output_dir / f"{day.isoformat()}_{section}.json"
                )
                with open(out_path, "w", encoding="utf-8") as fh:
                    json.dump({"jsonArray": mapped}, fh, ensure_ascii=False)
                written.append(out_path.resolve())
                logger.info(
                    "[dou.fetch_to_disk] %s %s: wrote %d acts (%s)",
                    day, section, len(mapped), out_path.name,
                )
            day += _dt.timedelta(days=1)

    logger.info(
        "[dou.fetch_to_disk] Done: %d file(s) across %d day(s)",
        len(written), (end - start).days + 1,
    )
    return sorted(written)


class DouPipeline(Pipeline):
    """ETL pipeline for DOU (Diario Oficial da Uniao) acts.

    Reads JSON files from data/dou/ containing act records from the
    Imprensa Nacional portal (in.gov.br). Each act becomes a DOUAct node,
    with relationships to Person (PUBLICOU) and Company (MENCIONOU) based
    on CPF/CNPJ extraction from act text.
    """

    name = "dou"
    source_id = "imprensa_nacional"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_acts: list[dict[str, str]] = []
        self.acts: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        dou_dir = Path(self.data_dir) / "dou"
        if not dou_dir.exists():
            msg = f"DOU data directory not found at {dou_dir}"
            raise FileNotFoundError(msg)

        # Try parquet (BigQuery), then XML (Imprensa Nacional), then JSON (legacy)
        parquet_files = sorted(dou_dir.rglob("*.parquet"))
        xml_files = sorted(dou_dir.rglob("*.xml"))
        json_files = sorted(dou_dir.glob("*.json"))

        if parquet_files:
            self._extract_parquet(parquet_files)
        elif xml_files:
            self._extract_xml(xml_files)
        elif json_files:
            self._extract_json(json_files)
        else:
            logger.warning("[dou] No parquet, XML, or JSON files found in %s", dou_dir)
            return

        logger.info("[dou] Extracted %d act records", len(self._raw_acts))

    def _extract_parquet(self, parquet_files: list[Path]) -> None:
        """Extract acts from BigQuery parquet exports (basedosdados DOU)."""
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.compute as pc  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found]

        parquet_cols = [
            "titulo", "orgao", "ementa", "excerto",
            "secao", "data_publicacao", "url", "tipo_edicao",
        ]

        for f in parquet_files:
            try:
                table = pq.read_table(f, columns=parquet_cols)
                # Cast all to string — avoids date32/dbdate pandas incompatibility
                str_cols = [pc.cast(table.column(c), pa.string()) for c in table.column_names]
                df = pa.table(dict(zip(table.column_names, str_cols, strict=True))).to_pandas()
            except Exception:
                logger.warning("[dou] Failed to read parquet: %s", f.name)
                continue

            logger.info("[dou] Reading %d rows from %s", len(df), f.name)

            for _, row in df.iterrows():
                titulo = str(row.get("titulo", "") or "").strip()
                orgao = str(row.get("orgao", "") or "").strip()
                ementa = str(row.get("ementa", "") or "").strip()
                excerto = str(row.get("excerto", "") or "").strip()
                secao = str(row.get("secao", "") or "").strip()
                pub_date = str(row.get("data_publicacao", "") or "").strip()
                url = str(row.get("url", "") or "").strip()
                tipo_edicao = str(row.get("tipo_edicao", "") or "").strip()

                # Use URL as identifier (stable across editions)
                url_title = url.rsplit("/", 1)[-1] if url else titulo[:60]

                # Combine ementa + excerto for abstract text
                abstract = f"{ementa} {excerto}".strip()

                self._raw_acts.append({
                    "urlTitle": url_title,
                    "title": titulo,
                    "abstract": abstract[:2000],
                    "pubDate": pub_date,
                    "pubName": f"DO{secao}" if secao else "",
                    "artCategory": tipo_edicao,
                    "hierarchyStr": orgao,
                })

                if self.limit and len(self._raw_acts) >= self.limit:
                    return

    def _extract_xml(self, xml_files: list[Path]) -> None:
        """Extract acts from Imprensa Nacional XML dumps."""
        for f in xml_files:
            try:
                tree = _safe_xml_parse(f)
            except _XmlParseError:
                logger.warning("[dou] Failed to parse XML: %s", f.name)
                continue

            root = tree.getroot()

            # Handle both <article> elements and <xml><article> wrappers
            articles = root.findall(".//article")
            if not articles:
                articles = [root] if root.tag == "article" else []

            for article in articles:
                identifica = article.find(".//identifica")
                texto = article.find(".//Texto")
                if texto is None:
                    texto = article.find(".//texto")
                date_el = identifica.find("data") if identifica is not None else None
                orgao_el = identifica.find("orgao") if identifica is not None else None
                titulo_el = identifica.find("titulo") if identifica is not None else None
                secao_el = identifica.find("secao") if identifica is not None else None

                title = (titulo_el.text or "").strip() if titulo_el is not None else ""
                pub_date = (date_el.text or "").strip() if date_el is not None else ""
                agency = (orgao_el.text or "").strip() if orgao_el is not None else ""
                section = (secao_el.text or "").strip() if secao_el is not None else ""

                # Collect all text from Texto element
                abstract = ""
                if texto is not None:
                    abstract = " ".join(
                        (p.text or "").strip()
                        for p in texto.iter()
                        if p.text and p.text.strip()
                    )

                # Use article id or generate from title+date
                art_id = article.get("id", "") or article.get("artType", "")

                self._raw_acts.append({
                    "urlTitle": art_id,
                    "title": title,
                    "abstract": abstract[:2000],
                    "pubDate": pub_date,
                    "pubName": f"DO{section}" if section else "",
                    "artCategory": article.get("artCategory", ""),
                    "hierarchyStr": agency,
                })

                if self.limit and len(self._raw_acts) >= self.limit:
                    return

    def _extract_json(self, json_files: list[Path]) -> None:
        """Extract acts from legacy JSON format (IN search API)."""
        for f in json_files:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)

            if isinstance(data, dict) and "jsonArray" in data:
                items = data["jsonArray"]
            elif isinstance(data, list):
                items = data
            else:
                logger.warning("[dou] Unexpected JSON format in %s", f.name)
                continue

            for item in items:
                self._raw_acts.append({
                    "urlTitle": str(item.get("urlTitle", "")),
                    "title": str(item.get("title", "")),
                    "abstract": str(item.get("abstract", "")),
                    "pubDate": str(item.get("pubDate", "")),
                    "pubName": str(item.get("pubName", "")),
                    "artCategory": str(item.get("artCategory", "")),
                    "hierarchyStr": str(item.get("hierarchyStr", "")),
                })

                if self.limit and len(self._raw_acts) >= self.limit:
                    return

    def transform(self) -> None:
        acts: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []
        skipped = 0

        for raw in self._raw_acts:
            url_title = raw["urlTitle"].strip()
            title = raw["title"].strip()
            abstract = raw["abstract"].strip()
            pub_date = raw["pubDate"].strip()

            if not url_title or not pub_date:
                skipped += 1
                continue

            date = parse_date(pub_date)
            act_id = _make_act_id(url_title, date)
            act_type = _classify_act(title, abstract)
            section = _SECTION_MAP.get(raw["pubName"].strip(), raw["pubName"].strip())
            agency = raw["hierarchyStr"].strip()
            category = raw["artCategory"].strip()

            # Build URL from urlTitle
            url = f"https://www.in.gov.br/web/dou/-/{url_title}"

            acts.append({
                "act_id": act_id,
                "title": title,
                "act_type": act_type,
                "date": date,
                "section": section,
                "agency": agency,
                "category": category,
                "text_excerpt": abstract[:500] if abstract else "",
                "url": url,
                "source": "imprensa_nacional",
            })

            # Extract CPFs -> PUBLICOU relationships
            cpfs = extract_cpfs(abstract)
            for cpf in cpfs:
                person_rels.append({
                    "source_key": cpf,
                    "target_key": act_id,
                })

            # Extract CNPJs -> MENCIONOU relationships
            cnpjs = extract_cnpjs(abstract)
            for cnpj in cnpjs:
                company_rels.append({
                    "source_key": cnpj,
                    "target_key": act_id,
                })

        self.acts = deduplicate_rows(acts, ["act_id"])
        self.person_rels = person_rels
        self.company_rels = company_rels

        logger.info(
            "[dou] Transformed %d acts (%d person links, %d company links, skipped %d)",
            len(self.acts),
            len(self.person_rels),
            len(self.company_rels),
            skipped,
        )

    def load(self) -> None:
        if not self.acts:
            logger.warning("[dou] No acts to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # Load DOUAct nodes
        count = loader.load_nodes("DOUAct", self.acts, key_field="act_id")
        logger.info("[dou] Loaded %d DOUAct nodes", count)

        # PUBLICOU: Person -> DOUAct (match existing persons by CPF)
        if self.person_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (a:DOUAct {act_id: row.target_key}) "
                "MERGE (p)-[:PUBLICOU]->(a)"
            )
            count = loader.run_query_with_retry(query, self.person_rels)
            logger.info("[dou] Created %d PUBLICOU relationships", count)

        # MENCIONOU: Company -> DOUAct (match existing companies by CNPJ)
        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MATCH (a:DOUAct {act_id: row.target_key}) "
                "MERGE (c)-[:MENCIONOU]->(a)"
            )
            count = loader.run_query_with_retry(query, self.company_rels)
            logger.info("[dou] Created %d MENCIONOU relationships", count)

"""ETL pipeline for Camara Municipal de Goiania.

Ingests vereadores (council members), their office expenses, and legislative
proposals from the Goiania city council transparency portal.

Nodes created:
  - GoVereador
  - GoCouncilExpense
  - GoLegislativeProposal

Relationships:
  - (GoVereador)-[:AUTOR_DE]->(GoLegislativeProposal)
  - (GoVereador)-[:DESPESA_GABINETE]->(GoCouncilExpense)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
    parse_date,
    parse_number_smart,
)
from bracc_etl.transforms import (
    stable_id as _stable_id,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_API_BASE = "https://www.goiania.go.leg.br"
_TIMEOUT = 30

# Canonical endpoint -> output filename mapping used by both the pipeline's
# offline fallback (``extract``) and the ``fetch_to_disk`` helper below.
_ENDPOINT_FILES: tuple[tuple[str, str], ...] = (
    ("@@portalmodelo-json", "vereadores.json"),
    ("@@transparency-json", "transparency.json"),
    ("@@pl-json", "proposicoes.json"),
)

# Fallback content-type quando o portal Plone omite o header — improvavel
# mas archival precisa de algum string pra derivar extensao. Os 3 endpoints
# ``@@portalmodelo-json`` / ``@@transparency-json`` / ``@@pl-json`` sempre
# devolvem ``application/json`` na pratica.
_CAMARA_JSON_CONTENT_TYPE = "application/json"

# Chave privada injetada em cada raw record pelo caminho online pra
# propagar a URI do snapshot archival ate ``transform``. Prefixo ``__``
# evita colisao com qualquer campo devolvido pelo portal. ``transform``
# le essa chave e filtra antes de carimbar a proveniencia.
_SNAPSHOT_KEY = "__snapshot_uri"


def _http_get_json(endpoint: str) -> Any:
    """Fetch a JSON endpoint from the Camara Goiania portal.

    Returns the raw decoded payload (list or dict) or ``None`` on failure.
    This helper is shared by the pipeline's in-memory extract and the
    ``fetch_to_disk`` CLI helper so both paths stay in sync on URLs/timeouts.
    """
    payload, _content, _ctype = _http_get_json_raw(endpoint)
    return payload


def _http_get_json_raw(
    endpoint: str,
) -> tuple[Any, bytes | None, str | None]:
    """Archival-aware variant of :func:`_http_get_json`.

    Returns ``(payload, content_bytes, content_type)``. Bytes e content-type
    ficam ``None`` em caso de falha HTTP/JSON — nada pra arquivar. Em sucesso,
    ``content`` carrega os bytes crus exatamente como o servidor devolveu
    pra :func:`archive_fetch` preservar sem reprocessamento.
    """
    url = f"{_API_BASE}/{endpoint}"
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.get(url)
            resp.raise_for_status()
            content = resp.content
            content_type = resp.headers.get(
                "content-type", _CAMARA_JSON_CONTENT_TYPE,
            )
            return resp.json(), content, content_type
    except (httpx.HTTPError, json.JSONDecodeError) as exc:
        logger.warning("[camara_goiania] API request failed (%s): %s", endpoint, exc)
        return None, None, None


def _snapshot_from_record(row: dict[str, Any]) -> str | None:
    """Leitura defensiva do URI archival injetado pelo caminho online.

    Retorna ``None`` quando a chave ``__snapshot_uri`` esta ausente
    (caminho offline) ou nao-string — o caller omite ``snapshot_uri`` em
    ``attach_provenance`` e o contrato opt-in se preserva.
    """
    raw = row.get(_SNAPSHOT_KEY)
    if isinstance(raw, str) and raw:
        return raw
    return None


def _unwrap_records(payload: Any) -> list[dict[str, Any]]:
    """Normalize a Camara Goiania JSON payload into a list of dict rows."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("items", "results", "data", "records"):
            if isinstance(payload.get(key), list):
                return [r for r in payload[key] if isinstance(r, dict)]
        return [payload]
    return []


def fetch_to_disk(
    output_dir: Path,
    limit: int | None = None,
) -> list[Path]:
    """Download the Camara Municipal de Goiania JSON feeds to ``output_dir``.

    Hits the three portal endpoints (vereadores, transparency, proposicoes),
    optionally truncates each to ``limit`` records, and writes them as
    ``vereadores.json`` / ``transparency.json`` / ``proposicoes.json`` — the
    exact filenames the pipeline's ``extract`` step looks for locally.

    Returns the list of files actually written. Endpoints that fail the
    network fetch are logged and skipped (the pipeline's online fallback
    will still retry them at run time if needed).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for endpoint, filename in _ENDPOINT_FILES:
        payload = _http_get_json(endpoint)
        if payload is None:
            logger.warning(
                "[camara_goiania] skipping %s (no payload)", filename
            )
            continue

        records = _unwrap_records(payload)
        if limit is not None:
            records = records[:limit]

        target = output_dir / filename
        target.write_text(
            json.dumps(records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        written.append(target)
        logger.info(
            "[camara_goiania] wrote %s (%d records)", target, len(records)
        )

    return written


class CamaraGoianiaPipeline(Pipeline):
    """ETL pipeline for Camara Municipal de Goiania."""

    name = "camara_goiania"
    source_id = "camara_goiania"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_vereadores: list[dict[str, Any]] = []
        self._raw_expenses: list[dict[str, Any]] = []
        self._raw_proposicoes: list[dict[str, Any]] = []

        self.vereadores: list[dict[str, Any]] = []
        self.expenses: list[dict[str, Any]] = []
        self.proposals: list[dict[str, Any]] = []
        self.autor_rels: list[dict[str, Any]] = []
        self.despesa_rels: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_json_file(path: Path) -> list[dict[str, Any]]:
        """Load a JSON file, returning a list of dicts."""
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            # try common wrapper keys
            for key in ("items", "results", "data", "records"):
                if isinstance(payload.get(key), list):
                    return [r for r in payload[key] if isinstance(r, dict)]
            return [payload]
        return []

    def _fetch_json(self, endpoint: str) -> list[dict[str, Any]]:
        """Fetch JSON from the Camara API, archiving the raw payload.

        Cada record devolvido ganha a chave privada ``__snapshot_uri`` com
        a URI devolvida por :func:`archive_fetch` — propagada mais tarde
        pra ``attach_provenance(snapshot_uri=...)`` em ``transform``.
        Payloads vazios ou erro HTTP → retorna ``[]`` e nao arquiva nada
        (nada pra preservar). Caminho offline (fixtures JSON locais) NAO
        passa por aqui, entao archival e opt-in preservado: rows derivadas
        de fixture nao ganham ``source_snapshot_uri``.
        """
        payload, content, content_type = _http_get_json_raw(endpoint)
        if payload is None:
            return []
        records = _unwrap_records(payload)
        if not records or content is None or content_type is None:
            return records

        uri = archive_fetch(
            url=f"{_API_BASE}/{endpoint}",
            content=content,
            content_type=content_type,
            run_id=self.run_id,
            source_id=self.source_id,
        )
        # Dicts recem-parseados do payload desta chamada — seguro mutar.
        # ``transform`` le a chave privada; ``load`` nao precisa ver.
        for rec in records:
            rec[_SNAPSHOT_KEY] = uri
        return records

    # ------------------------------------------------------------------
    # extract
    # ------------------------------------------------------------------

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "camara_goiania"

        # Try local files first (offline / fallback)
        self._raw_vereadores = self._load_json_file(src_dir / "vereadores.json")
        self._raw_expenses = self._load_json_file(src_dir / "transparency.json")
        self._raw_proposicoes = self._load_json_file(src_dir / "proposicoes.json")

        # If no local data, fetch from API (archival ativa no caminho online)
        if not self._raw_vereadores:
            self._raw_vereadores = self._fetch_json("@@portalmodelo-json")

        if not self._raw_expenses:
            self._raw_expenses = self._fetch_json("@@transparency-json")

        if not self._raw_proposicoes:
            self._raw_proposicoes = self._fetch_json("@@pl-json")

        if self.limit:
            self._raw_vereadores = self._raw_vereadores[: self.limit]
            self._raw_expenses = self._raw_expenses[: self.limit]
            self._raw_proposicoes = self._raw_proposicoes[: self.limit]

        logger.info(
            "[camara_goiania] extracted vereadores=%d expenses=%d proposicoes=%d",
            len(self._raw_vereadores),
            len(self._raw_expenses),
            len(self._raw_proposicoes),
        )

    # ------------------------------------------------------------------
    # transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        vereadores: list[dict[str, Any]] = []
        expenses: list[dict[str, Any]] = []
        proposals: list[dict[str, Any]] = []
        autor_rels: list[dict[str, Any]] = []
        despesa_rels: list[dict[str, Any]] = []

        # --- vereadores ---
        # Relacoes AUTOR_DE / DESPESA_GABINETE carimbam a URI do snapshot
        # da *rel* (proposicao ou despesa), nao da listagem — porque o
        # payload que "viu" o par (vereador, proposicao/despesa) foi o
        # feed correspondente. name_to_id e suficiente pra resolver
        # source_key; URI propaga a partir do row atual.
        name_to_id: dict[str, str] = {}
        for row in self._raw_vereadores:
            name = normalize_name(
                str(row.get("nome") or row.get("name") or ""),
            )
            if not name:
                continue
            party = str(row.get("partido") or row.get("party") or "").strip()
            legislature = str(row.get("legislatura") or row.get("legislature") or "").strip()

            vid = _stable_id("camara_goiania", name, party)
            name_to_id[name] = vid
            vereador_record_id = f"{name}|{party}"

            snapshot_uri = _snapshot_from_record(row)
            vereadores.append(self.attach_provenance(
                {
                    "vereador_id": vid,
                    "name": name,
                    "party": party,
                    "legislature": legislature,
                    "uf": "GO",
                    "municipality": "Goiania",
                    "municipality_code": "5208707",
                    "source": "camara_goiania",
                },
                record_id=vereador_record_id,
                snapshot_uri=snapshot_uri,
            ))

        # --- expenses ---
        for row in self._raw_expenses:
            vereador_name = normalize_name(
                str(row.get("vereador") or row.get("vereador_name") or ""),
            )
            exp_type = str(row.get("tipo") or row.get("type") or "").strip()
            description = str(row.get("descricao") or row.get("description") or "").strip()
            amount = parse_number_smart(row.get("valor") or row.get("amount"))
            date = parse_date(str(row.get("data") or row.get("date") or ""))
            year = str(row.get("ano") or row.get("year") or "").strip()
            if not year and date:
                year = date[:4]

            eid = _stable_id(
                "camara_goiania_expense",
                vereador_name,
                date,
                description,
                str(amount),
            )
            expense_record_id = f"{vereador_name}|{date}|{description}|{amount}"
            # Expense row carrega a URI do snapshot de ``@@transparency-json``;
            # a rel DESPESA_GABINETE propaga a mesma URI (expense e a fonte
            # de verdade do relacionamento, inclusive se a listagem vereadores
            # veio offline).
            snapshot_uri = _snapshot_from_record(row)
            expenses.append(self.attach_provenance(
                {
                    "expense_id": eid,
                    "vereador_name": vereador_name,
                    "type": exp_type,
                    "description": description,
                    "amount": amount,
                    "date": date,
                    "year": year,
                    "uf": "GO",
                    "municipality": "Goiania",
                    "source": "camara_goiania",
                },
                record_id=expense_record_id,
                snapshot_uri=snapshot_uri,
            ))

            # link to vereador if matched
            if vereador_name in name_to_id:
                despesa_rels.append(self.attach_provenance(
                    {
                        "source_key": name_to_id[vereador_name],
                        "target_key": eid,
                    },
                    record_id=expense_record_id,
                    snapshot_uri=snapshot_uri,
                ))

        # --- proposals ---
        for row in self._raw_proposicoes:
            number = str(row.get("numero") or row.get("number") or "").strip()
            year = str(row.get("ano") or row.get("year") or "").strip()
            prop_type = str(row.get("tipo") or row.get("type") or "").strip()
            subject = str(row.get("ementa") or row.get("subject") or "").strip()
            author = normalize_name(
                str(row.get("autor") or row.get("author") or ""),
            )
            status = str(row.get("situacao") or row.get("status") or "").strip()
            date = parse_date(str(row.get("data") or row.get("date") or ""))

            pid = _stable_id("camara_goiania_prop", number, year, prop_type)
            proposal_record_id = f"{number}|{year}|{prop_type}"
            # Proposal row carrega a URI do snapshot de ``@@pl-json``; rel
            # AUTOR_DE propaga a mesma URI (quem viu a autoria foi o payload
            # de proposicoes, nao o de vereadores).
            snapshot_uri = _snapshot_from_record(row)
            proposals.append(self.attach_provenance(
                {
                    "proposal_id": pid,
                    "number": number,
                    "year": year,
                    "type": prop_type,
                    "subject": subject,
                    "author": author,
                    "status": status,
                    "date": date,
                    "uf": "GO",
                    "municipality": "Goiania",
                    "source": "camara_goiania",
                },
                record_id=proposal_record_id,
                snapshot_uri=snapshot_uri,
            ))

            # link to vereador if author matches
            if author in name_to_id:
                autor_rels.append(self.attach_provenance(
                    {
                        "source_key": name_to_id[author],
                        "target_key": pid,
                    },
                    record_id=proposal_record_id,
                    snapshot_uri=snapshot_uri,
                ))

        self.vereadores = deduplicate_rows(vereadores, ["vereador_id"])
        self.expenses = deduplicate_rows(expenses, ["expense_id"])
        self.proposals = deduplicate_rows(proposals, ["proposal_id"])
        self.autor_rels = deduplicate_rows(autor_rels, ["source_key", "target_key"])
        self.despesa_rels = deduplicate_rows(despesa_rels, ["source_key", "target_key"])

    # ------------------------------------------------------------------
    # load
    # ------------------------------------------------------------------

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.vereadores:
            loader.load_nodes("GoVereador", self.vereadores, key_field="vereador_id")

        if self.expenses:
            loader.load_nodes("GoCouncilExpense", self.expenses, key_field="expense_id")

        if self.proposals:
            loader.load_nodes("GoLegislativeProposal", self.proposals, key_field="proposal_id")

        if self.autor_rels:
            loader.load_relationships(
                rel_type="AUTOR_DE",
                rows=self.autor_rels,
                source_label="GoVereador",
                source_key="vereador_id",
                target_label="GoLegislativeProposal",
                target_key="proposal_id",
            )

        if self.despesa_rels:
            loader.load_relationships(
                rel_type="DESPESA_GABINETE",
                rows=self.despesa_rels,
                source_label="GoVereador",
                source_key="vereador_id",
                target_label="GoCouncilExpense",
                target_key="expense_id",
            )

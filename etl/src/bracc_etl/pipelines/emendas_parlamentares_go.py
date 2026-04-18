"""Emendas parlamentares GO — Portal da Transparência com archival.

Substitui o live-call ``backend/apis_externas.buscar_emendas_transparencia()``
que o Flask faz com ``TRANSPARENCIA_API_KEY`` por dep. Este pipeline ingere
as emendas dos deputados federais eleitos por Goiás já presentes no grafo
(rodados pelo pipeline ``camara_politicos_go``), carimbando proveniência
completa + ``source_snapshot_uri`` via :func:`bracc_etl.archival.archive_fetch`.

Fonte:

* ``GET /api-de-dados/emendas?nomeAutor={nome}&ano={ano}&pagina={p}``
  — header obrigatório ``chave-api-dados: {TRANSPARENCIA_API_KEY}``.
  Pageado: cada página traz até 100 itens; o pipeline avança enquanto
  ``len(page) >= 100`` e respeita um teto defensivo de 50 páginas por
  deputado/ano pra proteger contra loops.

Escopo GO: o pipeline descobre os alvos consultando o grafo:

    MATCH (p:FederalLegislator {uf:'GO'}) RETURN p.id_camara, p.name

Consequentemente depende do pipeline ``camara_politicos_go`` ter rodado
antes. Se o grafo ainda não tiver deputados federais GO, o pipeline
termina vazio (e o Neo4j fica intocado).

Schema:

* Nó ``:Amendment`` (label já usada pelo pipeline federal ``transparencia``)
  ganha os campos pt-BR deste produto (``tipo``, ``funcao``, ``municipio``,
  ``uf``, ``valor_empenhado``, ``valor_pago``, ``ano``, ``autor_nome``).
  Chave estável: ``amendment_id`` (MERGE idempotente).
* Rel ``(:FederalLegislator)-[:PROPOS]->(:Amendment)`` — liga o deputado
  do grafo à emenda ingerida, com proveniência na própria relação.

Proveniência segue o padrão dos 11 pipelines retrofitados:
``source_id="portal_transparencia_emendas"``, ``source_url`` apontando
pro recurso paginado consumido, ``source_record_id=amendment_id``,
``ingested_at``, ``run_id``, ``source_snapshot_uri`` do
:func:`archive_fetch` de cada página.

TRANSPARENCIA_API_KEY é **obrigatória** — o pipeline falha explicitamente
em ``extract()`` se a env var estiver vazia. Evitamos o comportamento
silencioso do Flask (``return []``) porque o pipeline é um passo de
ingestão agendado; falha silenciosa geraria "dados ausentes" sem sinal.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import deduplicate_rows, normalize_name

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Portal da Transparência — endpoint público com auth por header.
_API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
_EMENDAS_ENDPOINT = f"{_API_BASE}/emendas"
_JSON_CONTENT_TYPE = "application/json"
_HTTP_TIMEOUT = 30.0

# Paginação: o Portal devolve no máximo 100 itens por página quando
# ``pagina`` vai sendo incrementado. Avançamos enquanto vier >= 100 e
# limitamos em 50 páginas por (deputado, ano) — teto defensivo.
_PAGE_SIZE_THRESHOLD = 100
_MAX_PAGES_PER_QUERY = 50

# Histórico default: 3 anos (corrente + 2 anteriores) espelhando o Flask.
_ANOS_HISTORICO_DEFAULT = 3

_TARGET_UF = "GO"
_SOURCE_ID = "portal_transparencia_emendas"
_ENV_VAR = "TRANSPARENCIA_API_KEY"


def _amendment_id(
    autor_nome: str, ano: int, numero: str | None, valor_empenhado: float,
) -> str:
    """Content-addressed ID pra emenda (estável entre re-runs).

    O Portal expõe ``codigoEmenda`` quando disponível; quando não, caímos
    num hash do autor + ano + valor empenhado (safety net — a probabilidade
    de colisão pra um mesmo deputado num mesmo ano é baixa).
    """
    if numero and str(numero).strip():
        return f"pte_{str(numero).strip()}"
    raw = f"pte_{autor_nome}_{ano}_{valor_empenhado:.2f}"
    return "pte_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _parse_float(value: Any) -> float:
    """Parse floats tolerante a str com vírgula ou None (-> 0.0)."""
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip().replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _extract_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Extrai os campos relevantes de um item de emenda do Portal.

    O shape do Portal varia ligeiramente entre anos; tentamos múltiplos
    nomes conhecidos pra cada campo e sempre devolvemos strings/floats
    normalizados.
    """
    numero = (
        row.get("codigoEmenda")
        or row.get("numero")
        or row.get("numeroEmenda")
        or ""
    )
    tipo = (
        row.get("tipoEmenda")
        or row.get("tipo")
        or ""
    )
    funcao = (
        row.get("funcao")
        or row.get("nomeFuncao")
        or ""
    )
    municipio = (
        row.get("municipio")
        or row.get("nomeMunicipio")
        or ""
    )
    uf = (
        row.get("uf")
        or row.get("siglaUF")
        or row.get("siglaUf")
        or ""
    )
    ano = row.get("ano") or row.get("anoEmenda") or 0
    try:
        ano_int = int(ano)
    except (TypeError, ValueError):
        ano_int = 0
    valor_empenhado = _parse_float(
        row.get("valorEmpenhado")
        or row.get("valorTotalEmpenhado")
        or row.get("empenhado")
        or 0,
    )
    valor_pago = _parse_float(
        row.get("valorPago")
        or row.get("valorTotalPago")
        or row.get("pago")
        or 0,
    )
    return {
        "numero": str(numero).strip(),
        "tipo": str(tipo).strip(),
        "funcao": str(funcao).strip(),
        "municipio": str(municipio).strip(),
        "uf": str(uf).strip().upper(),
        "ano": ano_int,
        "valor_empenhado": valor_empenhado,
        "valor_pago": valor_pago,
    }


class EmendasParlamentaresGoPipeline(Pipeline):
    """Ingere emendas parlamentares de deputados federais GO no grafo.

    Scope: deputados federais GO já presentes no grafo
    (``:FederalLegislator {uf:'GO'}``). Depende do pipeline
    ``camara_politicos_go`` ter rodado antes.
    """

    name = "emendas_parlamentares_go"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        *,
        archive_online: bool = True,
        **kwargs: Any,
    ) -> None:
        start_year = int(kwargs.pop("start_year", 0) or 0)
        end_year = int(
            kwargs.pop("end_year", 0) or 0,
        )
        http_client_factory = kwargs.pop(
            "http_client_factory",
            lambda: httpx.Client(
                timeout=_HTTP_TIMEOUT, follow_redirects=True,
            ),
        )
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )

        if not end_year:
            end_year = datetime.now(tz=UTC).year
        if not start_year:
            start_year = end_year - (_ANOS_HISTORICO_DEFAULT - 1)
        self.start_year = start_year
        self.end_year = end_year
        self._http_client_factory = http_client_factory

        # Opt-in: archive_online desliga em testes offline. O default replica
        # o padrão dos 11 pipelines retrofitados — pipelines novos ligam o
        # archival por default pra carimbar ``source_snapshot_uri`` em cada
        # row derivada do fetch.
        self._archive_online_enabled = archive_online

        self.amendments: list[dict[str, Any]] = []
        self.proposed_rels: list[dict[str, Any]] = []
        # Cache: (autor_nome, ano, pagina) -> (payload, url, snapshot_uri)
        self._pages: list[
            tuple[str, str, int, int, list[dict[str, Any]], str, str | None]
        ] = []  # (legislator_id, autor_nome, ano, pagina, items, page_url, snapshot_uri)
        # Deputados GO descobertos no grafo.
        self._targets: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # extract — descobre deputados GO + pagina o endpoint /emendas
    # ------------------------------------------------------------------

    def _discover_targets(self) -> list[dict[str, Any]]:
        """Lê do grafo os deputados federais GO alvo do fetch."""
        query = (
            "MATCH (p:FederalLegislator {uf: $uf}) "
            "RETURN p.legislator_id AS legislator_id, "
            "       p.id_camara AS id_camara, "
            "       p.name AS name"
        )
        targets: list[dict[str, Any]] = []
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                result = session.run(query, {"uf": _TARGET_UF})
                for record in result:
                    name = record.get("name") or ""
                    if not name:
                        continue
                    targets.append(
                        {
                            "legislator_id": str(
                                record.get("legislator_id") or "",
                            ),
                            "id_camara": str(record.get("id_camara") or ""),
                            "name": str(name),
                        },
                    )
        except Exception as exc:  # noqa: BLE001 — log and continue
            logger.warning(
                "[emendas_parlamentares_go] failed to read FederalLegislator "
                "targets: %s", exc,
            )
        if self.limit is not None:
            targets = targets[: self.limit]
        return targets

    def _fetch_author_year(
        self,
        client: httpx.Client,
        api_key: str,
        autor_nome: str,
        ano: int,
    ) -> list[tuple[int, list[dict[str, Any]], str, str | None]]:
        """Pagina ``/emendas?nomeAutor=...&ano=...`` e arquiva cada página.

        Retorna lista de ``(pagina, items, page_url, snapshot_uri)``.
        ``snapshot_uri`` é ``None`` no caminho offline (``archive_online=False``)
        — opt-in preservado no contrato de ``attach_provenance``.
        """
        pages: list[tuple[int, list[dict[str, Any]], str, str | None]] = []
        for pagina in range(1, _MAX_PAGES_PER_QUERY + 1):
            # Valores são todos ``str | int``, compatíveis com httpx.QueryParams.
            params: dict[str, str | int] = {
                "nomeAutor": autor_nome,
                "ano": ano,
                "pagina": pagina,
            }
            headers = {
                "chave-api-dados": api_key,
                "Accept": "application/json",
            }
            try:
                resp = client.get(
                    _EMENDAS_ENDPOINT, params=params, headers=headers,
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    "[emendas_parlamentares_go] HTTP error %s/%d pag %d: %s",
                    autor_nome, ano, pagina, exc,
                )
                break
            if resp.status_code == 401:
                logger.error(
                    "[emendas_parlamentares_go] TRANSPARENCIA_API_KEY invalida "
                    "(401) — abortar pra nao mascarar problema de credencial.",
                )
                raise RuntimeError(
                    "TRANSPARENCIA_API_KEY invalida (401 no Portal da "
                    "Transparencia /emendas)",
                )
            if resp.status_code >= 400:
                logger.warning(
                    "[emendas_parlamentares_go] %s/%d pag %d -> status %d",
                    autor_nome, ano, pagina, resp.status_code,
                )
                break
            page_url = str(resp.request.url)
            content_type = resp.headers.get("content-type", _JSON_CONTENT_TYPE)
            snapshot_uri: str | None = None
            if self._archive_online_enabled:
                snapshot_uri = archive_fetch(
                    url=page_url,
                    content=resp.content,
                    content_type=content_type,
                    run_id=self.run_id,
                    source_id=self.source_id,
                )
            try:
                payload = resp.json()
            except (json.JSONDecodeError, ValueError) as exc:
                logger.warning(
                    "[emendas_parlamentares_go] JSON decode error %s/%d "
                    "pag %d: %s", autor_nome, ano, pagina, exc,
                )
                break
            # A API devolve uma lista crua (não um envelope). Toleramos dict
            # com campo ``dados`` pra futuro-proofing.
            items: list[dict[str, Any]] = []
            if isinstance(payload, list):
                items = [p for p in payload if isinstance(p, dict)]
            elif isinstance(payload, dict):
                raw_items = payload.get("dados") or []
                if isinstance(raw_items, list):
                    items = [p for p in raw_items if isinstance(p, dict)]
            pages.append((pagina, items, page_url, snapshot_uri))
            if len(items) < _PAGE_SIZE_THRESHOLD:
                break
        if len(pages) >= _MAX_PAGES_PER_QUERY:
            logger.warning(
                "[emendas_parlamentares_go] pagination cap (%d) hit on "
                "%s/%d", _MAX_PAGES_PER_QUERY, autor_nome, ano,
            )
        return pages

    def extract(self) -> None:
        from bracc_etl.secrets import SecretNotFoundError, load_secret

        try:
            api_key = load_secret("transparencia-key", env_fallback=_ENV_VAR)
        except SecretNotFoundError as exc:
            raise ValueError(
                f"{_ENV_VAR} obrigatoria para emendas_parlamentares_go "
                "(ou configure GCP_PROJECT_ID + Secret Manager)",
            ) from exc

        self._targets = self._discover_targets()
        if not self._targets:
            logger.info(
                "[emendas_parlamentares_go] no FederalLegislator {uf:'GO'} "
                "nodes in graph; run camara_politicos_go first",
            )
            self.rows_in = 0
            return

        total_pages = 0
        with self._http_client_factory() as client:
            for target in self._targets:
                autor_nome = target["name"]
                legislator_id = target["legislator_id"] or (
                    f"camara_{target['id_camara']}"
                )
                for ano in range(self.start_year, self.end_year + 1):
                    try:
                        pages = self._fetch_author_year(
                            client, api_key, autor_nome, ano,
                        )
                    except RuntimeError:
                        # Credencial inválida: já logado; propaga pra quem
                        # chamou ``run()`` registrar quality_fail.
                        raise
                    for pagina, items, page_url, snapshot_uri in pages:
                        self._pages.append(
                            (
                                legislator_id,
                                autor_nome,
                                ano,
                                pagina,
                                items,
                                page_url,
                                snapshot_uri,
                            ),
                        )
                        total_pages += 1

        self.rows_in = sum(len(p[4]) for p in self._pages)
        logger.info(
            "[emendas_parlamentares_go] extracted %d pages (%d items) from %d targets",
            total_pages, self.rows_in, len(self._targets),
        )

    # ------------------------------------------------------------------
    # transform — produz Amendment nodes + PROPOS rels com proveniência
    # ------------------------------------------------------------------

    def transform(self) -> None:
        for (
            legislator_id,
            autor_nome,
            _ano_query,
            _pagina,
            items,
            page_url,
            snapshot_uri,
        ) in self._pages:
            autor_normalizado = normalize_name(autor_nome)
            for item in items:
                fields = _extract_fields(item)
                aid = _amendment_id(
                    autor_normalizado,
                    fields["ano"],
                    fields["numero"] or None,
                    fields["valor_empenhado"],
                )
                node_row = self.attach_provenance(
                    {
                        "amendment_id": aid,
                        "tipo": fields["tipo"],
                        "funcao": fields["funcao"],
                        "municipio": fields["municipio"],
                        "uf": fields["uf"],
                        "valor_empenhado": fields["valor_empenhado"],
                        "valor_pago": fields["valor_pago"],
                        "ano": fields["ano"],
                        "autor_nome": autor_normalizado,
                    },
                    record_id=aid,
                    record_url=page_url,
                    snapshot_uri=snapshot_uri,
                )
                self.amendments.append(node_row)

                rel_row = self.attach_provenance(
                    {
                        "source_key": legislator_id,
                        "target_key": aid,
                        "ano": fields["ano"],
                        "valor_empenhado": fields["valor_empenhado"],
                        "valor_pago": fields["valor_pago"],
                    },
                    record_id=aid,
                    record_url=page_url,
                    snapshot_uri=snapshot_uri,
                )
                self.proposed_rels.append(rel_row)

        self.amendments = deduplicate_rows(self.amendments, ["amendment_id"])
        self.proposed_rels = deduplicate_rows(
            self.proposed_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = len(self.amendments) + len(self.proposed_rels)
        logger.info(
            "[emendas_parlamentares_go] transformed %d amendments, %d rels",
            len(self.amendments), len(self.proposed_rels),
        )

    # ------------------------------------------------------------------
    # load — Amendment nodes + PROPOS rels
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.amendments:
            logger.info("[emendas_parlamentares_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes(
            "Amendment",
            self.amendments,
            key_field="amendment_id",
        )
        if self.proposed_rels:
            loader.load_relationships(
                rel_type="PROPOS",
                rows=self.proposed_rels,
                source_label="FederalLegislator",
                source_key="legislator_id",
                target_label="Amendment",
                target_key="amendment_id",
            )

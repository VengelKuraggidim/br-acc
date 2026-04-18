"""Camara dos Deputados — GO federal legislators pipeline (script_download).

Substitui o live-call que hoje o Flask (`backend/app.py::/politico`) faz
direto na API da Câmara. O pipeline ingere periodicamente os **deputados
federais eleitos por Goiás** + o consolidado de despesas CEAP no grafo,
carimbando `ProvenanceBlock` + `source_snapshot_uri` em cada nó/relação
via a camada de archival (``bracc_etl.archival.archive_fetch``).

Fontes (todas públicas, sem auth):

* ``GET /api/v2/deputados?siglaUf=GO`` — lista de deputados federais GO
  (paginada por ``links.next``).
* ``GET /api/v2/deputados/{id}`` — detalhe completo (CPF, gabinete,
  status, email, foto).
* ``GET /api/v2/deputados/{id}/despesas?ano=YYYY`` — CEAP ano a ano
  (paginada por ``links.next``). Histórico padrão: ano corrente até
  ``_DEFAULT_START_YEAR`` (2020).

Schema no grafo:

* Nó ``:FederalLegislator`` — espelha o shape de ``StateLegislator`` (ALEGO)
  porém rotulado separadamente pra permitir filtrar escopo federal vs
  estadual em queries (``CANDIDATO_EM`` continua sendo o caminho canônico
  pra contar federais, mas rotular o nó dá acesso direto à ingestão da
  Câmara e evita ambigüidade com `StateLegislator` da ALEGO que representa
  deputados estaduais).  Trade-off documentado: uma label nova é menos
  invasiva que mudar o schema ALEGO pra carregar um `scope`; consistência
  com outros rótulos existentes (``GoVereador``, ``StateEmployee``).
* Nó ``:LegislativeExpense`` — reaproveita a label já usada pelo pipeline
  ALEGO (deputados estaduais), diferenciada via propriedades ``scope`` e
  ``source_id``. Isso viabiliza queries unificadas "gastos de legisladores".
* Rel ``(:FederalLegislator)-[:INCURRED]->(:LegislativeExpense)`` —
  carrega ``tipo='CEAP'``, ``ano``, ``mes`` nas props.

CPF: deputado federal é agente público cujo CPF sai no DOU — é dado
público por força de publicação oficial. Mesmo assim seguimos o padrão
defensivo do ALEGO (``mask_cpf``) pra não regredir LGPD. Quando a API
pública (``/api/v1/politico``) quiser expor o CPF cru, basta reler da
archival.

Cadência recomendada (registry):
* Cadastro (``/deputados`` + detalhes) → semanal (baixo volume, alto
  churn quando há mudança de mandato / reeleições).
* Despesas CEAP → mensal (a Câmara consolida o mês anterior até meados
  do mês seguinte).

Scheduler fica fora do escopo deste pipeline — a trilha de automação é
chamada pelo operador via ``cron`` ou pelo orquestrador externo.
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
from bracc_etl.transforms import (
    deduplicate_rows,
    mask_cpf,
    normalize_name,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Câmara dos Deputados — API pública (https://dadosabertos.camara.leg.br).
_API_BASE = "https://dadosabertos.camara.leg.br/api/v2"
_DEFAULT_HEADERS = {"Accept": "application/json"}
_HTTP_TIMEOUT = 30.0
_PAGE_SIZE = 100

# Target scope: só Goiás (alinha com Fiscal Cidadão — escopo estadual).
_TARGET_UF = "GO"

# Histórico default das despesas CEAP: de 2020 até o ano corrente.
# Valor escolhido pra cobrir pelo menos um mandato completo sem puxar
# duas décadas de histórico em toda rodada.
_DEFAULT_START_YEAR = 2020

# Content-type que a Câmara devolve (``application/json;charset=UTF-8``).
_JSON_CONTENT_TYPE = "application/json"

_SOURCE_ID_CADASTRO = "camara_deputados"
_SOURCE_ID_CEAP = "camara_deputados_ceap"


def _expense_id(deputy_id: str, ano: int, mes: int, doc: str, valor: str) -> str:
    """Content-addressed ID pra despesa CEAP (estável entre re-runs)."""
    raw = f"ceap_{deputy_id}_{ano}_{mes}_{doc}_{valor}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _follow_pagination(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    run_id: str,
    source_id: str,
    max_pages: int = 100,
) -> list[tuple[list[dict[str, Any]], str, str]]:
    """Walk through Câmara v2 pagination, archival-ing each page payload.

    Returns a list of ``(dados, page_url, snapshot_uri)`` tuples — one per
    page fetched — so callers can attach provenance per-page. Archival is
    content-addressed so idempotent: re-running just hits the cache.
    """
    pages: list[tuple[list[dict[str, Any]], str, str]] = []
    current_url: str | None = url
    current_params = params
    page_num = 0
    while current_url and page_num < max_pages:
        resp = client.get(current_url, params=current_params, headers=_DEFAULT_HEADERS)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", _JSON_CONTENT_TYPE)
        snapshot_uri = archive_fetch(
            url=str(resp.request.url),
            content=resp.content,
            content_type=content_type,
            run_id=run_id,
            source_id=source_id,
        )
        try:
            payload = resp.json()
        except json.JSONDecodeError as exc:
            logger.warning(
                "[camara_politicos_go] JSON decode error on %s: %s",
                resp.request.url, exc,
            )
            break
        dados = payload.get("dados") or []
        pages.append((
            [d for d in dados if isinstance(d, dict)],
            str(resp.request.url),
            snapshot_uri,
        ))
        # Seguir ``links.next`` se a Câmara informa. Sem params extras —
        # a URL já carrega query string paginada completa.
        next_url: str | None = None
        for link in payload.get("links") or []:
            if isinstance(link, dict) and link.get("rel") == "next":
                next_url = str(link.get("href") or "") or None
                break
        current_url = next_url
        current_params = None  # subsequent pages já vêm com query completa
        page_num += 1
    if page_num >= max_pages:
        logger.warning(
            "[camara_politicos_go] pagination cap (%d) hit on %s",
            max_pages, url,
        )
    return pages


def _fetch_deputy_detail(
    client: httpx.Client,
    deputy_id: str,
    *,
    run_id: str,
    source_id: str,
) -> tuple[dict[str, Any], str, str] | None:
    """Fetch ``/deputados/{id}`` and archive the payload.

    Returns ``(detail_dict, deputy_url, snapshot_uri)`` or ``None`` on
    any HTTP / JSON error (logged — upstream continues with the listing
    shape, which still has the minimum props).
    """
    url = f"{_API_BASE}/deputados/{deputy_id}"
    try:
        resp = client.get(url, headers=_DEFAULT_HEADERS)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "[camara_politicos_go] deputy detail %s failed: %s",
            deputy_id, exc,
        )
        return None
    content_type = resp.headers.get("content-type", _JSON_CONTENT_TYPE)
    snapshot_uri = archive_fetch(
        url=str(resp.request.url),
        content=resp.content,
        content_type=content_type,
        run_id=run_id,
        source_id=source_id,
    )
    try:
        payload = resp.json()
    except json.JSONDecodeError as exc:
        logger.warning(
            "[camara_politicos_go] deputy detail %s decode error: %s",
            deputy_id, exc,
        )
        return None
    dados = payload.get("dados")
    if not isinstance(dados, dict):
        return None
    return dados, str(resp.request.url), snapshot_uri


class CamaraPoliticosGoPipeline(Pipeline):
    """Ingere deputados federais GO + despesas CEAP no grafo.

    Scope: **apenas Goiás** (``siglaUf=GO``) — consistente com o produto
    Fiscal Cidadão.

    Cadência recomendada (não é responsabilidade do pipeline agendar):
    * Cadastro: semanal
    * CEAP: mensal
    """

    name = "camara_politicos_go"
    source_id = _SOURCE_ID_CADASTRO

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        # Campos específicos deste pipeline não podem subir pra ``Pipeline.__init__``.
        start_year = int(kwargs.pop("start_year", _DEFAULT_START_YEAR))
        end_year = int(
            kwargs.pop("end_year", datetime.now(tz=UTC).year),
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
        self.start_year = start_year
        self.end_year = end_year
        # Override pra testes injetarem cliente httpx mockado.
        self._http_client_factory = http_client_factory

        self.legislators: list[dict[str, Any]] = []
        self.expenses: list[dict[str, Any]] = []
        self.expense_rels: list[dict[str, Any]] = []
        # CEAP tem source_id distinto de cadastro (``camara_deputados_ceap``).
        # Geramos um run_id separado pro bucket archival continuar coerente
        # por source_id e o ProvenanceBlock das despesas referenciar a fonte
        # correta.
        self._ceap_run_id = (
            f"{_SOURCE_ID_CEAP}_{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S')}"
        )
        # Mapa ``deputy_id -> snapshot_uri`` do detalhe — usado pra
        # carimbar ``source_snapshot_uri`` no nó do legislador com o
        # snapshot mais rico (``/deputados/{id}``), não só o da listagem.
        self._detail_snapshot_by_id: dict[str, str] = {}
        self._listing_snapshot_by_id: dict[str, str] = {}

    def _stamp_ceap_provenance(
        self,
        row: dict[str, Any],
        *,
        record_id: object,
        record_url: str,
        snapshot_uri: str,
    ) -> dict[str, Any]:
        """Carimba proveniência CEAP (source_id distinto do cadastro).

        ``Pipeline.attach_provenance`` usa ``self.source_id`` — que pro
        pipeline é o cadastro. CEAP é uma fonte lógica separada no
        ``source_registry_br_v1.csv`` (separa cadência e rastreabilidade),
        então carimbamos manualmente os 6 campos do contrato de
        proveniência (ver ``docs/provenance.md``).
        """
        normalized_record_id = "" if record_id in (None, "") else str(record_id)
        stamped: dict[str, Any] = {
            **row,
            "source_id": _SOURCE_ID_CEAP,
            "source_record_id": normalized_record_id,
            "source_url": record_url,
            "ingested_at": self._provenance_ingested_at,
            "run_id": self._ceap_run_id,
        }
        if snapshot_uri:
            stamped["source_snapshot_uri"] = snapshot_uri
        return stamped

    # ------------------------------------------------------------------
    # extract — faz os GETs, arquiva cada payload, guarda dados brutos
    # ------------------------------------------------------------------

    def extract(self) -> None:
        """Baixa deputados (listagem + detalhe) e despesas CEAP.

        Cada fetch chama ``archive_fetch`` → URI é guardada pra carimbar
        ``source_snapshot_uri`` em cada nó/relação na fase ``transform``.
        """
        deputies_by_id: dict[str, dict[str, Any]] = {}
        expense_pages: list[
            tuple[str, int, list[dict[str, Any]], str, str]
        ] = []  # (deputy_id, ano, dados, page_url, snapshot_uri)

        with self._http_client_factory() as client:
            # --- Listagem paginada (/deputados?siglaUf=GO) ---
            listing_pages = _follow_pagination(
                client,
                f"{_API_BASE}/deputados",
                params={
                    "siglaUf": _TARGET_UF,
                    "ordem": "ASC",
                    "ordenarPor": "nome",
                    "itens": _PAGE_SIZE,
                },
                run_id=self.run_id,
                source_id=_SOURCE_ID_CADASTRO,
            )
            listing_rows: list[tuple[dict[str, Any], str, str]] = []
            for dados, page_url, snapshot_uri in listing_pages:
                for dep in dados:
                    dep_id = dep.get("id")
                    if dep_id is None:
                        continue
                    listing_rows.append((dep, page_url, snapshot_uri))

            # Clip pra limite de smoke (cada item = um deputado).
            if self.limit is not None:
                listing_rows = listing_rows[: self.limit]

            # --- Detalhe por deputado (/deputados/{id}) ---
            for dep_listing, page_url, listing_snapshot in listing_rows:
                dep_id = str(dep_listing.get("id"))
                self._listing_snapshot_by_id[dep_id] = listing_snapshot
                detail = _fetch_deputy_detail(
                    client,
                    dep_id,
                    run_id=self.run_id,
                    source_id=_SOURCE_ID_CADASTRO,
                )
                merged = dict(dep_listing)
                if detail is not None:
                    detail_dict, detail_url, detail_snapshot = detail
                    merged["_detail"] = detail_dict
                    merged["_detail_url"] = detail_url
                    self._detail_snapshot_by_id[dep_id] = detail_snapshot
                else:
                    merged["_detail"] = {}
                    merged["_detail_url"] = page_url
                merged["_listing_url"] = page_url
                deputies_by_id[dep_id] = merged

            # --- Despesas CEAP por deputado, ano a ano ---
            for dep_id in deputies_by_id:
                for ano in range(self.start_year, self.end_year + 1):
                    try:
                        pages = _follow_pagination(
                            client,
                            f"{_API_BASE}/deputados/{dep_id}/despesas",
                            params={"ano": ano, "itens": _PAGE_SIZE},
                            run_id=self.run_id,
                            source_id=_SOURCE_ID_CEAP,
                        )
                    except httpx.HTTPError as exc:
                        logger.warning(
                            "[camara_politicos_go] CEAP %s/%d failed: %s",
                            dep_id, ano, exc,
                        )
                        continue
                    for dados, page_url, snapshot_uri in pages:
                        expense_pages.append(
                            (dep_id, ano, dados, page_url, snapshot_uri),
                        )

        self._deputies_by_id = deputies_by_id
        self._expense_pages = expense_pages
        self.rows_in = (
            len(deputies_by_id)
            + sum(len(page[2]) for page in expense_pages)
        )
        logger.info(
            "[camara_politicos_go] extracted %d deputies, %d CEAP pages",
            len(deputies_by_id), len(expense_pages),
        )

    # ------------------------------------------------------------------
    # transform — produz os dicts finais + carimba proveniência
    # ------------------------------------------------------------------

    def transform(self) -> None:
        for dep_id, data in self._deputies_by_id.items():
            detail = data.get("_detail") or {}
            dados_pessoais = detail.get("ultimoStatus") or {}
            gabinete = dados_pessoais.get("gabinete") or {}
            # Fallback: campos da listagem ficam disponíveis quando o
            # /detail falha — garante que o deputado ainda aparece no grafo.
            nome = normalize_name(
                dados_pessoais.get("nomeEleitoral")
                or data.get("nome")
                or "",
            )
            partido = str(
                dados_pessoais.get("siglaPartido")
                or data.get("siglaPartido")
                or "",
            ).strip()
            uf_elected = str(
                dados_pessoais.get("siglaUf")
                or data.get("siglaUf")
                or _TARGET_UF,
            ).strip().upper()
            if uf_elected != _TARGET_UF:
                # Safety net: a API já devolve filtrado, mas se algum
                # detail trouxer outra UF (reeleição, carga cruzada),
                # mantemos o escopo GO-only.
                logger.debug(
                    "[camara_politicos_go] skipping non-GO deputy %s (uf=%s)",
                    dep_id, uf_elected,
                )
                continue
            cpf_raw = str(detail.get("cpf") or "")
            cpf_masked = mask_cpf(cpf_raw) if strip_document(cpf_raw) else ""
            email = str(gabinete.get("email") or data.get("email") or "").strip()
            url_foto = str(
                dados_pessoais.get("urlFoto") or data.get("urlFoto") or "",
            ).strip()
            situacao = str(dados_pessoais.get("situacao") or "").strip()
            legislatura = dados_pessoais.get("idLegislatura")

            detail_url = str(data.get("_detail_url") or "")
            snapshot_uri = self._detail_snapshot_by_id.get(
                dep_id,
                self._listing_snapshot_by_id.get(dep_id, ""),
            ) or None

            node_row = self.attach_provenance(
                {
                    "id_camara": dep_id,
                    "legislator_id": f"camara_{dep_id}",
                    "name": nome,
                    "cpf": cpf_masked,
                    "partido": partido,
                    "uf": _TARGET_UF,
                    "email": email,
                    "url_foto": url_foto,
                    "situacao": situacao,
                    "legislatura_atual": (
                        int(legislatura)
                        if isinstance(legislatura, int)
                        else (
                            int(legislatura)
                            if isinstance(legislatura, str)
                            and legislatura.isdigit()
                            else 0
                        )
                    ),
                    "scope": "federal",
                    "source": _SOURCE_ID_CADASTRO,
                },
                record_id=dep_id,
                record_url=detail_url or None,
                snapshot_uri=snapshot_uri,
            )
            self.legislators.append(node_row)

        self.legislators = deduplicate_rows(self.legislators, ["id_camara"])

        # --- Despesas CEAP ---
        for dep_id, ano, dados, page_url, snapshot_uri in self._expense_pages:
            # Só processa despesas de deputados que entraram no grafo.
            if not any(leg["id_camara"] == dep_id for leg in self.legislators):
                continue
            for row in dados:
                valor_liquido = row.get("valorLiquido") or 0
                try:
                    valor_float = float(valor_liquido)
                except (TypeError, ValueError):
                    valor_float = 0.0
                if valor_float <= 0:
                    continue
                mes = row.get("mes") or 0
                try:
                    mes_int = int(mes)
                except (TypeError, ValueError):
                    mes_int = 0
                fornecedor_cnpj_raw = str(row.get("cnpjCpfFornecedor") or "")
                fornecedor_cnpj = strip_document(fornecedor_cnpj_raw)
                fornecedor_nome = normalize_name(
                    str(row.get("nomeFornecedor") or ""),
                )
                documento = str(row.get("numDocumento") or "").strip()
                tipo_despesa = str(row.get("tipoDespesa") or "").strip()
                eid = _expense_id(
                    dep_id,
                    int(ano),
                    mes_int,
                    fornecedor_cnpj or "sem_doc",
                    f"{valor_float:.2f}",
                )
                expense_node = self._stamp_ceap_provenance(
                    {
                        "expense_id": eid,
                        "tipo": "CEAP",
                        "tipo_despesa": tipo_despesa,
                        "ano": int(ano),
                        "mes": mes_int,
                        "valor_liquido": valor_float,
                        "documento": documento,
                        "fornecedor_cnpj": fornecedor_cnpj,
                        "fornecedor_nome": fornecedor_nome,
                        "deputy_id_camara": dep_id,
                        "scope": "federal",
                        "uf": _TARGET_UF,
                        "source": _SOURCE_ID_CEAP,
                    },
                    record_id=eid,
                    record_url=page_url,
                    snapshot_uri=snapshot_uri,
                )
                self.expenses.append(expense_node)
                rel_row = self._stamp_ceap_provenance(
                    {
                        "source_key": f"camara_{dep_id}",
                        "target_key": eid,
                        "tipo": "CEAP",
                        "ano": int(ano),
                        "mes": mes_int,
                        "valor_liquido": valor_float,
                        "documento": documento,
                        "fornecedor_cnpj": fornecedor_cnpj,
                        "fornecedor_nome": fornecedor_nome,
                    },
                    record_id=eid,
                    record_url=page_url,
                    snapshot_uri=snapshot_uri,
                )
                self.expense_rels.append(rel_row)

        self.expenses = deduplicate_rows(self.expenses, ["expense_id"])
        self.expense_rels = deduplicate_rows(
            self.expense_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = (
            len(self.legislators)
            + len(self.expenses)
            + len(self.expense_rels)
        )
        logger.info(
            "[camara_politicos_go] transformed %d legislators, %d expenses, %d rels",
            len(self.legislators),
            len(self.expenses),
            len(self.expense_rels),
        )

    # ------------------------------------------------------------------
    # load — grava no grafo
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not (self.legislators or self.expenses):
            logger.warning("[camara_politicos_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        if self.legislators:
            loader.load_nodes(
                "FederalLegislator",
                self.legislators,
                key_field="id_camara",
            )
        if self.expenses:
            loader.load_nodes(
                "LegislativeExpense",
                self.expenses,
                key_field="expense_id",
            )
        if self.expense_rels:
            loader.load_relationships(
                rel_type="INCURRED",
                rows=self.expense_rels,
                source_label="FederalLegislator",
                source_key="legislator_id",
                target_label="LegislativeExpense",
                target_key="expense_id",
            )

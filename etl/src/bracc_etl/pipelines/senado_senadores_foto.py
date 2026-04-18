"""Senado Federal — fotos oficiais dos senadores em exercicio (GO).

Pipeline equivalente ao ``camara_politicos_go`` para deputados federais,
mas escopado **somente as fotos oficiais** dos senadores em exercicio
representando Goias. Preserva binario JPG via ``archive_fetch`` para que
o PWA tenha um snapshot rastreavel mesmo se o portal do Senado retirar
a imagem do ar.

Fontes (todas publicas, sem auth):

* ``GET /senador/lista/atual`` (XML) — listagem de parlamentares em
  exercicio. Filtramos client-side por ``Parlamentar/IdentificacaoParlamentar/UfParlamentar="GO"``
  pois o endpoint nao aceita ``?uf=`` (somente a versao por legislatura).
* ``UrlFotoParlamentar`` — URL canonica vinda do XML (com fallback para
  ``http://www.senado.leg.br/senadores/img/fotos-oficiais/senador{codigo}.jpg``,
  padrao estavel observado no portal). O fetch segue redirects HTTP→HTTPS
  e arquiva o binario via ``archive_fetch``.

Schema no grafo:

* No ``:Senator`` — espelha o shape de ``FederalLegislator`` (Camara) e
  carrega ``id_senado``, ``senator_id`` (``"senado_{codigo}"``), ``name``,
  ``partido``, ``uf="GO"``, ``url_foto``, ``foto_url``, ``foto_snapshot_uri``,
  ``foto_content_type``, ``scope="senate"``. Decisao de label documentada
  no commit body.

CPF: o endpoint ``/senador/{codigo}`` parou de publicar CPF (ver nota em
``bracc_etl.pipelines.senado``); por isso este pipeline NAO carrega CPF
no no — a chave estavel e ``id_senado``. Quando a fonte voltar a
expor CPF, basta adicionar enrichment opt-in nesta mesma pipeline.

Cadencia recomendada (registry):
* Foto raramente muda — semanal e suficiente. Idempotencia do archival
  garante que reruns nao re-escrevem disco quando o binario nao mudou.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from xml.etree import ElementTree as ET

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import deduplicate_rows, normalize_name

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Senado Dados Abertos — endpoint de parlamentares em exercicio.
_SENADO_API_BASE = "https://legis.senado.leg.br/dadosabertos"
_LISTA_ATUAL_URL = f"{_SENADO_API_BASE}/senador/lista/atual"

# Padrao estavel observado no portal: ``senador{CodigoParlamentar}.jpg``.
# Usado como fallback quando ``UrlFotoParlamentar`` no XML vier vazio.
_FOTO_URL_FALLBACK = (
    "http://www.senado.leg.br/senadores/img/fotos-oficiais/senador{codigo}.jpg"
)

_DEFAULT_HEADERS = {"Accept": "application/xml"}
_HTTP_TIMEOUT = 30.0
_XML_CONTENT_TYPE = "application/xml"

# Escopo Fiscal Cidadao: somente Goias.
_TARGET_UF = "GO"

# source_id principal (registrado no source_registry_br_v1.csv) — o valor
# que o pipeline produz no grafo e a foto arquivada.
_SOURCE_ID_FOTO = "senado_senadores_foto"

# source_id interno do bucket de archival da listagem XML. Nao registrado
# (mesmo padrao do ``camara_deputados_foto`` no pipeline da Camara). Mantem
# bucket separado pra nao misturar XML com binarios JPG no archival.
_SOURCE_ID_LISTA = "senado_senadores_lista_atual"

# Content-types aceitos pro binario da foto. Qualquer outro valor (ex.:
# ``text/html`` de erro de CDN) e descartado e a foto nao e arquivada.
_PHOTO_CONTENT_TYPES = frozenset({"image/png", "image/jpeg", "image/jpg"})


def _fetch_lista_atual(
    client: httpx.Client,
    *,
    run_id: str,
) -> tuple[bytes, str, str] | None:
    """Fetch the live ``/senador/lista/atual`` XML and archive the payload.

    Returns ``(content, request_url, snapshot_uri)`` or ``None`` on HTTP
    failure (logged — pipeline continues with no senators to ingest).
    """
    try:
        resp = client.get(_LISTA_ATUAL_URL, headers=_DEFAULT_HEADERS)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "[senado_senadores_foto] lista/atual fetch failed: %s", exc,
        )
        return None
    content_type = resp.headers.get("content-type", _XML_CONTENT_TYPE)
    snapshot_uri = archive_fetch(
        url=str(resp.request.url),
        content=resp.content,
        content_type=content_type,
        run_id=run_id,
        source_id=_SOURCE_ID_LISTA,
    )
    return resp.content, str(resp.request.url), snapshot_uri


def _parse_senadores_go(xml_bytes: bytes) -> list[dict[str, str]]:
    """Parse the ``ListaParlamentarEmExercicio`` XML and keep only GO senators.

    The endpoint returns every senator currently in exercise; we filter
    client-side on ``IdentificacaoParlamentar/UfParlamentar == "GO"``.
    Returns plain dicts with ``codigo``, ``nome``, ``nome_completo``,
    ``partido`` and ``url_foto`` (may be empty — caller falls back to the
    portal pattern).
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        logger.warning(
            "[senado_senadores_foto] XML parse error: %s", exc,
        )
        return []
    out: list[dict[str, str]] = []
    for parlamentar in root.iter("Parlamentar"):
        ident = parlamentar.find("IdentificacaoParlamentar")
        if ident is None:
            continue
        uf = (ident.findtext("UfParlamentar") or "").strip().upper()
        if uf != _TARGET_UF:
            continue
        codigo = (ident.findtext("CodigoParlamentar") or "").strip()
        if not codigo:
            continue
        out.append({
            "codigo": codigo,
            "nome": (ident.findtext("NomeParlamentar") or "").strip(),
            "nome_completo": (
                ident.findtext("NomeCompletoParlamentar") or ""
            ).strip(),
            "partido": (ident.findtext("SiglaPartidoParlamentar") or "").strip(),
            "url_foto": (ident.findtext("UrlFotoParlamentar") or "").strip(),
        })
    return out


def _fetch_senator_photo(
    client: httpx.Client,
    photo_url: str,
    *,
    run_id: str,
) -> tuple[str, str] | None:
    """Fetch the binary photo from the Senado portal and archive it.

    Returns ``(snapshot_uri, normalized_content_type)`` or ``None`` if the
    URL is empty, the GET fails, or the response doesn't look like an
    image (e.g. CDN served an HTML error page). Failures are logged and
    swallowed — the node still lands in the graph without snapshot.
    """
    if not photo_url:
        return None
    try:
        resp = client.get(photo_url, headers=_DEFAULT_HEADERS)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "[senado_senadores_foto] photo fetch %s failed: %s",
            photo_url, exc,
        )
        return None
    content_type_raw = resp.headers.get("content-type", "")
    primary = content_type_raw.split(";", 1)[0].strip().lower()
    if primary not in _PHOTO_CONTENT_TYPES:
        logger.warning(
            "[senado_senadores_foto] photo %s returned non-image "
            "content-type %r",
            photo_url, content_type_raw,
        )
        return None
    # Normaliza ``image/jpg`` (alguns CDNs devolvem assim) pra ``image/jpeg``
    # padrao IANA — consistente com o pipeline da Camara.
    normalized = "image/jpeg" if primary == "image/jpg" else primary
    try:
        snapshot_uri = archive_fetch(
            url=str(resp.request.url),
            content=resp.content,
            content_type=primary,
            run_id=run_id,
            source_id=_SOURCE_ID_FOTO,
        )
    except OSError as exc:
        logger.warning(
            "[senado_senadores_foto] photo archival %s failed: %s",
            photo_url, exc,
        )
        return None
    return snapshot_uri, normalized


class SenadoSenadoresFotoPipeline(Pipeline):
    """Ingere fotos oficiais dos senadores em exercicio de Goias.

    Scope: **apenas Goias** (filtrado client-side em
    ``Parlamentar/IdentificacaoParlamentar/UfParlamentar``).

    Cadencia recomendada (nao e responsabilidade do pipeline agendar):
    semanal — fotos raramente mudam, mas a listagem ``/senador/lista/atual``
    atualiza com troca de mandato/suplencia.
    """

    name = "senado_senadores_foto"
    source_id = _SOURCE_ID_FOTO

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        archive_photos = bool(kwargs.pop("archive_photos", True))
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
        # Opt-in por default — permite desligar em testes offline.
        self.archive_photos = archive_photos
        # Override pra testes injetarem cliente httpx mockado.
        self._http_client_factory = http_client_factory

        self.senators: list[dict[str, Any]] = []

        # Bucket de proveniencia pra listagem XML — usado pra carimbar o
        # ``source_snapshot_uri`` no no.
        self._lista_url: str = _LISTA_ATUAL_URL
        self._lista_snapshot_uri: str | None = None
        # Foto archival por codigo de parlamentar.
        self._photo_snapshot_by_codigo: dict[str, str] = {}
        self._photo_content_type_by_codigo: dict[str, str] = {}
        self._photo_url_used_by_codigo: dict[str, str] = {}
        # Senadores brutos extraidos do XML (antes do transform).
        self._raw_senators: list[dict[str, str]] = []

    # ------------------------------------------------------------------
    # extract — faz os GETs, arquiva cada payload, guarda dados brutos
    # ------------------------------------------------------------------

    def extract(self) -> None:
        """Baixa lista atual + foto de cada senador GO em exercicio.

        Cada fetch chama ``archive_fetch`` → URI e guardada pra carimbar
        ``source_snapshot_uri`` em cada no na fase ``transform``.
        """
        with self._http_client_factory() as client:
            lista = _fetch_lista_atual(client, run_id=self.run_id)
            if lista is None:
                logger.warning(
                    "[senado_senadores_foto] lista/atual indisponivel — "
                    "pipeline encerra sem senadores",
                )
                self._raw_senators = []
                return
            xml_bytes, lista_url, lista_snapshot = lista
            self._lista_url = lista_url
            self._lista_snapshot_uri = lista_snapshot

            senators = _parse_senadores_go(xml_bytes)
            if self.limit is not None:
                senators = senators[: self.limit]
            self._raw_senators = senators

            if not self.archive_photos:
                logger.info(
                    "[senado_senadores_foto] archive_photos=False — "
                    "pulando fetch de fotos",
                )
                self.rows_in = len(senators)
                return

            for sen in senators:
                codigo = sen["codigo"]
                photo_url = sen.get("url_foto") or _FOTO_URL_FALLBACK.format(
                    codigo=codigo,
                )
                self._photo_url_used_by_codigo[codigo] = photo_url
                photo_result = _fetch_senator_photo(
                    client, photo_url, run_id=self.run_id,
                )
                if photo_result is not None:
                    snapshot_uri, normalized_ct = photo_result
                    self._photo_snapshot_by_codigo[codigo] = snapshot_uri
                    self._photo_content_type_by_codigo[codigo] = normalized_ct

        self.rows_in = len(self._raw_senators)
        logger.info(
            "[senado_senadores_foto] extracted %d senators, %d fotos arquivadas",
            len(self._raw_senators),
            len(self._photo_snapshot_by_codigo),
        )

    # ------------------------------------------------------------------
    # transform — produz os dicts finais + carimba proveniencia
    # ------------------------------------------------------------------

    def transform(self) -> None:
        for sen in self._raw_senators:
            codigo = sen["codigo"]
            nome = normalize_name(
                sen.get("nome_completo") or sen.get("nome") or "",
            )
            partido = sen.get("partido") or ""
            url_foto = (
                self._photo_url_used_by_codigo.get(codigo)
                or sen.get("url_foto")
                or ""
            )
            foto_snapshot_uri = self._photo_snapshot_by_codigo.get(codigo) or None
            foto_content_type = (
                self._photo_content_type_by_codigo.get(codigo) or None
            )

            node_row = self.attach_provenance(
                {
                    "id_senado": codigo,
                    "senator_id": f"senado_{codigo}",
                    "name": nome,
                    "partido": partido,
                    "uf": _TARGET_UF,
                    "url_foto": url_foto,
                    "foto_url": url_foto or None,
                    "foto_snapshot_uri": foto_snapshot_uri,
                    "foto_content_type": foto_content_type,
                    "scope": "senate",
                    "source": _SOURCE_ID_FOTO,
                },
                record_id=codigo,
                record_url=self._lista_url,
                snapshot_uri=self._lista_snapshot_uri,
            )
            self.senators.append(node_row)

        self.senators = deduplicate_rows(self.senators, ["id_senado"])
        self.rows_loaded = len(self.senators)
        logger.info(
            "[senado_senadores_foto] transformed %d senators",
            len(self.senators),
        )

    # ------------------------------------------------------------------
    # load — grava no grafo
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.senators:
            logger.warning("[senado_senadores_foto] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes(
            "Senator",
            self.senators,
            key_field="id_senado",
        )

"""Wikidata — fallback de fotos pra ex-políticos GO já no grafo.

Pipelines como ``camara_politicos_go`` (deputados federais ativos),
``alego`` (deputados estaduais GO ativos) e ``camara_goiania`` (vereadores
de Goiânia) cobrem cargos *ativos*. Não há fonte oficial unificada pra
fotos de **ex-governadores, ex-senadores, ex-deputados federais/estaduais**
ou candidatos derrotados em ciclos passados — esses políticos aparecem no
grafo via TSE/CEAP/folha sem imagem.

Wikidata é o fallback universal: Marconi Perillo (Q6757791), Iris Rezende,
José Eliton, etc. todos têm Q-id e (geralmente) propriedade ``P18``
(image) com foto Creative Commons hospedada no Wikimedia Commons.

## Estratégia

1. ``extract()`` faz Cypher pra listar políticos GO no grafo que ainda
   não têm ``foto_url`` (campo carimbado pelos pipelines de cargo ativo).
   Cobre ``:FederalLegislator``, ``:StateLegislator`` e qualquer node com
   label ``:Person`` ligado a ``:Election`` GO via TSE.
2. Pra cada nome, faz um SPARQL no ``query.wikidata.org`` filtrando por:
   - ``wdt:P31 wd:Q5`` (instance of human)
   - ``wdt:P27 wd:Q155`` (cidadania=Brasil)
   - ``wdt:P106/wdt:P279*`` envolvendo ``Q82955`` (ocupação=politician
     ou subclasses como ``Q193391`` deputy, ``Q4175034`` governor, etc.)
   - label/alias casa com ``normalize_name(nome)``.
3. **Stop on ambiguidade**: se o SPARQL devolve >1 candidato, log warning
   e pula. Política do projeto é "nunca acusar/inventar" (CLAUDE.md §3),
   então atribuir Q-id errado pra alguém é pior que deixar sem foto.
4. Se exatamente 1 hit, busca ``Special:EntityData/Q{id}.json``, extrai
   ``P18`` (filename), faz GET em ``Special:FilePath/{filename}`` (que
   resolve em redirect pro binário JPEG/PNG). Os 3 fetches (SPARQL JSON,
   entity JSON, binário) são archival via :func:`archive_fetch`.

## Etiqueta Wikidata

A `query.wikidata.org` exige User-Agent identificável e tem rate limit
generoso mas não infinito. Seguimos a recomendação:

- User-Agent: ``FiscalCidadao/0.1 (https://github.com/VengelKuraggidim/fiscal-cidadao)``
- Throttle: ≥1s entre requests (cobre SPARQL + EntityData + FilePath).
- LIMIT 5 no SPARQL pra reduzir payload (filtro nome estrito limita
  resultado naturalmente; LIMIT 5 só guarda contra label muito comum).

## Schema no grafo

Não cria nodes — só faz ``SET`` em existentes. Propriedades carimbadas:

- ``foto_url``: URL pública do binário (``Special:FilePath/...``).
- ``foto_snapshot_uri``: URI archival relativa do binário.
- ``foto_content_type``: ``image/jpeg`` ou ``image/png``.
- ``wikidata_qid``: Q-id matched (auditoria + reuso futuro).
- Proveniência: ``foto_source_id``, ``foto_source_url``, ``foto_run_id``,
  ``foto_ingested_at``. Prefixo ``foto_*`` evita conflito com proveniência
  do node de cargo (a foto é fonte secundária; carga primária já tem
  source_id próprio).

## Cadência

Trimestral. Catálogo de Q-ids muda devagar, foto raramente é trocada,
mas ex-político novo entra no grafo a cada eleição (4 anos). Trimestral
cobre o pulso entre TSE refresh e atualizações esporádicas no Wikidata.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.transforms import normalize_name

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_SOURCE_ID = "wikidata_politicos_foto"

_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_ENTITY_BASE = "https://www.wikidata.org/wiki/Special:EntityData"
_COMMONS_FILEPATH = "https://commons.wikimedia.org/wiki/Special:FilePath"

# User-Agent identificavel é exigência da Wikimedia Foundation pro acesso
# automatizado (https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy).
_USER_AGENT = (
    "FiscalCidadao/0.1 (https://github.com/VengelKuraggidim/fiscal-cidadao)"
)
_DEFAULT_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": "application/sparql-results+json",
}
_HTTP_TIMEOUT = 30.0

# Throttle entre requests (em segundos). 1.0s cobre 3 requests por
# politico (~1 SPARQL + 1 entity + 1 imagem) num ritmo respeitoso.
_THROTTLE_SECONDS = 1.0

# Cap defensivo no batch — politicos sem foto sao finitos (~poucas
# centenas no horizonte), mas o cap ajuda smoke tests e abortos cedo.
_DEFAULT_BATCH_SIZE = 100

# Content-types aceitos pro binário da imagem (mesma lista de
# camara_politicos_go pra consistência).
_PHOTO_CONTENT_TYPES = frozenset({"image/png", "image/jpeg", "image/jpg"})

# JSON content-type devolvido pelo SPARQL endpoint e pelo EntityData.
_JSON_CONTENT_TYPE = "application/sparql-results+json"
_ENTITY_JSON_CONTENT_TYPE = "application/json"

# Cypher: lista nomes únicos de políticos GO sem foto. Considera os 3
# labels de pessoa que carregam o conceito de "político" no schema atual:
# - :FederalLegislator (camara_politicos_go) com uf=GO;
# - :StateLegislator (alego) — todos sao GO por construção;
# - :Person ligado a uma candidatura GO via TSE (politicos historicos).
# A query filtra por nome não vazio, sem foto preexistente, deduplica.
_DISCOVERY_QUERY = """
CALL {
    MATCH (n:FederalLegislator)
    WHERE n.uf = 'GO'
      AND coalesce(n.name, '') <> ''
      AND coalesce(n.foto_url, '') = ''
    RETURN n.name AS name, labels(n) AS labels, n.legislator_id AS key
UNION
    MATCH (n:StateLegislator)
    WHERE coalesce(n.name, '') <> ''
      AND coalesce(n.foto_url, '') = ''
    RETURN n.name AS name, labels(n) AS labels, n.legislator_id AS key
UNION
    MATCH (n:Person)-[:CANDIDATO_EM]->(:Election {uf: 'GO'})
    WHERE coalesce(n.name, '') <> ''
      AND coalesce(n.foto_url, '') = ''
    RETURN n.name AS name, labels(n) AS labels,
           coalesce(n.cpf, n.name) AS key
}
RETURN name, labels, key
ORDER BY name
LIMIT $batch_size
"""


def _build_sparql_query(name_normalized: str) -> str:
    """Monta SPARQL pra achar humano BR político por nome normalizado.

    Usa ``rdfs:label`` + ``skos:altLabel`` em pt e pt-br, comparado em
    upper+sem-accent (espelha o ``normalize_name`` do pipeline) via
    ``LCASE`` + funções string do SPARQL. Filtros:

    - ``P31 = Q5`` (humano)
    - ``P27 = Q155`` (cidadania Brasil)
    - ``P106 / P279*`` envolvendo ``Q82955`` (politician) — cobre
      especializações (deputado, senador, governador, prefeito, etc.)

    LIMIT 5 é cap defensivo: nome muito comum (ex.: "JOSE SILVA") pode
    bater em vários, mas se vier >1 o pipeline já pula (ambiguidade).
    O LIMIT só evita download bobo.
    """
    # Escape único: SPARQL string literals usam aspas duplas. O
    # ``normalize_name`` já tira acentos e força upper-case, então o
    # único caractere que precisa cuidado é a aspa dupla — um nome com
    # aspas é absurdo e seria descartado upstream, mas escapamos por
    # garantia.
    safe = name_normalized.replace('"', '\\"')
    return f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P27 wd:Q155 .
  ?item wdt:P106/wdt:P279* wd:Q82955 .
  {{
    ?item rdfs:label ?label .
    FILTER(LANG(?label) IN ("pt", "pt-br", "en"))
    FILTER(UCASE(STR(?label)) = "{safe}")
  }} UNION {{
    ?item skos:altLabel ?alt .
    FILTER(LANG(?alt) IN ("pt", "pt-br", "en"))
    FILTER(UCASE(STR(?alt)) = "{safe}")
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pt,en". }}
}}
LIMIT 5
""".strip()


def _strip_accents_upper(value: str) -> str:
    """Espelha o ``normalize_name`` do pipeline pra comparar com label SPARQL."""
    return normalize_name(value)


class WikidataPoliticosFotoPipeline(Pipeline):
    """Enriquece políticos GO existentes com foto do Wikidata/Commons.

    Não cria nodes — só faz ``SET`` em existentes. Não toca nodes que já
    têm ``foto_url`` (respeita o pipeline de cargo ativo, que é a fonte
    canônica). Quando o nome casa com >1 Q-id, pula com log — política
    do projeto é nunca atribuir identidade ambígua.

    Cadência recomendada: trimestral (catálogo Wikidata muda devagar).
    """

    name = "wikidata_politicos_foto"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        throttle_seconds: float = _THROTTLE_SECONDS,
        http_client_factory: Any = None,
        sleep_fn: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        self.batch_size = int(batch_size)
        self.throttle_seconds = float(throttle_seconds)
        self._http_client_factory = http_client_factory or (
            lambda: httpx.Client(
                timeout=_HTTP_TIMEOUT,
                follow_redirects=True,
                headers={"User-Agent": _USER_AGENT},
            )
        )
        # Permite testes injetarem sleep no-op (evita travar pytest).
        self._sleep = sleep_fn or time.sleep

        # Discovery output: lista de dicts {name, labels, key}.
        self._targets: list[dict[str, Any]] = []
        # Por-político updates resolvidos: dicts prontos pra UNWIND no load.
        self._updates: list[dict[str, Any]] = []
        # Stats pra logging final.
        self._stats = {
            "skipped_no_match": 0,
            "skipped_ambiguous": 0,
            "skipped_no_p18": 0,
            "skipped_image_fetch_failed": 0,
            "matched": 0,
        }

    # ------------------------------------------------------------------
    # discovery — lista nomes alvo do grafo
    # ------------------------------------------------------------------

    def _discover_targets(self) -> list[dict[str, Any]]:
        """Le do grafo políticos GO sem foto. Limita por ``batch_size``."""
        params = {"batch_size": self.batch_size}
        targets: list[dict[str, Any]] = []
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                result = session.run(_DISCOVERY_QUERY, params)
                for record in result:
                    name = (record.get("name") or "").strip()
                    if not name:
                        continue
                    labels = list(record.get("labels") or [])
                    key = record.get("key") or name
                    targets.append({
                        "name": name,
                        "name_normalized": _strip_accents_upper(name),
                        "labels": labels,
                        "key": str(key),
                    })
        except Exception as exc:  # noqa: BLE001 — log + continue
            logger.warning(
                "[wikidata_politicos_foto] discovery query failed: %s",
                exc,
            )
            return []
        if self.limit is not None:
            targets = targets[: self.limit]
        # Deduplica por name_normalized — mesma pessoa pode aparecer em 2
        # labels (ex.: ex-deputado federal que virou Person via TSE).
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for target in targets:
            if target["name_normalized"] in seen:
                continue
            seen.add(target["name_normalized"])
            deduped.append(target)
        return deduped

    # ------------------------------------------------------------------
    # SPARQL + EntityData + image — resolve um Q-id por nome
    # ------------------------------------------------------------------

    def _sparql_lookup(
        self,
        client: httpx.Client,
        name_normalized: str,
    ) -> tuple[list[str], str | None]:
        """Retorna (lista de Q-ids candidatos, snapshot_uri do payload SPARQL).

        - Lista vazia: nenhum candidato (ou erro silencioso, logado).
        - Lista com >1: ambíguo (caller deve pular).
        - Lista com 1: única correspondência, caller pode prosseguir.
        """
        query = _build_sparql_query(name_normalized)
        try:
            resp = client.post(
                _SPARQL_ENDPOINT,
                data={"query": query, "format": "json"},
                headers=_DEFAULT_HEADERS,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "[wikidata_politicos_foto] SPARQL lookup %r failed: %s",
                name_normalized, exc,
            )
            return [], None

        content_type = resp.headers.get("content-type", _JSON_CONTENT_TYPE)
        snapshot_uri = archive_fetch(
            url=str(resp.request.url),
            content=resp.content,
            content_type=content_type,
            run_id=self.run_id,
            source_id=_SOURCE_ID,
        )
        try:
            payload = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "[wikidata_politicos_foto] SPARQL JSON decode %r failed: %s",
                name_normalized, exc,
            )
            return [], snapshot_uri

        bindings = (
            payload.get("results", {}).get("bindings", [])
            if isinstance(payload, dict)
            else []
        )
        qids: list[str] = []
        for binding in bindings:
            item = binding.get("item", {})
            uri = item.get("value", "") if isinstance(item, dict) else ""
            # URIs vem como ``http://www.wikidata.org/entity/Q12345``.
            if "/entity/Q" in uri:
                qid = uri.rsplit("/", 1)[-1]
                if qid.startswith("Q") and qid not in qids:
                    qids.append(qid)
        return qids, snapshot_uri

    def _fetch_entity_p18(
        self,
        client: httpx.Client,
        qid: str,
    ) -> tuple[str | None, str | None, str]:
        """Baixa ``Special:EntityData/Q{id}.json``, extrai P18 (filename).

        Retorna ``(filename, snapshot_uri, entity_url)``. Filename é
        ``None`` quando não há P18 (político sem foto no Wikidata) ou
        payload anômalo.
        """
        url = f"{_ENTITY_BASE}/{qid}.json"
        try:
            resp = client.get(
                url,
                headers={
                    "User-Agent": _USER_AGENT,
                    "Accept": "application/json",
                },
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "[wikidata_politicos_foto] EntityData %s fetch failed: %s",
                qid, exc,
            )
            return None, None, url

        content_type = resp.headers.get(
            "content-type", _ENTITY_JSON_CONTENT_TYPE,
        )
        snapshot_uri = archive_fetch(
            url=str(resp.request.url),
            content=resp.content,
            content_type=content_type,
            run_id=self.run_id,
            source_id=_SOURCE_ID,
        )
        try:
            payload = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "[wikidata_politicos_foto] EntityData %s decode failed: %s",
                qid, exc,
            )
            return None, snapshot_uri, url

        entities = payload.get("entities", {}) if isinstance(payload, dict) else {}
        entity = entities.get(qid, {}) if isinstance(entities, dict) else {}
        claims = entity.get("claims", {}) if isinstance(entity, dict) else {}
        p18_claims = claims.get("P18", []) if isinstance(claims, dict) else []
        for claim in p18_claims:
            if not isinstance(claim, dict):
                continue
            mainsnak = claim.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue", {}) if isinstance(mainsnak, dict) else {}
            value = datavalue.get("value") if isinstance(datavalue, dict) else None
            if isinstance(value, str) and value.strip():
                return value.strip(), snapshot_uri, url
        return None, snapshot_uri, url

    def _fetch_image(
        self,
        client: httpx.Client,
        filename: str,
    ) -> tuple[str | None, str | None, str | None]:
        """GET em ``Special:FilePath/{filename}``, retorna (uri, ct, url).

        ``Special:FilePath`` faz redirect 302 pro binário CDN-hospedado.
        ``follow_redirects=True`` resolve transparentemente. Filename
        carrega caracteres tipo espaço/acento — passamos cru e o httpx
        URL-encoda. Retorna ``(None, None, image_url)`` em qualquer
        falha (HTTP, content-type não-imagem, archival error).
        """
        # ``filename`` vem do P18 já com a extensão. Não percent-encodamos
        # manualmente — o httpx faz isso ao montar a request.
        image_url = f"{_COMMONS_FILEPATH}/{filename}"
        try:
            resp = client.get(
                image_url,
                headers={"User-Agent": _USER_AGENT},
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "[wikidata_politicos_foto] image %s fetch failed: %s",
                filename, exc,
            )
            return None, None, image_url

        content_type_raw = resp.headers.get("content-type", "")
        primary = content_type_raw.split(";", 1)[0].strip().lower()
        if primary not in _PHOTO_CONTENT_TYPES:
            logger.warning(
                "[wikidata_politicos_foto] image %s non-image content-type %r",
                filename, content_type_raw,
            )
            return None, None, image_url
        normalized = "image/jpeg" if primary == "image/jpg" else primary

        try:
            snapshot_uri = archive_fetch(
                url=str(resp.request.url),
                content=resp.content,
                content_type=primary,
                run_id=self.run_id,
                source_id=_SOURCE_ID,
            )
        except OSError as exc:
            logger.warning(
                "[wikidata_politicos_foto] image %s archival failed: %s",
                filename, exc,
            )
            return None, None, image_url

        return snapshot_uri, normalized, image_url

    # ------------------------------------------------------------------
    # extract — orquestra os 3 fetches por político
    # ------------------------------------------------------------------

    def extract(self) -> None:
        self._targets = self._discover_targets()
        if not self._targets:
            logger.info(
                "[wikidata_politicos_foto] nenhum politico GO sem foto "
                "(grafo vazio ou todos ja cobertos por outros pipelines).",
            )
            self.rows_in = 0
            return

        logger.info(
            "[wikidata_politicos_foto] %d politico(s) GO pra enriquecer "
            "(batch_size=%d)",
            len(self._targets), self.batch_size,
        )
        self.rows_in = len(self._targets)

        with self._http_client_factory() as client:
            for idx, target in enumerate(self._targets):
                name_normalized = target["name_normalized"]
                self._sleep(self.throttle_seconds)

                qids, sparql_uri = self._sparql_lookup(client, name_normalized)
                if not qids:
                    self._stats["skipped_no_match"] += 1
                    logger.info(
                        "[wikidata_politicos_foto] %r -> 0 candidatos no Wikidata; pulando.",
                        name_normalized,
                    )
                    continue
                if len(qids) > 1:
                    self._stats["skipped_ambiguous"] += 1
                    logger.warning(
                        "[wikidata_politicos_foto] %r -> %d candidatos "
                        "ambiguos %s; pulando (politica anti-acusacao).",
                        name_normalized, len(qids), qids,
                    )
                    continue

                qid = qids[0]
                self._sleep(self.throttle_seconds)
                filename, entity_uri, entity_url = self._fetch_entity_p18(
                    client, qid,
                )
                if not filename:
                    self._stats["skipped_no_p18"] += 1
                    logger.info(
                        "[wikidata_politicos_foto] %r (%s) sem P18; pulando.",
                        name_normalized, qid,
                    )
                    continue

                self._sleep(self.throttle_seconds)
                image_snapshot, image_ct, image_url = self._fetch_image(
                    client, filename,
                )
                if not image_snapshot or not image_url:
                    self._stats["skipped_image_fetch_failed"] += 1
                    continue

                self._stats["matched"] += 1
                self._updates.append({
                    "name": target["name"],
                    "name_normalized": name_normalized,
                    "labels": target["labels"],
                    "key": target["key"],
                    "wikidata_qid": qid,
                    "foto_url": image_url,
                    "foto_snapshot_uri": image_snapshot,
                    "foto_content_type": image_ct,
                    "entity_url": entity_url,
                    "entity_snapshot_uri": entity_uri,
                    "sparql_snapshot_uri": sparql_uri,
                })
                logger.info(
                    "[wikidata_politicos_foto] %d/%d matched %r -> %s",
                    idx + 1, len(self._targets), name_normalized, qid,
                )

        logger.info(
            "[wikidata_politicos_foto] discovery done: matched=%d "
            "no_match=%d ambiguous=%d no_p18=%d image_failed=%d",
            self._stats["matched"],
            self._stats["skipped_no_match"],
            self._stats["skipped_ambiguous"],
            self._stats["skipped_no_p18"],
            self._stats["skipped_image_fetch_failed"],
        )

    # ------------------------------------------------------------------
    # transform — extract ja monta os updates; aqui so contamos
    # ------------------------------------------------------------------

    def transform(self) -> None:
        self.rows_loaded = len(self._updates)

    # ------------------------------------------------------------------
    # load — SET em :FederalLegislator / :StateLegislator / :Person por nome
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self._updates:
            logger.info("[wikidata_politicos_foto] nada pra carregar")
            return

        ingested_at = datetime.now(tz=UTC).isoformat()
        # MATCH explicito por nome (case/accent-insensitive). Não usamos
        # MERGE — pipeline e enrichment puro, nao queremos criar nodes.
        # O ``ANY(label IN labels(n) WHERE ...)`` cobre os 3 labels alvo
        # sem repetir o SET 3 vezes. Filtra por uf=GO no FederalLegislator
        # (StateLegislator e GO por construcao).
        query = """
        UNWIND $rows AS row
        MATCH (n)
        WHERE ANY(lbl IN labels(n)
                  WHERE lbl IN ['FederalLegislator', 'StateLegislator', 'Person'])
          AND coalesce(n.name, '') <> ''
          AND toUpper(replace(replace(replace(replace(replace(replace(
                replace(replace(replace(replace(coalesce(n.name, ''),
                'Á','A'),'É','E'),'Í','I'),'Ó','O'),'Ú','U'),'Â','A'),
                'Ê','E'),'Ô','O'),'Ã','A'),'Ç','C')) = row.name_normalized
          AND coalesce(n.foto_url, '') = ''
        SET n.foto_url = row.foto_url,
            n.foto_snapshot_uri = row.foto_snapshot_uri,
            n.foto_content_type = row.foto_content_type,
            n.wikidata_qid = row.wikidata_qid,
            n.foto_source_id = $source_id,
            n.foto_source_url = row.foto_url,
            n.foto_run_id = $run_id,
            n.foto_ingested_at = $ingested_at
        """

        rows = [
            {
                "name_normalized": upd["name_normalized"],
                "foto_url": upd["foto_url"],
                "foto_snapshot_uri": upd["foto_snapshot_uri"],
                "foto_content_type": upd["foto_content_type"],
                "wikidata_qid": upd["wikidata_qid"],
            }
            for upd in self._updates
        ]

        try:
            with self.driver.session(database=self.neo4j_database) as session:
                session.run(
                    query,
                    {
                        "rows": rows,
                        "source_id": self.source_id,
                        "run_id": self.run_id,
                        "ingested_at": ingested_at,
                    },
                )
        except Exception as exc:  # noqa: BLE001 — log + continue
            logger.warning(
                "[wikidata_politicos_foto] load failed: %s", exc,
            )
            return

        logger.info(
            "[wikidata_politicos_foto] enriched %d politico(s) GO com foto Wikidata",
            len(rows),
        )

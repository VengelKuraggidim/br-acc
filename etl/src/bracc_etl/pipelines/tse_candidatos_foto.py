"""TSE — fotos oficiais de candidatos GO já no grafo.

O pipeline ``tse`` carrega candidatos como nodes ``:Person`` com a
propriedade ``sq_candidato`` (o ID sequencial único do TSE por candidato
por eleição). O divulgacandcontas serve a foto oficial de cada candidato
em cada ciclo eleitoral, sob URL canônica derivada de ``cd_eleicao``
(código composto da eleição) + ``sq_candidato`` + ``uf``.

URL canônica (validada empiricamente em 2026-04-18 contra Caiado/
Mendanha/Vitor Hugo/Wolmir, 2022 GO)::

    https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/img/{cd_eleicao}/{sq_candidato}/{uf}

Importante: o pattern informal ``/img/{ano}/{sq}/foto.jpg`` que circula
em forums **NÃO funciona** — o servidor sempre devolve uma silhueta
placeholder (4704 bytes, SHA ``267865f1...``) com HTTP 200, mascarando
a falha. Mantemos o filtro defensivo de placeholder (skip silencioso
quando o SHA bate) pra que ex-candidatos sem foto oficial no portal
não poluam o grafo com a silhueta.

## Mapeamento ``ano → cd_eleicao``

Hardcoded a partir do endpoint ``/divulga/rest/v1/eleicao/ordinaria/{ano}``
(consultado uma vez na sessão de pesquisa). Cobre eleições gerais
(federais 2018/2022) e municipais (2020/2024) onde GO tem candidatos.
Anos antigos (≤ 2014) usam IDs curtos (``680`` etc.) e estão fora do
escopo MVP — a infra de fotos do TSE só ficou consistente a partir de
2018. Eleições 2026 podem ser adicionadas após o pleito.

## Estratégia (espelha ``wikidata_politicos_foto``)

1. ``extract()`` consulta o grafo por ``:Person`` GO com ``sq_candidato``,
   sem ``foto_url`` ainda (respeita pipelines ``camara_politicos_go`` /
   ``alego`` / ``camara_goiania`` / ``wikidata_politicos_foto`` que já
   carimbaram). Pra cada candidato, descobre o ano de eleição via
   ``CANDIDATO_EM``, escolhe o **ano mais recente** (foto mais recente
   é a melhor), busca a foto via URL canônica.
2. Skip silencioso se HTTP falha, content-type não é imagem, ou SHA
   bate com o placeholder TSE.
3. Cada GET é archival via :func:`archive_fetch` (binário PNG/JPG
   content-addressed). Idempotente: re-runs aproveitam cache.
4. Não cria nodes — só faz ``SET`` em existentes via Cypher
   (``MATCH (p:Person {sq_candidato: ...})``). Carimba proveniência
   sob prefixo ``foto_*`` pra não conflitar com a fonte do node TSE.

## Schema no grafo

Propriedades carimbadas em ``:Person``:

- ``foto_url``: URL canônica do divulgacandcontas;
- ``foto_snapshot_uri``: URI archival relativa do binário;
- ``foto_content_type``: ``image/jpeg`` (TSE serve JPG);
- ``foto_source_id``, ``foto_source_url``, ``foto_run_id``,
  ``foto_ingested_at``: bloco de proveniência prefixado.

## Cadência

Bienal — alinhada com calendário eleitoral (eleições gerais 2026/2030,
municipais 2028/2032). Foto raramente muda entre ciclos; rodar logo
após o TSE publicar candidaturas (~Aug-Sep do ano de eleição).

## Etiqueta TSE

User-Agent identificável (mesma política do Wikidata pipeline). Throttle
1s/req — divulgacandcontas é generoso mas não tem SLA público; preferir
ritmo respeitoso a risco de bloqueio.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_SOURCE_ID = "tse_candidatos_foto"

_PHOTO_URL_TEMPLATE = (
    "https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/img/"
    "{cd_eleicao}/{sq_candidato}/{uf}"
)

# User-Agent identificavel (mesma política do wikidata_politicos_foto).
# divulgacandcontas não publica regras formais de UA mas espelhamos as
# boas práticas de rate-limit + identificação.
_USER_AGENT = (
    "FiscalCidadao/0.1 (https://github.com/VengelKuraggidim/fiscal-cidadao)"
)
_DEFAULT_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": "image/jpeg,image/png,*/*;q=0.8",
}
_HTTP_TIMEOUT = 30.0

# Throttle entre requests (em segundos). 1.0s é conservador — TSE é
# generoso mas não tem SLA público.
_THROTTLE_SECONDS = 1.0

# Cap defensivo no batch — políticos GO com sq_candidato são finitos
# (~4k no histórico). 500 × 1s de throttle = ~8min por run, balanço
# razoável entre cobertura por execução e risco de banimento do TSE.
# Pra cobrir a cauda toda, use `refresh_photos.py --tse-iterations N`.
_DEFAULT_BATCH_SIZE = 500

# Content-types aceitos pro binário da imagem (mesma lista de
# camara_politicos_go pra consistência).
_PHOTO_CONTENT_TYPES = frozenset({"image/png", "image/jpeg", "image/jpg"})

# Placeholder TSE — silhueta servida quando o sq_candidato não tem foto
# oficial. SHA-256 do binário (171x235, 4704 bytes) capturado via curl
# em 2026-04-18 contra IDs claramente inválidos (000000000000) e contra
# o pattern de URL antigo informal ``/img/{ano}/{sq}/foto.jpg``. Skip
# silencioso quando bate — ex-candidatos sem foto não poluem o grafo.
_TSE_PLACEHOLDER_SHA256 = (
    "267865f138dc06f9a552a22c4b34f6aaf4c1b051b1fc51d24d02673fdcc07cee"
)
_TSE_PLACEHOLDER_SIZE = 4704

# Mapeamento canônico ``ano → cd_eleicao`` (descoberto via endpoint
# ``/divulga/rest/v1/eleicao/ordinaria/{ano}`` em 2026-04-18). Cobre
# todo ciclo a partir de 2018 (infra de fotos do TSE inconsistente
# antes disso). Adicionar eleições novas (2026, 2028, ...) editando
# este dict — é a única fonte de truth do mapping.
_ANO_TO_CD_ELEICAO: dict[int, str] = {
    2024: "2045202024",  # Eleições Municipais 2024
    2022: "2040602022",  # Eleição Geral Federal 2022
    2020: "2030402020",  # Eleições Municipais 2020
    2018: "2022802018",  # Eleição Geral Federal 2018
}

# Cypher: lista candidatos GO com sq_candidato, sem foto, e o ano mais
# recente de eleição que a gente sabe mapear pro cd_eleicao. Filtra:
# - p.uf = 'GO' (escopo do produto);
# - p.sq_candidato existe (pré-condição da URL);
# - p.foto_url ausente/vazio (não regride pipelines anteriores);
# - existe :CANDIDATO_EM pra um Election no dict de anos suportados.
# Ordena por ano DESC pra preferir foto mais recente (foto antiga é
# pior) e dedup por sq_candidato.
_DISCOVERY_QUERY = """
MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election)
WHERE p.uf = 'GO'
  AND coalesce(p.sq_candidato, '') <> ''
  AND coalesce(p.foto_url, '') = ''
  AND e.year IN $supported_years
WITH p, max(e.year) AS most_recent_year
RETURN p.sq_candidato AS sq_candidato,
       p.name AS name,
       most_recent_year AS year
ORDER BY p.name
LIMIT $batch_size
"""


class TseCandidatosFotoPipeline(Pipeline):
    """Enriquece candidatos GO já no grafo com foto oficial do TSE.

    Não cria nodes — só faz ``SET`` em ``:Person`` existentes (saída
    do pipeline ``tse``). Não toca nodes que já têm ``foto_url``
    (respeita ``camara_politicos_go`` / ``alego`` / ``wikidata_politicos_foto``
    que são fontes preferidas pra cargos ativos).

    Cadência recomendada: bienal (alinhada com eleições gerais e
    municipais brasileiras).
    """

    name = "tse_candidatos_foto"
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

        # Discovery output: list of dicts {sq_candidato, name, year}.
        self._targets: list[dict[str, Any]] = []
        # Per-candidate updates resolvidos: dicts prontos pra UNWIND no load.
        self._updates: list[dict[str, Any]] = []
        # Stats pra logging final.
        self._stats: dict[str, int] = {
            "skipped_unsupported_year": 0,
            "skipped_http_error": 0,
            "skipped_non_image": 0,
            "skipped_placeholder": 0,
            "skipped_archival_error": 0,
            "matched": 0,
        }

    # ------------------------------------------------------------------
    # discovery — lista candidatos alvo do grafo
    # ------------------------------------------------------------------

    def _discover_targets(self) -> list[dict[str, Any]]:
        """Lê do grafo candidatos GO sem foto. Limita por ``batch_size``."""
        params = {
            "batch_size": self.batch_size,
            "supported_years": sorted(_ANO_TO_CD_ELEICAO.keys()),
        }
        targets: list[dict[str, Any]] = []
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                result = session.run(_DISCOVERY_QUERY, params)
                for record in result:
                    sq = (record.get("sq_candidato") or "").strip()
                    name = (record.get("name") or "").strip()
                    year = record.get("year")
                    if not sq or year is None:
                        continue
                    targets.append({
                        "sq_candidato": sq,
                        "name": name,
                        "year": int(year),
                    })
        except Exception as exc:  # noqa: BLE001 — log + continue
            logger.warning(
                "[tse_candidatos_foto] discovery query failed: %s",
                exc,
            )
            return []
        if self.limit is not None:
            targets = targets[: self.limit]
        return targets

    # ------------------------------------------------------------------
    # photo fetch — busca o binário e archival
    # ------------------------------------------------------------------

    def _fetch_photo(
        self,
        client: httpx.Client,
        sq_candidato: str,
        year: int,
    ) -> tuple[str, str, str] | None:
        """Baixa a foto oficial do TSE pra ``(sq_candidato, year)``.

        Retorna ``(image_url, snapshot_uri, normalized_content_type)``
        ou ``None`` em qualquer falha (HTTP, content-type não-imagem,
        placeholder TSE, erro de archival).
        """
        cd_eleicao = _ANO_TO_CD_ELEICAO.get(year)
        if cd_eleicao is None:
            self._stats["skipped_unsupported_year"] += 1
            logger.warning(
                "[tse_candidatos_foto] sq=%s year=%d sem mapping cd_eleicao; pulando",
                sq_candidato, year,
            )
            return None

        url = _PHOTO_URL_TEMPLATE.format(
            cd_eleicao=cd_eleicao,
            sq_candidato=sq_candidato,
            uf="GO",
        )
        try:
            resp = client.get(url, headers=_DEFAULT_HEADERS)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            self._stats["skipped_http_error"] += 1
            logger.warning(
                "[tse_candidatos_foto] photo fetch sq=%s year=%d failed: %s",
                sq_candidato, year, exc,
            )
            return None

        content_type_raw = resp.headers.get("content-type", "")
        primary = content_type_raw.split(";", 1)[0].strip().lower()
        if primary not in _PHOTO_CONTENT_TYPES:
            self._stats["skipped_non_image"] += 1
            logger.warning(
                "[tse_candidatos_foto] sq=%s year=%d non-image content-type %r",
                sq_candidato, year, content_type_raw,
            )
            return None

        # Detecta placeholder TSE (silhueta servida com HTTP 200 quando
        # a foto oficial não existe). Fallback duplo (size + sha256) pra
        # não depender só de byte count em caso de revisão futura do
        # placeholder pelo TSE.
        body = resp.content
        if len(body) == _TSE_PLACEHOLDER_SIZE:
            digest = hashlib.sha256(body).hexdigest()
            if digest == _TSE_PLACEHOLDER_SHA256:
                self._stats["skipped_placeholder"] += 1
                logger.info(
                    "[tse_candidatos_foto] sq=%s year=%d -> placeholder TSE; pulando",
                    sq_candidato, year,
                )
                return None

        # Normaliza ``image/jpg`` (alguns CDNs devolvem assim) pra
        # ``image/jpeg`` padrão IANA.
        normalized = "image/jpeg" if primary == "image/jpg" else primary
        try:
            snapshot_uri = archive_fetch(
                url=str(resp.request.url),
                content=body,
                content_type=primary,
                run_id=self.run_id,
                source_id=_SOURCE_ID,
            )
        except OSError as exc:
            self._stats["skipped_archival_error"] += 1
            logger.warning(
                "[tse_candidatos_foto] sq=%s archival failed: %s",
                sq_candidato, exc,
            )
            return None

        return url, snapshot_uri, normalized

    # ------------------------------------------------------------------
    # extract — orquestra os fetches por candidato
    # ------------------------------------------------------------------

    def extract(self) -> None:
        self._targets = self._discover_targets()
        if not self._targets:
            logger.info(
                "[tse_candidatos_foto] nenhum candidato GO sem foto "
                "(grafo vazio ou todos ja cobertos por outros pipelines).",
            )
            self.rows_in = 0
            return

        logger.info(
            "[tse_candidatos_foto] %d candidato(s) GO pra enriquecer "
            "(batch_size=%d)",
            len(self._targets), self.batch_size,
        )
        self.rows_in = len(self._targets)

        with self._http_client_factory() as client:
            for idx, target in enumerate(self._targets):
                sq = target["sq_candidato"]
                year = target["year"]
                self._sleep(self.throttle_seconds)

                result = self._fetch_photo(client, sq, year)
                if result is None:
                    continue

                image_url, snapshot_uri, content_type = result
                self._stats["matched"] += 1
                self._updates.append({
                    "sq_candidato": sq,
                    "name": target["name"],
                    "year": year,
                    "foto_url": image_url,
                    "foto_snapshot_uri": snapshot_uri,
                    "foto_content_type": content_type,
                })
                logger.info(
                    "[tse_candidatos_foto] %d/%d matched sq=%s (%s, %d)",
                    idx + 1, len(self._targets), sq, target["name"], year,
                )

        logger.info(
            "[tse_candidatos_foto] discovery done: matched=%d "
            "no_year_mapping=%d http_error=%d non_image=%d placeholder=%d "
            "archival_error=%d",
            self._stats["matched"],
            self._stats["skipped_unsupported_year"],
            self._stats["skipped_http_error"],
            self._stats["skipped_non_image"],
            self._stats["skipped_placeholder"],
            self._stats["skipped_archival_error"],
        )

    # ------------------------------------------------------------------
    # transform — extract ja monta os updates; aqui so contamos
    # ------------------------------------------------------------------

    def transform(self) -> None:
        self.rows_loaded = len(self._updates)

    # ------------------------------------------------------------------
    # load — SET em :Person por sq_candidato
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self._updates:
            logger.info("[tse_candidatos_foto] nada pra carregar")
            return

        ingested_at = datetime.now(tz=UTC).isoformat()
        # MATCH explicito por sq_candidato — chave única por candidato
        # por eleição no schema TSE. Não usamos MERGE — pipeline e
        # enrichment puro, nao queremos criar nodes. Filtra por
        # uf=GO defensivamente (discovery ja restringe, mas o load
        # roda em sessão separada e pode ter race com outros pipelines).
        # Re-aplica a guarda ``foto_url IS NULL OR ''`` pra não
        # sobrescrever foto carimbada por pipeline preferido (ex.:
        # ``camara_politicos_go``) entre discovery e load.
        query = """
        UNWIND $rows AS row
        MATCH (p:Person {sq_candidato: row.sq_candidato})
        WHERE p.uf = 'GO'
          AND coalesce(p.foto_url, '') = ''
        SET p.foto_url = row.foto_url,
            p.foto_snapshot_uri = row.foto_snapshot_uri,
            p.foto_content_type = row.foto_content_type,
            p.foto_source_id = $source_id,
            p.foto_source_url = row.foto_url,
            p.foto_run_id = $run_id,
            p.foto_ingested_at = $ingested_at
        """

        rows = [
            {
                "sq_candidato": upd["sq_candidato"],
                "foto_url": upd["foto_url"],
                "foto_snapshot_uri": upd["foto_snapshot_uri"],
                "foto_content_type": upd["foto_content_type"],
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
                "[tse_candidatos_foto] load failed: %s", exc,
            )
            return

        logger.info(
            "[tse_candidatos_foto] enriched %d candidato(s) GO com foto TSE",
            len(rows),
        )

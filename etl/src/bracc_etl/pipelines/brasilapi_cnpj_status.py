"""BrasilAPI CNPJ status — pipeline batch pra situacao cadastral das empresas.

Hoje o perfil do politico mostra CNPJs que doaram pra campanha ou em que ele
aparece como socio, sem verificar **se a empresa ainda existe na Receita
Federal**. Empresa BAIXADA, SUSPENSA ou INAPTA doando pra campanha ou com
socio politico e sinal vermelho relevante (laranja, caixa 2, fraude).

O caminho "bulk RFB" (pipeline ``cnpj`` via prompt 07) resolveria todas as
empresas do grafo de uma vez, mas exige baixar dezenas de GB mensais e ainda
nao rodou. Esta pipeline e o MVP: consome a BrasilAPI ("gratuita, rate
limit 500/dia") por lote, priorizando as empresas mais relevantes do grafo
(doadores e socios), e carimba propriedades + proveniencia no no
``:Company``. Cache de 7d via ``c.situacao_verified_at`` evita bater a API
de novo no mesmo CNPJ — empresa baixada nao volta a ativa, e ativa
raramente muda.

Archival obrigatorio: cada resposta da BrasilAPI e gravada content-addressed
via :func:`bracc_etl.archival.archive_fetch`, e a URI resultante e carimbada
em ``source_snapshot_uri`` do no ``:Company`` atualizado.

Fora de escopo:

* Live-call em request-path da API (``/cnpj/{cnpj}/status``). Fernando
  (``feedback_everything_automated.md``) pede pipeline batch archival.
* Bulk dump RFB. Esta pipeline convive com o pipeline ``cnpj`` existente e
  nao o substitui — quando o bulk rodar, os dados daqui ficam coerentes
  porque ambos gravam os mesmos campos em ``:Company``.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.transforms import strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_API_BASE = "https://brasilapi.com.br/api/cnpj/v1"
_JSON_CONTENT_TYPE = "application/json"
_HTTP_TIMEOUT = 20.0
_USER_AGENT = "br-acc-etl/1.0 (+https://github.com/brunoclz/br-acc)"

# Rate limit: 2 req/s (BrasilAPI doc fala em 500/dia gratuito). Mantemos
# margem — batch_size default conservador abaixo faz o resto.
_RATE_LIMIT_SECONDS = 0.5

# Cache TTL: 7 dias. Empresa baixada nao volta a ativa, e ativa raramente
# muda. Ganho de fresquidade menor que o custo de rate-limit.
_CACHE_TTL_DAYS = 7

# Batch default: 400 CNPJs / run deixa 100/dia de margem sobre o rate limit.
# CLI ``run brasilapi_cnpj_status --batch-size N`` sobrescreve.
_DEFAULT_BATCH_SIZE = 400

_SOURCE_ID = "brasilapi_cnpj"

# Situacoes validas conforme documentacao RFB (espelhadas pela BrasilAPI).
_SITUACOES_VALIDAS: frozenset[str] = frozenset({
    "ATIVA", "BAIXADA", "SUSPENSA", "INAPTA", "NULA",
})


def _cache_cutoff_iso() -> str:
    """Fronteira de cache TTL (ISO 8601 UTC) em 7 dias atras."""
    return (
        datetime.now(tz=UTC) - timedelta(days=_CACHE_TTL_DAYS)
    ).isoformat()


def _map_situacao(raw: Any) -> str | None:
    """Normaliza ``situacao_cadastral`` do payload pra uma das 5 categorias.

    A BrasilAPI tanto devolve a string crua (``"ATIVA"``) quanto, em payloads
    mais antigos, o codigo numerico RFB (2=ATIVA, 3=SUSPENSA, 4=INAPTA,
    8=BAIXADA, 1=NULA). Fallback: nenhum = None.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        code = int(raw)
        return {
            1: "NULA",
            2: "ATIVA",
            3: "SUSPENSA",
            4: "INAPTA",
            8: "BAIXADA",
        }.get(code)
    if isinstance(raw, str):
        normalized = raw.strip().upper()
        if normalized in _SITUACOES_VALIDAS:
            return normalized
    return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Pega os campos que a pipeline ``SET`` no no :Company.

    BrasilAPI devolve varios nomes historicos pra cada campo; tentamos os
    mais estaveis primeiro. Payload inesperado vira None silencioso — o
    alerta fica "situacao_cadastral=None" que a API publica como "nao
    verificado".
    """
    situacao = _map_situacao(
        payload.get("situacao_cadastral")
        or payload.get("descricao_situacao_cadastral"),
    )
    return {
        "situacao_cadastral": situacao,
        "data_situacao": _as_str(
            payload.get("data_situacao_cadastral")
            or payload.get("data_situacao"),
        ),
        "cnae_principal": _as_str(payload.get("cnae_fiscal")),
        "cnae_descricao": _as_str(payload.get("cnae_fiscal_descricao")),
        "porte": _as_str(
            payload.get("porte")
            or payload.get("descricao_porte"),
        ),
        "capital_social": _as_float(payload.get("capital_social")),
        "municipio_rfb": _as_str(payload.get("municipio")),
        "uf_rfb": _as_str(payload.get("uf")),
        "data_abertura": _as_str(
            payload.get("data_inicio_atividade")
            or payload.get("data_abertura"),
        ),
    }


class BrasilapiCnpjStatusPipeline(Pipeline):
    """Batch ETL: popula situacao cadastral em :Company via BrasilAPI.

    O extract discobre os CNPJs alvo consultando o grafo (doadores +
    socios), filtrando por TTL de cache. transform parseia cada payload.
    load faz ``SET`` nas propriedades + proveniencia.
    """

    name = "brasilapi_cnpj_status"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        archive_online: bool = True,
        http_client_factory: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        # ``batch_size`` aqui e o numero de CNPJs consultados por run,
        # nao o chunk do loader. Cap defensivo de 500 (rate limit diario
        # gratuito da BrasilAPI).
        self.batch_size = min(int(batch_size), 500)
        self._archive_online_enabled = archive_online
        self._http_client_factory = http_client_factory or (
            lambda: httpx.Client(
                timeout=_HTTP_TIMEOUT,
                follow_redirects=True,
                headers={
                    "Accept": "application/json",
                    "User-Agent": _USER_AGENT,
                },
            )
        )

        self._targets: list[dict[str, Any]] = []
        # Por-CNPJ: fields extraidos + snapshot_uri + record_url (deep-link).
        self._updates: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # extract — descobre alvos no grafo + fetch BrasilAPI
    # ------------------------------------------------------------------

    def _discover_targets(self) -> list[dict[str, Any]]:
        """Le do grafo CNPJs de :Company que aparecem como doador OU socio.

        Prioriza por valor total doado (somatorio das DOOU saindo da
        empresa); fallback pra count de DOOU. Filtra pelo TTL: empresas
        ja verificadas < 7d atras nao voltam pra fila.

        ``DOOU`` no grafo BR-ACC vai (Company|Person|CampaignDonor)->Person
        (candidato), porque PJ doadora apos ADI 4650 (2015) e excecao —
        a relacao empresa-doadora e o caso comum (TSE 2014, comites PJ
        residuais). Logo a discovery cobre Company **que doa** + sociedade
        nas duas direcoes (sendo socia ou tendo socio).
        """
        cutoff = _cache_cutoff_iso()
        query = (
            "MATCH (c:Company) "
            "WHERE c.cnpj IS NOT NULL AND c.cnpj <> '' "
            "AND (c.situacao_verified_at IS NULL "
            "     OR c.situacao_verified_at < $cutoff) "
            "AND (EXISTS { (c)-[:DOOU]->() } "
            "     OR EXISTS { ()-[:DOOU]->(c) } "
            "     OR EXISTS { ()-[:SOCIO_DE]->(c) } "
            "     OR EXISTS { (c)-[:SOCIO_DE]->() }) "
            "OPTIONAL MATCH (c)-[d:DOOU]->() "
            "WITH c, sum(coalesce(d.valor, 0.0)) AS prio_valor_doado, "
            "     count(d) AS prio_doacoes_emitidas "
            "RETURN c.cnpj AS cnpj, "
            "       c.razao_social AS razao_social, "
            "       prio_valor_doado, "
            "       prio_doacoes_emitidas "
            "ORDER BY prio_valor_doado DESC, prio_doacoes_emitidas DESC "
            "LIMIT $batch_size"
        )
        targets: list[dict[str, Any]] = []
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                result = session.run(
                    query,
                    {"cutoff": cutoff, "batch_size": self.batch_size},
                )
                for record in result:
                    cnpj_raw = record.get("cnpj") or ""
                    digits = strip_document(str(cnpj_raw))
                    if len(digits) != 14:
                        continue
                    targets.append({
                        "cnpj_raw": str(cnpj_raw),
                        "cnpj_digits": digits,
                        "razao_social": str(record.get("razao_social") or ""),
                    })
        except Exception as exc:  # noqa: BLE001 — log and continue
            logger.warning(
                "[brasilapi_cnpj_status] failed to read :Company targets: %s",
                exc,
            )
        if self.limit is not None:
            targets = targets[: self.limit]
        return targets

    def _fetch_one(
        self,
        client: httpx.Client,
        cnpj_digits: str,
    ) -> tuple[dict[str, Any] | None, str | None, str]:
        """Baixa ``/api/cnpj/v1/{cnpj}``, arquiva, retorna (payload, uri, url).

        - HTTP 404: CNPJ nao existe na RFB (ou a BrasilAPI nao tem cache).
          Retorna ``(None, None, url)`` — caller pula silenciosamente.
        - Timeout / HTTP 5xx / 429 (rate limit): log warning, mesma resposta.
        - HTTP 200: arquiva bytes + retorna payload.
        """
        url = f"{_API_BASE}/{cnpj_digits}"
        try:
            resp = client.get(url)
        except httpx.HTTPError as exc:
            logger.warning(
                "[brasilapi_cnpj_status] HTTP error for %s: %s",
                cnpj_digits, exc,
            )
            return None, None, url

        if resp.status_code == 404:
            logger.info(
                "[brasilapi_cnpj_status] %s -> 404 (nao encontrado)",
                cnpj_digits,
            )
            return None, None, url
        if resp.status_code == 429:
            logger.warning(
                "[brasilapi_cnpj_status] %s -> 429 rate limit — abortando "
                "batch pra nao queimar quota.",
                cnpj_digits,
            )
            # Propaga pra extract parar o loop cedo.
            raise RuntimeError("brasilapi rate limit hit (429)")
        if resp.status_code >= 400:
            logger.warning(
                "[brasilapi_cnpj_status] %s -> status %d",
                cnpj_digits, resp.status_code,
            )
            return None, None, url

        snapshot_uri: str | None = None
        if self._archive_online_enabled:
            content_type = resp.headers.get(
                "content-type", _JSON_CONTENT_TYPE,
            )
            snapshot_uri = archive_fetch(
                url=url,
                content=resp.content,
                content_type=content_type,
                run_id=self.run_id,
                source_id=self.source_id,
            )
        try:
            payload = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "[brasilapi_cnpj_status] %s JSON decode error: %s",
                cnpj_digits, exc,
            )
            return None, snapshot_uri, url
        if not isinstance(payload, dict):
            return None, snapshot_uri, url
        return payload, snapshot_uri, url

    def extract(self) -> None:
        self._targets = self._discover_targets()
        if not self._targets:
            logger.info(
                "[brasilapi_cnpj_status] nenhum CNPJ elegivel "
                "(todos ja verificados < 7d ou grafo vazio).",
            )
            self.rows_in = 0
            return

        logger.info(
            "[brasilapi_cnpj_status] %d CNPJ(s) pra verificar (batch_size=%d)",
            len(self._targets), self.batch_size,
        )
        self.rows_in = len(self._targets)

        updates: list[dict[str, Any]] = []
        with self._http_client_factory() as client:
            for idx, target in enumerate(self._targets):
                cnpj_digits = target["cnpj_digits"]
                try:
                    payload, snapshot_uri, page_url = self._fetch_one(
                        client, cnpj_digits,
                    )
                except RuntimeError:
                    # 429: para o batch — a quota do dia ja foi. O proximo
                    # run do pipeline retoma pq os CNPJs nao gravados nao
                    # ganharam situacao_verified_at novo.
                    logger.warning(
                        "[brasilapi_cnpj_status] abortado em %d/%d por 429",
                        idx, len(self._targets),
                    )
                    break
                if payload is None:
                    # 404 ou erro transitorio: pula, mantem cache vazio pra
                    # retomar no proximo run (nao gravamos verified_at).
                    continue
                fields = _extract_fields(payload)
                updates.append({
                    "cnpj_raw": target["cnpj_raw"],
                    "cnpj_digits": cnpj_digits,
                    "fields": fields,
                    "snapshot_uri": snapshot_uri,
                    "source_url": page_url,
                })
                # Politeness: ~2 req/s.
                time.sleep(_RATE_LIMIT_SECONDS)

        self._updates = updates
        logger.info(
            "[brasilapi_cnpj_status] %d CNPJ(s) coletados pra atualizar",
            len(self._updates),
        )

    # ------------------------------------------------------------------
    # transform — nada a fazer; extract ja montou os updates
    # ------------------------------------------------------------------

    def transform(self) -> None:
        self.rows_loaded = len(self._updates)

    # ------------------------------------------------------------------
    # load — SET nas propriedades + proveniencia + verified_at
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self._updates:
            logger.info("[brasilapi_cnpj_status] nada pra carregar")
            return

        verified_at = datetime.now(tz=UTC).isoformat()
        # Duas formas de chave aceitaveis: digitos crus OU formatado (os 2
        # caminhos ja convivem no grafo). MERGE e OVERKILL — queremos so
        # atualizar :Company existentes. MATCH garante nao inserir empresa
        # que o grafo nao conhece.
        query = (
            "UNWIND $rows AS row "
            "MATCH (c:Company) "
            "WHERE c.cnpj = row.cnpj_raw OR c.cnpj = row.cnpj_digits "
            "SET c.situacao_cadastral = row.situacao_cadastral, "
            "    c.data_situacao = row.data_situacao, "
            "    c.cnae_principal = row.cnae_principal, "
            "    c.cnae_descricao = row.cnae_descricao, "
            "    c.porte = row.porte, "
            "    c.capital_social = row.capital_social, "
            "    c.municipio_rfb = row.municipio_rfb, "
            "    c.uf_rfb = row.uf_rfb, "
            "    c.data_abertura = row.data_abertura, "
            "    c.situacao_verified_at = $verified_at, "
            "    c.source_snapshot_uri = row.source_snapshot_uri, "
            "    c.situacao_source_id = $source_id, "
            "    c.situacao_source_url = row.source_url, "
            "    c.situacao_run_id = $run_id, "
            "    c.situacao_ingested_at = $verified_at"
        )

        rows: list[dict[str, Any]] = []
        for upd in self._updates:
            fields = upd["fields"]
            rows.append({
                "cnpj_raw": upd["cnpj_raw"],
                "cnpj_digits": upd["cnpj_digits"],
                "situacao_cadastral": fields["situacao_cadastral"],
                "data_situacao": fields["data_situacao"],
                "cnae_principal": fields["cnae_principal"],
                "cnae_descricao": fields["cnae_descricao"],
                "porte": fields["porte"],
                "capital_social": fields["capital_social"],
                "municipio_rfb": fields["municipio_rfb"],
                "uf_rfb": fields["uf_rfb"],
                "data_abertura": fields["data_abertura"],
                "source_snapshot_uri": upd["snapshot_uri"],
                "source_url": upd["source_url"],
            })

        with self.driver.session(database=self.neo4j_database) as session:
            session.run(
                query,
                {
                    "rows": rows,
                    "verified_at": verified_at,
                    "source_id": self.source_id,
                    "run_id": self.run_id,
                },
            )
        logger.info(
            "[brasilapi_cnpj_status] atualizadas %d :Company com situacao "
            "cadastral", len(rows),
        )

"""ALEGO — fotos dos deputados estaduais GO em exercício (HTML scraping).

A API JSON de transparência (``transparencia.al.go.leg.br``) que o pipeline
``alego.py`` consome **não publica a foto** do deputado — só gastos/cota e
cadastro mínimo. A foto fica no portal institucional
(``portal.al.go.leg.br/deputados``) renderizado via Rails server-side, então
precisamos scrapear HTML.

Este pipeline é **secundário e atualizável**: ele encontra os
``:StateLegislator`` já criados pelo ``alego.py`` (via mesma
``legislator_id`` chave) e carimba os campos ``foto_url``,
``foto_snapshot_uri``, ``foto_content_type`` + ``url_foto`` (alias). Se o
``StateLegislator`` ainda não existe quando esse pipeline roda, o
``MERGE`` cria um stub com a chave correta — o ``alego.py`` posterior
preenche o resto.

Fontes (públicas, sem auth):

* ``GET https://portal.al.go.leg.br/deputados`` — redireciona pra
  ``/deputados/em-exercicio``; HTML lista 41 deputados em exercício com
  link ``/deputados/perfil/{id}`` + nome + partido na ``<table>``.
* ``GET https://portal.al.go.leg.br/deputados/perfil/{id}`` — perfil
  individual; foto principal em ``<img class="foto" src=...>``. URL da
  foto é hash opaco em ``saba.al.go.leg.br/v1/view/portal/public/...`` —
  não-previsível, precisa scrapear.
* ``GET <foto_url>`` — binário JPEG/PNG (≈100 KB cada).

## Selector escolhido (frágil — documentar bem)

Listagem: regex ``<a class="link" href=/deputados/perfil/(\\d+)>([^<]+)</a>``
produz ``(id, nome)`` por ocorrência. Robusto contra reordenação de
classes porque o HTML do Rails do portal usa exatamente essa forma sem
aspas no ``href`` da tabela.

Perfil: regex ``<img class="foto" src="([^"]+)"`` extrai a URL da foto.
A classe ``foto`` é exclusiva do retrato principal — tested em 6 perfis
diferentes (808, 137, 138, 809, 51, 140, 117) em 2026-04-18.

Se o portal mudar a estrutura: o pipeline **falha explicitamente** com
``RuntimeError`` em ``extract()`` quando a listagem volta sem nenhum
match. Por-perfil, falha graciosa: log + skip do deputado, e o pipeline
continua. Isso é **deliberado** — não inventar URL de foto.

## Cadência

Mensal (fotos quase nunca mudam — só em mandato novo ou foto oficial
trocada). Listagem é leve (~180 KB), 41 perfis × ~95 KB + 41 fotos × ~100
KB = ~12 MB por run.

## Não-objetivos

* Ex-deputados (``/perfil-biografico/{id}``) — fora do MVP. Quando der pra
  expandir, basta um segundo loop sobre IDs históricos com URL diferente.
* Telefones, e-mail — já vem da listagem mas a fonte canônica é o
  ``alego.py`` (transparência); duplicar aqui é débito.
"""

from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.pipelines.alego import _hash_id
from bracc_etl.transforms import deduplicate_rows, normalize_name

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Portal institucional — Rails server-side render, sem JSON API pública
# pra fotos. ``/deputados`` redireciona pra ``/deputados/em-exercicio`` —
# seguimos redirects (httpx default false).
_PORTAL_BASE = "https://portal.al.go.leg.br"
_LISTING_URL = f"{_PORTAL_BASE}/deputados"
_PROFILE_URL_TEMPLATE = f"{_PORTAL_BASE}/deputados/perfil/{{id}}"

# User-Agent identificável — boa prática, e dá ao operador do portal um
# contato pra reclamar se o tráfego incomodar.
_USER_AGENT = (
    "FiscalCidadao/0.1 (+https://github.com/VengelKuraggidim/fiscal-cidadao)"
)
_DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml",
    "User-Agent": _USER_AGENT,
}
_HTTP_TIMEOUT = 30.0
# ≥0.5s entre requests — portal é público e estável, mas é polidez. Em
# 41 deputados x 2 (perfil + foto) + 1 listagem = ~83 fetches por run, o
# que dá ≥41s só de throttle (aceitável pra cadência mensal).
_RATE_LIMIT_SECONDS = 0.5

_HTML_CONTENT_TYPE = "text/html"
_PHOTO_CONTENT_TYPES = frozenset({"image/png", "image/jpeg", "image/jpg"})

_SOURCE_ID = "alego_deputados_foto"

# --- Selectors (regex — html.parser stdlib é overkill pra esses 2 casos) ---

# Listagem: ``<a class="link" href=/deputados/perfil/{id}>{nome}</a>``.
# Note: o portal não coloca aspas no href — match flexível pra ambos casos.
_LISTING_LINK_RE = re.compile(
    r'<a\s+class="link"\s+href=["\']?/deputados/perfil/(\d+)["\']?\s*>([^<]+)</a>',
    re.IGNORECASE,
)
# Perfil: ``<img class="foto" src="https://saba.al.go.leg.br/...">``.
# Aceita classes adicionais (``class="foto destaque"``) e ordem de attrs.
_PROFILE_PHOTO_RE = re.compile(
    r'<img\b[^>]*\bclass="(?:[^"]*\s)?foto(?:\s[^"]*)?"[^>]*\bsrc="([^"]+)"',
    re.IGNORECASE,
)
# Fallback: ``<img src="..." class="foto">`` (atributos invertidos).
_PROFILE_PHOTO_REV_RE = re.compile(
    r'<img\b[^>]*\bsrc="([^"]+)"[^>]*\bclass="(?:[^"]*\s)?foto(?:\s[^"]*)?"',
    re.IGNORECASE,
)


def _parse_listing(html: str) -> list[tuple[str, str]]:
    """Extract ``(deputy_id, nome_display)`` pairs from the listing HTML.

    Returns the raw display name; the caller normalizes via
    :func:`normalize_name` only when computing the matching key (we keep
    the original form in the row for readability/debugging).
    """
    seen: dict[str, str] = {}
    for match in _LISTING_LINK_RE.finditer(html):
        dep_id = match.group(1).strip()
        nome = match.group(2).strip()
        if not dep_id or not nome:
            continue
        # The same ID appears twice in the listing (table row + sidebar
        # ``ul.deputados-e-comissoes__lista`` index). dict.setdefault
        # keeps the first occurrence — both carry the same ``nome``.
        seen.setdefault(dep_id, nome)
    return [(dep_id, nome) for dep_id, nome in seen.items()]


def _parse_photo_url(html: str) -> str | None:
    """Extract the principal photo URL from a deputy profile HTML."""
    match = _PROFILE_PHOTO_RE.search(html) or _PROFILE_PHOTO_REV_RE.search(html)
    if match is None:
        return None
    url = match.group(1).strip()
    return url or None


def _fetch_html(
    client: httpx.Client,
    url: str,
    *,
    run_id: str,
) -> tuple[str, str, str] | None:
    """GET ``url`` returning ``(text, final_url, snapshot_uri)`` or ``None``.

    Archives the raw HTML body under ``alego_deputados_foto/...`` regardless
    of how the caller parses it. ``None`` on any HTTP/decode failure (logged).
    """
    try:
        resp = client.get(url, headers=_DEFAULT_HEADERS)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[alego_deputados_foto] HTML fetch %s failed: %s", url, exc)
        return None
    content_type_raw = resp.headers.get("content-type", _HTML_CONTENT_TYPE)
    primary = content_type_raw.split(";", 1)[0].strip().lower()
    # The portal occasionally serves ``application/xhtml+xml``; treat both as HTML.
    if primary not in {"text/html", "application/xhtml+xml"}:
        logger.warning(
            "[alego_deputados_foto] %s returned non-HTML content-type %r",
            url, content_type_raw,
        )
        return None
    snapshot_uri = archive_fetch(
        url=str(resp.request.url),
        content=resp.content,
        content_type="text/html",
        run_id=run_id,
        source_id=_SOURCE_ID,
    )
    try:
        text = resp.content.decode("utf-8")
    except UnicodeDecodeError:
        text = resp.content.decode("latin-1", errors="replace")
    return text, str(resp.request.url), snapshot_uri


def _fetch_photo_binary(
    client: httpx.Client,
    photo_url: str,
    *,
    run_id: str,
) -> tuple[str, str] | None:
    """Fetch the photo binary and archive it.

    Returns ``(snapshot_uri, normalized_content_type)`` or ``None`` on
    HTTP error or non-image content-type. Same pattern as
    ``camara_politicos_go._fetch_deputy_photo``.
    """
    if not photo_url:
        return None
    try:
        resp = client.get(photo_url, headers={"User-Agent": _USER_AGENT})
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning(
            "[alego_deputados_foto] photo fetch %s failed: %s", photo_url, exc,
        )
        return None
    content_type_raw = resp.headers.get("content-type", "")
    primary = content_type_raw.split(";", 1)[0].strip().lower()
    if primary not in _PHOTO_CONTENT_TYPES:
        logger.warning(
            "[alego_deputados_foto] photo %s returned non-image content-type %r",
            photo_url, content_type_raw,
        )
        return None
    normalized = "image/jpeg" if primary == "image/jpg" else primary
    try:
        snapshot_uri = archive_fetch(
            url=str(resp.request.url),
            content=resp.content,
            content_type=primary,
            run_id=run_id,
            source_id=_SOURCE_ID,
        )
    except OSError as exc:
        logger.warning(
            "[alego_deputados_foto] photo archival %s failed: %s", photo_url, exc,
        )
        return None
    return snapshot_uri, normalized


class AlegoDeputadosFotoPipeline(Pipeline):
    """Atualiza ``:StateLegislator`` com foto scrapeada do portal ALEGO.

    Estratégia de matching: ``legislator_id = _hash_id(normalize_name(nome), "", "")``
    — exatamente o que o ``alego.py`` produz quando a transparência não
    expõe CPF/legislatura (caso comum). Garante MERGE no node já existente.

    Falha explícita se a listagem volta vazia (``RuntimeError``) — ou o
    portal mudou estrutura, ou foi block. Por-perfil/foto, falha
    graciosa: skip + log, pipeline continua.
    """

    name = "alego_deputados_foto"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        # Override pra testes injetarem cliente httpx mockado (mesmo
        # padrão do camara_politicos_go).
        http_client_factory = kwargs.pop(
            "http_client_factory",
            lambda: httpx.Client(
                timeout=_HTTP_TIMEOUT, follow_redirects=True,
            ),
        )
        rate_limit_seconds = float(
            kwargs.pop("rate_limit_seconds", _RATE_LIMIT_SECONDS),
        )
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        self._http_client_factory = http_client_factory
        self._rate_limit_seconds = rate_limit_seconds

        # Estado intermediário entre extract → transform.
        self._listing_url: str = ""
        self._listing_snapshot_uri: str | None = None
        self._raw_deputies: list[dict[str, Any]] = []

        self.legislators: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # extract — GET listing + GET cada perfil + GET cada foto
    # ------------------------------------------------------------------

    def extract(self) -> None:
        """Scrape o portal: listagem → perfis → fotos. Tudo arquivado."""
        with self._http_client_factory() as client:
            listing_result = _fetch_html(
                client, _LISTING_URL, run_id=self.run_id,
            )
            if listing_result is None:
                raise RuntimeError(
                    "[alego_deputados_foto] failed to fetch listing "
                    f"{_LISTING_URL}; aborting (no fallback — portal likely "
                    "down or selector broken)",
                )
            listing_html, listing_url, listing_snapshot = listing_result
            self._listing_url = listing_url
            self._listing_snapshot_uri = listing_snapshot

            entries = _parse_listing(listing_html)
            if not entries:
                raise RuntimeError(
                    "[alego_deputados_foto] listing parsed 0 deputies — "
                    "selector likely broke. Inspect "
                    f"{listing_url} (snapshot: {listing_snapshot}). "
                    "Expected pattern: "
                    "'<a class=\"link\" href=/deputados/perfil/{id}>{nome}</a>'",
                )
            logger.info(
                "[alego_deputados_foto] listing parsed %d deputies", len(entries),
            )
            if self.limit is not None:
                entries = entries[: self.limit]

            for dep_id, nome_display in entries:
                time.sleep(self._rate_limit_seconds)
                profile_url = _PROFILE_URL_TEMPLATE.format(id=dep_id)
                profile_result = _fetch_html(
                    client, profile_url, run_id=self.run_id,
                )
                if profile_result is None:
                    logger.warning(
                        "[alego_deputados_foto] skipping deputy %s (%s) — "
                        "profile fetch failed", dep_id, nome_display,
                    )
                    continue
                profile_html, profile_final_url, profile_snapshot = profile_result
                photo_url = _parse_photo_url(profile_html)
                if not photo_url:
                    logger.warning(
                        "[alego_deputados_foto] no <img class=\"foto\"> in "
                        "perfil/%s (%s); skipping foto archival",
                        dep_id, nome_display,
                    )
                    self._raw_deputies.append({
                        "deputy_id": dep_id,
                        "nome_display": nome_display,
                        "profile_url": profile_final_url,
                        "profile_snapshot_uri": profile_snapshot,
                        "photo_url": None,
                        "photo_snapshot_uri": None,
                        "photo_content_type": None,
                    })
                    continue

                time.sleep(self._rate_limit_seconds)
                photo_result = _fetch_photo_binary(
                    client, photo_url, run_id=self.run_id,
                )
                if photo_result is None:
                    photo_snapshot_uri = None
                    photo_content_type = None
                else:
                    photo_snapshot_uri, photo_content_type = photo_result
                self._raw_deputies.append({
                    "deputy_id": dep_id,
                    "nome_display": nome_display,
                    "profile_url": profile_final_url,
                    "profile_snapshot_uri": profile_snapshot,
                    "photo_url": photo_url,
                    "photo_snapshot_uri": photo_snapshot_uri,
                    "photo_content_type": photo_content_type,
                })

        self.rows_in = len(self._raw_deputies)
        logger.info(
            "[alego_deputados_foto] extracted %d deputies (with or without photo)",
            len(self._raw_deputies),
        )

    # ------------------------------------------------------------------
    # transform — produz dicts pra MERGE em :StateLegislator
    # ------------------------------------------------------------------

    def transform(self) -> None:
        """Compute ``legislator_id`` matching ``alego.py`` and stamp provenance."""
        for dep in self._raw_deputies:
            nome_normalizado = normalize_name(dep["nome_display"])
            if not nome_normalizado:
                logger.warning(
                    "[alego_deputados_foto] empty normalized name for deputy_id=%s; "
                    "skipping", dep["deputy_id"],
                )
                continue
            # Mesma fórmula do alego.py: _hash_id(name, cpf_digits[-4:] or "", legislature).
            # Como o portal HTML não expõe CPF nem legislatura nesta versão,
            # ambos vão vazios — e o alego.py também grava vazios quando a
            # transparência não traz CPF (caso default). Match-determinístico.
            legislator_id = _hash_id(nome_normalizado, "", "")
            row = self.attach_provenance(
                {
                    "legislator_id": legislator_id,
                    "name": nome_normalizado,
                    "uf": "GO",
                    "alego_deputy_id": dep["deputy_id"],
                    "url_foto": dep["photo_url"] or "",
                    "foto_url": dep["photo_url"],
                    "foto_snapshot_uri": dep["photo_snapshot_uri"],
                    "foto_content_type": dep["photo_content_type"],
                    "scope": "estadual",
                    "source": _SOURCE_ID,
                },
                record_id=dep["deputy_id"],
                record_url=dep["profile_url"],
                snapshot_uri=dep["profile_snapshot_uri"],
            )
            self.legislators.append(row)

        self.legislators = deduplicate_rows(self.legislators, ["legislator_id"])
        self.rows_loaded = len(self.legislators)
        logger.info(
            "[alego_deputados_foto] transformed %d StateLegislator updates",
            len(self.legislators),
        )

    # ------------------------------------------------------------------
    # load — MERGE em :StateLegislator (atualiza nodes existentes)
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.legislators:
            logger.warning("[alego_deputados_foto] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes(
            "StateLegislator",
            self.legislators,
            key_field="legislator_id",
        )

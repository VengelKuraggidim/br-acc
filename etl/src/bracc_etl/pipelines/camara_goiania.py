"""ETL pipeline for Camara Municipal de Goiania — Fase 2a (vereadores).

Background: os 3 endpoints Plone JSON do portal CMG (``@@portalmodelo-json``,
``@@transparency-json``, ``@@pl-json``) só devolvem stubs de metadata, não
vereadores ativos nem despesas. Documentado em
``todo-list-prompts/medium_priority/debitos/camara-goiania-scraping.md``.

Esta versão (Fase 2a, 2026-05-02) abandona aqueles endpoints em favor de
scraping HTML estável da listagem e dos perfis individuais:

* ``GET /institucional/parlamentares/`` — HTML com 28 anchors apontando
  pra ``parlamentares/<slug>`` (20ª Legislatura ativa, exclui
  ``legislaturas-anteriores``).
* ``GET /institucional/parlamentares/<slug>`` — HTML por vereador com
  campos rotulados (``Partido:``, ``Nascimento:``, ``Telefones:``,
  ``E-mail:``, ``Gabinete:``) + biografia + foto sob
  ``/Fotos-de-parlamentares/``.

Fase 2b (pendente): despesas, diárias e folha estão em
``camaragoiania.nucleogov.com.br``, que é SPA RequireJS — exige
Selenium/Playwright. TODO mantido em
``todo-list-prompts/medium_priority/debitos/camara-goiania-scraping.md``
seção "Fase 2b".

Nodes criados:
  * ``GoVereador`` — registro rico (party, photo_url, gabinete, phones,
    email, birth_date, bio_summary).

Sem relationships nesta fase (despesas/proposições virão em 2b).
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import deduplicate_rows, normalize_name, parse_date
from bracc_etl.transforms import stable_id as _stable_id

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_API_BASE = "https://www.goiania.go.leg.br"
_LISTAGEM_PATH = "/institucional/parlamentares/"
_TIMEOUT = 30.0
# Portal Plone bloqueia User-Agent default do httpx em alguns paths
# (``camaragoiania.nucleogov.com.br`` retorna 403). Manter UA realista
# mesmo no portal CMG por simetria com a fase 2b futura.
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_HTML_CONTENT_TYPE = "text/html; charset=utf-8"

# Chave privada injetada em cada vereador pelo caminho online pra propagar
# a URI archival do HTML do perfil até ``transform``. Prefixo ``__`` evita
# colisão com qualquer campo extraído do HTML.
_SNAPSHOT_KEY = "__snapshot_uri"

# Pausa entre requests pra não martelar o portal Plone (28 perfis × 0.5s
# = ~14s; aceitável). Se o portal limitar, aumentar pra 1.0.
_REQUEST_DELAY_SECONDS = 0.5


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _http_get_text(
    url: str,
) -> tuple[str, bytes, str] | None:
    """Fetch HTML and return ``(text, raw_bytes, content_type)``.

    Retorna ``None`` em qualquer falha de rede / HTTP 4xx-5xx — caller
    decide se loga e segue ou aborta. Os bytes crus são preservados pra
    archival; ``text`` é a versão decodificada pra parsing.
    """
    try:
        with httpx.Client(
            timeout=_TIMEOUT, headers={"User-Agent": _USER_AGENT},
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()
            content = resp.content
            content_type = resp.headers.get("content-type", _HTML_CONTENT_TYPE)
            return resp.text, content, content_type
    except httpx.HTTPError as exc:
        logger.warning("[camara_goiania] HTTP error fetching %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


_PROFILE_SLUG_RE = re.compile(
    r'href="https://www\.goiania\.go\.leg\.br/institucional/parlamentares/'
    r'([A-Za-z0-9][A-Za-z0-9_-]*)"',
)
# Slugs estruturais que aparecem na mesma listagem mas não são vereadores
# da legislatura corrente. ``legislaturas-anteriores`` é a área de arquivo.
# ``Parlamentares_*-Legislatura`` é a própria página de listagem
# (vem do ``<base href>`` da página) — filtrar por padrão pra não ingerir
# o stub da própria 20ª Legislatura como se fosse vereador.
_NON_VEREADOR_SLUGS: frozenset[str] = frozenset({"legislaturas-anteriores"})
_PARLAMENTARES_BASE_SLUG_RE = re.compile(
    r"^Parlamentares_\d+-Legislatura$",
)


def _extract_profile_slugs(listagem_html: str) -> list[str]:
    """Lista slugs únicos de vereadores ativos a partir da listagem HTML.

    Preserva ordem de aparição (estável entre runs pra dedup determinístico).
    Slugs em ``_NON_VEREADOR_SLUGS`` ou matching ``Parlamentares_*-Legislatura``
    (auto-referência da listagem) são filtrados.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for match in _PROFILE_SLUG_RE.finditer(listagem_html):
        slug = match.group(1)
        if (
            slug in _NON_VEREADOR_SLUGS
            or slug in seen
            or _PARLAMENTARES_BASE_SLUG_RE.match(slug)
        ):
            continue
        seen.add(slug)
        ordered.append(slug)
    return ordered


_TITLE_RE = re.compile(
    r"<title>\s*([^<—]+?)\s*—\s*C[âa]mara",
    re.IGNORECASE,
)
_PROFILE_PHOTO_RE = re.compile(
    r'<img[^>]+src="(https://www\.goiania\.go\.leg\.br/institucional/'
    r'parlamentares/Fotos-de-parlamentares/[^"]+)"',
)
# Ordem dos labels no HTML (observada em Bessa + Aava + Anselmo, com
# variações ``Partido:`` vs ``Partido atual:`` e ``Telefones:`` vs
# ``Telefone:``). Lookahead até o próximo label OU até "Natural" (início
# canônico da biografia) garante que o valor não invade o campo seguinte.
_FIELD_TERMINATORS = (
    "Partido atual:",
    "Partido:",
    "Nascimento:",
    "Telefones:",
    "Telefone:",
    "E-mail:",
    "Gabinete:",
    "Natural ",
    "Facebook:",
    "Instagram:",
)


def _strip_html(html: str) -> str:
    """Remove ``<script>``/``<style>`` blocks then all tags, collapse spaces."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_field(label: str, text: str) -> str:
    """Pull the value after ``label`` up to the next known terminator.

    Empty string if label not present. Strips trailing punctuation like
    " ," or " ." that some perfis carregam por colagem do template Plone.
    """
    pattern = (
        re.escape(label)
        + r"\s*(.*?)(?="
        + "|".join(re.escape(t) for t in _FIELD_TERMINATORS if t != label)
        + r"|$)"
    )
    match = re.search(pattern, text, re.IGNORECASE | re.S)
    if not match:
        return ""
    value = match.group(1).strip()
    return value.rstrip(" ,.;")


def _extract_bio_summary(text: str, max_chars: int = 600) -> str:
    """Bio: tudo após ``Natural`` (início canônico da biografia no Plone).

    Fallback: tudo após ``Gabinete: <num>`` quando ``Natural`` não está
    presente (ex.: perfis sem prefixo geográfico como Aava Santiago).
    Trunca em ``max_chars`` no último espaço pra não cortar palavra e
    para em rodapés sociais (``Facebook:`` etc.).
    """
    match = re.search(r"Natural\s+(.+)", text, re.S)
    if not match:
        match = re.search(r"Gabinete\s*:\s*\d+\s+(.+)", text, re.S)
    if not match:
        return ""
    bio = match.group(1).strip()
    # Stop em rodapés/menus que aparecem grudados no fim do bloco de
    # conteúdo no Plone. ``Tweet por`` é o widget de tweets do tema
    # Sunburst que vem na sidebar de alguns perfis.
    for stop in (
        "Facebook:",
        "Instagram:",
        "Twitter:",
        "Tweet por",
        "Agenda de Eventos",
    ):
        idx = bio.find(stop)
        if idx >= 0:
            bio = bio[:idx].strip()
    if len(bio) <= max_chars:
        return bio
    cut = bio[:max_chars].rsplit(" ", 1)[0]
    return cut.rstrip(" ,.;") + "…"


def _parse_profile_html(
    html: str, slug: str, profile_url: str,
) -> dict[str, Any]:
    """Parse a vereador profile HTML into a structured dict.

    Returns dict with keys: ``slug``, ``name``, ``party``, ``photo_url``,
    ``gabinete``, ``phones``, ``email``, ``birth_date`` (ISO ``YYYY-MM-DD``
    quando parseável), ``bio_summary``, ``profile_url``, ``legislature``.
    Campos ausentes ficam vazios — caller decide se descarta a row.
    """
    title_match = _TITLE_RE.search(html)
    name_raw = (
        title_match.group(1).strip()
        if title_match
        else slug.replace("-", " ")
    )

    photo_match = _PROFILE_PHOTO_RE.search(html)
    photo_url = photo_match.group(1) if photo_match else ""

    text = _strip_html(html)

    # ``Partido atual:`` (Aava, Anselmo) tem precedência sobre ``Partido:``
    # (Bessa) — o portal Plone usa ambas variantes.
    party = _extract_field("Partido atual:", text) or _extract_field(
        "Partido:", text,
    )
    nascimento_raw = _extract_field("Nascimento:", text)
    # Algumas pages usam ``Telefone:`` (singular). Tentar plural primeiro.
    phones = _extract_field("Telefones:", text) or _extract_field(
        "Telefone:", text,
    )
    email = _extract_field("E-mail:", text)
    # ``Gabinete:`` é sempre numérico (1-3 dígitos). Restringir o match
    # pra não invadir a biografia em perfis sem "Natural" como prefixo
    # (ex.: Aava Santiago — bio começa direto com o nome). Fallback pro
    # extrator genérico se o regex curto não casar.
    gabinete_match = re.search(r"Gabinete\s*:\s*(\d{1,3})\b", text)
    gabinete = (
        gabinete_match.group(1) if gabinete_match
        else _extract_field("Gabinete:", text)
    )

    birth_date = ""
    if nascimento_raw:
        # ``parse_date`` aceita ``DD/MM/YYYY`` e devolve ``YYYY-MM-DD``.
        birth_date = parse_date(nascimento_raw) or ""

    bio_summary = _extract_bio_summary(text)

    return {
        "slug": slug,
        "name": name_raw,
        "party": party,
        "photo_url": photo_url,
        "gabinete": gabinete,
        "phones": phones,
        "email": email,
        "birth_date": birth_date,
        "bio_summary": bio_summary,
        "profile_url": profile_url,
        "legislature": "20",
    }


# ---------------------------------------------------------------------------
# fetch_to_disk — listagem + 1 GET por perfil, archival opt-in
# ---------------------------------------------------------------------------


def _fetch_run_id() -> str:
    """Run ID sintético pro caminho ``fetch_to_disk`` (offline preserva opt-in).

    Formato canônico ``{source_id}_YYYYMMDDHHMMSS`` — ``archive_fetch``
    deriva o bucket mensal a partir desse string.
    """
    return f"camara_goiania_{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S')}"


def fetch_to_disk(
    output_dir: Path,
    limit: int | None = None,
    archival: bool = True,
) -> list[Path]:
    """Baixa listagem + perfis HTML, escreve ``vereadores.json`` consolidado.

    Args:
      output_dir: Diretório destino. ``vereadores.json`` é escrito direto
        nele; HTMLs crus em ``output_dir/raw/`` (listagem.html + 1 por
        perfil) pra evidência local independente do archival storage.
      limit: Cap de perfis baixados (smoke test). ``None`` = todos.
      archival: Se ``True``, chama :func:`archive_fetch` por HTML pra
        carimbar ``__snapshot_uri`` em cada vereador. Desliga só pra
        smoke tests offline-only.

    Returns:
      Lista de paths escritos (sempre ``[vereadores.json]`` em runs
      bem-sucedidos; ``[]`` se a listagem falhar).
    """
    output_dir = Path(output_dir)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    listagem_url = f"{_API_BASE}{_LISTAGEM_PATH}"
    listagem = _http_get_text(listagem_url)
    if listagem is None:
        logger.error(
            "[camara_goiania] failed to fetch listagem %s — aborting",
            listagem_url,
        )
        return []

    listagem_html, listagem_bytes, listagem_ctype = listagem
    (raw_dir / "parlamentares.html").write_bytes(listagem_bytes)

    slugs = _extract_profile_slugs(listagem_html)
    if not slugs:
        logger.error(
            "[camara_goiania] listagem returned no parlamentar slugs",
        )
        return []
    if limit is not None:
        slugs = slugs[:limit]
    logger.info("[camara_goiania] %d parlamentares to fetch", len(slugs))

    run_id = _fetch_run_id() if archival else ""

    vereadores: list[dict[str, Any]] = []
    for idx, slug in enumerate(slugs, start=1):
        profile_url = f"{_API_BASE}{_LISTAGEM_PATH}{slug}"
        result = _http_get_text(profile_url)
        if result is None:
            logger.warning(
                "[camara_goiania] skipping %s (HTTP failed)", slug,
            )
            continue
        html, raw_bytes, ctype = result
        (raw_dir / f"perfil_{slug}.html").write_bytes(raw_bytes)

        record = _parse_profile_html(html, slug, profile_url)
        if archival:
            uri = archive_fetch(
                url=profile_url,
                content=raw_bytes,
                content_type=ctype,
                run_id=run_id,
                source_id="camara_goiania",
            )
            record[_SNAPSHOT_KEY] = uri

        vereadores.append(record)
        logger.debug(
            "[camara_goiania] %d/%d %s party=%s gabinete=%s",
            idx, len(slugs), slug, record["party"], record["gabinete"],
        )
        if idx < len(slugs):
            time.sleep(_REQUEST_DELAY_SECONDS)

    # Archival da listagem fora do loop — uma URI separada (não vai pra
    # nenhum vereador específico).
    if archival and listagem_bytes:
        archive_fetch(
            url=listagem_url,
            content=listagem_bytes,
            content_type=listagem_ctype,
            run_id=run_id,
            source_id="camara_goiania",
        )

    target = output_dir / "vereadores.json"
    target.write_text(
        json.dumps(vereadores, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(
        "[camara_goiania] wrote %s (%d vereadores)", target, len(vereadores),
    )
    return [target]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _snapshot_from_record(row: dict[str, Any]) -> str | None:
    """Defensive read of the private ``__snapshot_uri`` propagation key."""
    raw = row.get(_SNAPSHOT_KEY)
    if isinstance(raw, str) and raw:
        return raw
    return None


class CamaraGoianiaPipeline(Pipeline):
    """Ingest GoVereador rows from the disk-cached vereadores.json."""

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
        super().__init__(
            driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs,
        )
        self._raw_vereadores: list[dict[str, Any]] = []
        self.vereadores: list[dict[str, Any]] = []

    @staticmethod
    def _load_json_file(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning(
                "[camara_goiania] %s is not valid JSON — skipping", path,
            )
            return []
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        return []

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "camara_goiania"
        self._raw_vereadores = self._load_json_file(src_dir / "vereadores.json")
        if self.limit:
            self._raw_vereadores = self._raw_vereadores[: self.limit]
        logger.info(
            "[camara_goiania] extracted vereadores=%d",
            len(self._raw_vereadores),
        )

    def transform(self) -> None:
        vereadores: list[dict[str, Any]] = []
        for row in self._raw_vereadores:
            name = normalize_name(str(row.get("name") or row.get("nome") or ""))
            party = str(row.get("party") or row.get("partido") or "").strip()
            slug = str(row.get("slug") or "").strip()
            if not name:
                continue

            vid = _stable_id("camara_goiania", name, party)
            record_id = f"{name}|{party}|{slug}"
            snapshot_uri = _snapshot_from_record(row)

            vereadores.append(self.attach_provenance(
                {
                    "vereador_id": vid,
                    "name": name,
                    "slug": slug,
                    "party": party,
                    "photo_url": str(row.get("photo_url") or ""),
                    "gabinete": str(row.get("gabinete") or "").strip(),
                    "phones": str(row.get("phones") or "").strip(),
                    "email": str(row.get("email") or "").strip(),
                    "birth_date": str(row.get("birth_date") or "").strip(),
                    "bio_summary": str(row.get("bio_summary") or "").strip(),
                    "profile_url": str(row.get("profile_url") or ""),
                    "legislature": str(
                        row.get("legislature") or "20",
                    ).strip(),
                    "uf": "GO",
                    "municipality": "Goiania",
                    "municipality_code": "5208707",
                    "source": "camara_goiania",
                },
                record_id=record_id,
                snapshot_uri=snapshot_uri,
            ))

        self.vereadores = deduplicate_rows(vereadores, ["vereador_id"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        if self.vereadores:
            loader.load_nodes(
                "GoVereador", self.vereadores, key_field="vereador_id",
            )

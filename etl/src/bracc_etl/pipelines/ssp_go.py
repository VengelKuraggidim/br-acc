"""ETL pipeline for Goias public-security statistics.

SSP-GO (Secretaria de Seguranca Publica de Goias, via goias.gov.br/seguranca)
publishes aggregate statistics as one PDF per year
(``estatisticas_<YYYY>.pdf`` plus consolidated multi-year PDFs). There
is no CSV/XLSX export, no Power BI embed, and no CKAN dataset with
occurrence counts (the only CSV the SSP organization owns on
``dadosabertos.go.gov.br`` is ``doacoes-recebidas-ssp`` — donations,
unrelated to crime statistics). Audited 2026-04-22.

Granularity is **state-wide** only (no per-municipality breakdown
upstream). Each yearly bulletin is a single page with a table of
~15 crime naturezas × 12 months + a TOTAL column. The pipeline parses
those PDFs with :mod:`pypdf` and materialises one row per
(naturaza × mes), stamped with ``cod_ibge=5200000`` (UF GO sentinel)
and ``municipality="ESTADO DE GOIAS"``.

Extraction strategy (in order of preference):

1. If ``data/ssp_go/ocorrencias.csv`` exists, read it verbatim. Lets
   operators drop a LAI-obtained CSV with per-municipality data next to
   the pipeline and override the PDF parse path.
2. Otherwise, scrape the estatisticas index online, download each
   yearly PDF, archive the bytes via :func:`archive_fetch`, and parse
   the crime table. Snapshot URIs are stamped back onto the rows via
   ``source_snapshot_uri``.
3. Offline fallback: parse any ``estatisticas*.pdf`` found directly
   under ``data/ssp_go/`` (useful when the portal is unreachable but a
   prior ``fetch_to_disk`` dump is cached locally).

Data source: https://goias.gov.br/seguranca/estatisticas/
Per-municipality data is tracked as a débito — see
``todo-list-prompts/high_priority/debitos/ssp-go-granularidade-municipio.md``.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    normalize_name,
    row_pick,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Upstream URLs used by ``fetch_to_disk``. Kept at module scope so tests
# (and any future monkeypatching) can override them without editing the
# function body.
_ESTATISTICAS_INDEX_URL = "https://goias.gov.br/seguranca/estatisticas/"
_DADOS_ABERTOS_BASE = "https://dadosabertos.go.gov.br/api/3/action"
_DOACOES_SSP_DATASET = "doacoes-recebidas-ssp"

# Match ``<a href="…estatisticas…/something.pdf">`` and
# ``<a href="…/Estatisticas-de-…pdf">`` variants, case-insensitive, on a
# single line. The site uploads are versioned via ``/sites/56/YYYY/MM/``
# path prefixes, so the filename is the only stable slug we store.
_PDF_HREF_RE = re.compile(
    r'href="(https?://[^"]*?[Ee]statistica[^"]*?\.pdf)"',
)

# Extrai o ano do nome do arquivo PDF (ex.: ``estatisticas_2024.pdf`` ou
# ``Estatisticas-de-2025.pdf``) — usado pra casar snapshot_uri com a coluna
# ``periodo`` das rows (``YYYY-MM``) na etapa de transform.
_PDF_YEAR_RE = re.compile(r"(?:^|[_\-])(\d{4})(?=\.pdf$|[_\-])", re.IGNORECASE)
# Fallback content-type: quando o servidor da SSP-GO não carimba
# ``Content-Type`` na resposta, assumimos PDF — é o único tipo que o
# fetch de PDFs trata. CSV tem seu próprio content_type vindo do CKAN.
_PDF_CONTENT_TYPE = "application/pdf"


def _extract_pdf_links(html: str) -> list[str]:
    """Return unique PDF URLs referenced from the SSP estatisticas page.

    Deduplicated while preserving first-seen order so the downstream
    ``limit`` cap is deterministic (``--limit 2`` picks the first two
    PDFs in page order, not a random pair).
    """
    seen: set[str] = set()
    urls: list[str] = []
    for match in _PDF_HREF_RE.finditer(html):
        url = match.group(1)
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _slug_from_pdf_url(url: str) -> str:
    """Return a stable filename for a remote SSP PDF bulletin."""
    # URL tail is the uploaded filename (e.g. ``estatisticas_2024.pdf``
    # or ``Estatisticas-de-2025.pdf``). Lowercase and collapse so files
    # sort chronologically in the destination directory.
    tail = url.rsplit("/", 1)[-1]
    return tail.lower()


def _year_from_pdf_slug(slug: str) -> str | None:
    """Return ``YYYY`` parsed from a PDF filename slug, or ``None``.

    Usado pra mapear snapshot URIs (por PDF anual) pros rows do CSV de
    ocorrências (chaveados por ``periodo = YYYY-MM``). Um match None
    significa que o nome do PDF não tem ano reconhecível e, portanto,
    nenhuma row será stampada a partir dele.
    """
    match = _PDF_YEAR_RE.search(slug)
    return match.group(1) if match else None


# Sentinela do IBGE pro estado de Goiás inteiro — os boletins anuais do
# SSP-GO só publicam totais estaduais, então todas as rows carregam esse
# valor. Operadores que tiverem dado municipal (ex.: via LAI) sobrescrevem
# via ``ocorrencias.csv`` e o parser é pulado.
_GO_STATE_COD_IBGE = "5200000"
_GO_STATE_MUNICIPIO = "ESTADO DE GOIAS"

# Taxonomia que o boletim publica (confirmada 2018–2025). Usada só pra
# logar drift — o parser não rejeita linhas desconhecidas, pra não falhar
# silenciosamente se a SSP adicionar uma naturaza nova.
_KNOWN_NATUREZAS: frozenset[str] = frozenset({
    "HOMICIDIO DOLOSO",
    "FEMINICIDIO",
    "ESTUPRO",
    "LATROCINIO",
    "LESAO SEGUIDA DE MORTE",
    "ROUBO A TRANSEUNTE",
    "ROUBO DE VEICULOS",
    "ROUBO EM COMERCIO",
    "ROUBO EM RESIDENCIA",
    "ROUBO DE CARGA",
    "ROUBO A INSTITUICAO FINANCEIRA",
    "FURTO DE VEICULOS",
    "FURTO EM COMERCIO",
    "FURTO EM RESIDENCIA",
    "FURTO A TRANSEUNTE",
})

# Regex do cabeçalho "DEMONSTRATIVO - ANO YYYY" usado pra descobrir o ano
# do boletim direto do conteúdo — mais confiável que o nome do arquivo
# (o SSP às vezes sobe PDFs sob slugs inconsistentes ou re-postados com
# o mesmo nome).
_ANO_HEADER_RE = re.compile(
    r"DEMONSTRATIVO\s*[-–]\s*ANO\s+(\d{4})",
    re.IGNORECASE,
)


def _parse_number(token: str) -> int | None:
    """Parse um número do boletim (ex.: ``903`` ou ``28.119``).

    O PDF usa ``.`` como separador de milhar — só removemos pontos e
    convertemos. Retorna ``None`` quando o token não é um inteiro
    reconhecível, pro caller pular a linha sem tratar como erro fatal.
    """
    clean = token.replace(".", "")
    if not clean.isdigit():
        return None
    return int(clean)


def _parse_bulletin_pdf(pdf_bytes: bytes) -> tuple[int | None, list[dict[str, str]]]:
    """Extract (year, rows) from a single SSP-GO yearly bulletin.

    The bulletin is a 1-page PDF with a ``NATUREZAS × JAN..DEZ + TOTAL``
    table. We use :mod:`pypdf` to pull text and then parse line-by-line:
    any line whose last 13 whitespace-separated tokens are numbers is
    treated as a data row. The 13th number is the pre-computed TOTAL
    and is discarded — we re-expand to 12 monthly rows so downstream
    callers always see one row per ``periodo = YYYY-MM``.

    Returns ``(year, rows)``. ``year`` is pulled from the
    ``DEMONSTRATIVO - ANO YYYY`` header; if missing, returns
    ``(None, [])`` because rows without a ``periodo`` are useless.
    """
    # pypdf import lazy: keeps the module importable in environments
    # where PDF parsing is unused (already a hard dep in ``pyproject.toml``,
    # but this keeps the cost off the happy path when only CSVs are read).
    from io import BytesIO

    from pypdf import PdfReader

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:  # pypdf raises many things (ValueError, etc.)  # noqa: BLE001
        logger.warning("[ssp_go] could not read PDF bulletin: %s", exc)
        return None, []

    year_match = _ANO_HEADER_RE.search(text)
    if year_match is None:
        logger.warning(
            "[ssp_go] bulletin has no 'DEMONSTRATIVO - ANO YYYY' header; skipping",
        )
        return None, []
    year = int(year_match.group(1))

    rows: list[dict[str, str]] = []
    for raw_line in text.splitlines():
        parts = raw_line.strip().split()
        # A data row has 1+ natureza tokens + 12 monthly counts + 1 total.
        if len(parts) < 14:
            continue
        # Parse the trailing 13 tokens; bail if any isn't a number.
        numbers = [_parse_number(t) for t in parts[-13:]]
        if any(n is None for n in numbers):
            continue
        natureza_raw = " ".join(parts[:-13]).strip()
        if not natureza_raw:
            continue
        # Defensive: observation lines can occasionally end with
        # numeric tokens (CPFs, dates). If the "natureza" contains
        # digits it's almost certainly not a crime row.
        if any(c.isdigit() for c in natureza_raw):
            continue

        natureza = normalize_name(natureza_raw)
        if natureza not in _KNOWN_NATUREZAS:
            logger.info(
                "[ssp_go] unknown naturaza %r in %d bulletin; loading anyway",
                natureza, year,
            )
        for month_idx in range(12):
            quantidade = numbers[month_idx]
            assert quantidade is not None  # guarded by `any(n is None ...)` above
            rows.append({
                "municipio": _GO_STATE_MUNICIPIO,
                "cod_ibge": _GO_STATE_COD_IBGE,
                "natureza": natureza,
                "periodo": f"{year}-{month_idx + 1:02d}",
                "quantidade": str(quantidade),
            })
    return year, rows


def _download_binary(
    client: httpx.Client,
    url: str,
    target: Path,
    *,
    run_id: str | None = None,
    source_id: str | None = None,
    default_content_type: str = _PDF_CONTENT_TYPE,
) -> tuple[Path, str | None] | None:
    """Stream a URL to ``target``; return ``(path, snapshot_uri)`` or ``None``.

    Quando ``run_id`` e ``source_id`` são fornecidos, os bytes brutos
    também são gravados via :func:`bracc_etl.archival.archive_fetch` e a
    URI é devolvida. O download em disco é preservado (cache/debug);
    archival é idempotente então não duplica o conteúdo.

    Sem ``run_id``/``source_id`` o helper volta ao comportamento original
    (apenas escreve em disco e devolve a URI como ``None``), preservando
    o path do CLI ``fetch_to_disk`` onde archival não é necessária.
    """
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[ssp_go] failed to download %s: %s", url, exc)
        return None
    content = resp.content
    target.write_bytes(content)
    logger.info("[ssp_go] wrote %s (%d bytes)", target, len(content))

    snapshot_uri: str | None = None
    if run_id and source_id:
        content_type = resp.headers.get("content-type", default_content_type)
        snapshot_uri = archive_fetch(
            url=url,
            content=content,
            content_type=content_type,
            run_id=run_id,
            source_id=source_id,
        )
    return target, snapshot_uri


def _download_ckan_ssp_donations(
    client: httpx.Client,
    output_dir: Path,
) -> Path | None:
    """Download the ``doacoes-recebidas-ssp`` CSV from the state CKAN.

    Returns the written path or ``None`` when the dataset/resource could
    not be discovered (e.g. CKAN outage, schema change). Failures are
    logged but do not raise, so a PDF-only run still succeeds.
    """
    try:
        resp = client.get(
            f"{_DADOS_ABERTOS_BASE}/package_show",
            params={"id": _DOACOES_SSP_DATASET},
        )
        resp.raise_for_status()
        resources = resp.json().get("result", {}).get("resources", [])
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "[ssp_go] could not discover CKAN dataset %s: %s",
            _DOACOES_SSP_DATASET,
            exc,
        )
        return None

    csv_url: str | None = None
    for res in resources:
        if str(res.get("format", "")).upper() == "CSV" and res.get("url"):
            csv_url = str(res["url"])
            break
    if not csv_url:
        logger.warning(
            "[ssp_go] no CSV resource in CKAN dataset %s", _DOACOES_SSP_DATASET,
        )
        return None

    target = output_dir / "doacoes_ssp.csv"
    result = _download_binary(
        client, csv_url, target, default_content_type="text/csv",
    )
    if result is None:
        return None
    path, _uri = result
    return path


def fetch_to_disk(
    output_dir: Path | str,
    limit: int | None = None,
) -> list[Path]:
    """Download SSP-GO public-security raw artifacts to ``output_dir``.

    What gets written:

    - ``estatisticas_<slug>.pdf`` — one file per yearly bulletin linked
      from ``goias.gov.br/seguranca/estatisticas/``. These are the only
      machine-readable crime statistics SSP-GO publishes (PDFs, not
      CSVs — confirmed by upstream audit 2026-04-17).
    - ``doacoes_ssp.csv`` — the SSP organization's sole CSV resource on
      ``dadosabertos.go.gov.br`` (donations received). Useful as a
      transparency cross-check even though it is not crime-statistics.

    Args:
        output_dir: Destination directory. Created if missing.
        limit: Optional cap on the number of PDF bulletins to fetch
            (applied in page order — i.e. newest-first as the index
            lists them). ``None`` downloads every bulletin. The CKAN
            donations CSV is always fetched regardless of ``limit``,
            as it is a single file.

    Returns:
        List of files written. Empty when nothing could be fetched.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        # --- 1. Scrape the estatisticas index for PDF links.
        try:
            resp = client.get(_ESTATISTICAS_INDEX_URL)
            resp.raise_for_status()
            pdf_urls = _extract_pdf_links(resp.text)
        except httpx.HTTPError as exc:
            logger.error(
                "[ssp_go] could not fetch estatisticas index %s: %s",
                _ESTATISTICAS_INDEX_URL,
                exc,
            )
            pdf_urls = []

        if limit is not None and limit >= 0:
            pdf_urls = pdf_urls[:limit]

        logger.info(
            "[ssp_go] estatisticas index yielded %d PDF bulletin(s) to fetch",
            len(pdf_urls),
        )
        for url in pdf_urls:
            target = output_dir / _slug_from_pdf_url(url)
            result = _download_binary(client, url, target)
            if result:
                path, _uri = result
                written.append(path)

        # --- 2. CKAN donations CSV (single file, always attempted).
        donations = _download_ckan_ssp_donations(client, output_dir)
        if donations:
            written.append(donations)

    if not written:
        logger.warning(
            "[ssp_go] fetch_to_disk wrote no files — index and CKAN both empty",
        )
    return written


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


class SspGoPipeline(Pipeline):
    """Scaffold pipeline for Goias public-security aggregate statistics."""

    name = "ssp_go"
    source_id = "ssp_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        *,
        archive_pdfs: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_stats: pd.DataFrame = pd.DataFrame()
        self.stats: list[dict[str, Any]] = []
        # Opt-out switch pro fetch online dos PDFs anuais. Fixtures
        # offline (sem mock de HTTP) desativam via ``archive_pdfs=False``
        # pra não hit network. Produção e testes com ``MockTransport``
        # deixam ``True`` (default) — cada PDF baixado é persistido
        # content-addressed via :func:`archive_fetch`.
        self._archive_pdfs_enabled = archive_pdfs
        # Mapa ``YYYY -> snapshot_uri`` alimentado pelo fetch online dos
        # PDFs anuais do SSP-GO. ``transform`` usa pra carimbar
        # ``source_snapshot_uri`` em cada row cujo ``periodo`` (formato
        # ``YYYY-MM``) casa com um PDF arquivado. Vazio no caminho offline
        # (fixture/local CSV sem HTTP) — consistente com o contrato
        # opt-in do campo.
        self._pdf_snapshot_uris: dict[str, str] = {}

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        for sep in (";", ","):
            try:
                df = pd.read_csv(
                    path, sep=sep, dtype=str, keep_default_na=False,
                    encoding="utf-8", engine="python", on_bad_lines="skip",
                )
                if len(df.columns) > 1:
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        try:
            return pd.read_csv(
                path, sep=";", dtype=str, keep_default_na=False,
                encoding="latin-1", engine="python", on_bad_lines="skip",
            )
        except (OSError, pd.errors.ParserError) as exc:
            logger.warning("[ssp_go] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def _fetch_archive_parse_bulletins_online(
        self,
    ) -> tuple[list[dict[str, str]], dict[str, str]]:
        """Baixa, arquiva e parseia os PDFs anuais de estatisticas.

        Retorna ``(rows, {YYYY: snapshot_uri})``: ``rows`` são os
        registros por ``(natureza × mês)`` achatados através de todos os
        PDFs baixados, e o dict mapeia o ano extraído do nome do arquivo
        pra URI relativa devolvida por :func:`archive_fetch`.

        Falhas de HTTP são logadas e engolidas: o pipeline continua com
        a lista vazia se o portal estiver fora. Falhas de parse (PDF
        corrompido, cabeçalho faltando) também são absorvidas — o
        snapshot ainda é gravado mesmo que o parser não consiga extrair
        linhas, pra preservar o bruto pra análise manual.
        """
        rows: list[dict[str, str]] = []
        uris: dict[str, str] = {}
        try:
            with httpx.Client(timeout=60, follow_redirects=True) as client:
                try:
                    index = client.get(_ESTATISTICAS_INDEX_URL)
                    index.raise_for_status()
                    pdf_urls = _extract_pdf_links(index.text)
                except httpx.HTTPError as exc:
                    logger.warning(
                        "[ssp_go] could not fetch estatisticas index %s: %s",
                        _ESTATISTICAS_INDEX_URL,
                        exc,
                    )
                    return rows, uris

                if self.limit is not None and self.limit >= 0:
                    pdf_urls = pdf_urls[: self.limit]

                for url in pdf_urls:
                    try:
                        resp = client.get(url)
                        resp.raise_for_status()
                    except httpx.HTTPError as exc:
                        logger.warning(
                            "[ssp_go] failed to download %s: %s", url, exc,
                        )
                        continue
                    content_type = resp.headers.get(
                        "content-type", _PDF_CONTENT_TYPE,
                    )
                    uri = archive_fetch(
                        url=url,
                        content=resp.content,
                        content_type=content_type,
                        run_id=self.run_id,
                        source_id=self.source_id,
                    )
                    slug = _slug_from_pdf_url(url)
                    slug_year = _year_from_pdf_slug(slug)
                    if slug_year is not None:
                        # PDFs mais recentes sobrescrevem os antigos pra
                        # um mesmo ano — consistente com a ordem "newest
                        # first" do índice HTML.
                        uris.setdefault(slug_year, uri)
                    logger.info(
                        "[ssp_go] archived PDF %s -> %s", slug, uri,
                    )

                    _parsed_year, parsed_rows = _parse_bulletin_pdf(resp.content)
                    rows.extend(parsed_rows)
        except httpx.HTTPError as exc:
            logger.warning("[ssp_go] online archival aborted: %s", exc)
        return rows, uris

    def _parse_local_bulletins(self, src_dir: Path) -> list[dict[str, str]]:
        """Parse cached ``estatisticas*.pdf`` found directly under ``src_dir``.

        Offline fallback path: if the online scrape fails (or archival
        is opt-out) but a prior ``fetch_to_disk`` run left PDFs on
        disk, those get parsed so the pipeline still loads. Snapshot
        URIs are *not* populated here — provenance lives in the
        archival layer, which is online-only.
        """
        rows: list[dict[str, str]] = []
        if not src_dir.exists():
            return rows
        for pdf_path in sorted(src_dir.glob("estatisticas*.pdf")):
            try:
                pdf_bytes = pdf_path.read_bytes()
            except OSError as exc:
                logger.warning("[ssp_go] could not read %s: %s", pdf_path, exc)
                continue
            _year, parsed_rows = _parse_bulletin_pdf(pdf_bytes)
            rows.extend(parsed_rows)
        return rows

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "ssp_go"
        # Operator-supplied CSV (ex.: LAI com dado municipal) tem
        # precedência sobre os PDFs. Se presente e não-vazio, pula o
        # scrape online do boletim anual.
        operator_csv: pd.DataFrame | None = None
        csv_path = src_dir / "ocorrencias.csv"
        if csv_path.exists() and csv_path.stat().st_size > 0:
            df = self._read_csv_optional(csv_path)
            if not df.empty:
                operator_csv = df

        # Online archival + parse dos PDFs anuais. Rodando produção com
        # ``archive_pdfs=True`` (default), tenta scrape do índice HTML +
        # download de cada PDF pra gerar snapshot via :func:`archive_fetch`
        # **e** materializar as rows. Falhas de HTTP são absorvidas.
        # Testes offline passam ``archive_pdfs=False`` pra evitar network.
        online_rows: list[dict[str, str]] = []
        if self._archive_pdfs_enabled:
            online_rows, self._pdf_snapshot_uris = (
                self._fetch_archive_parse_bulletins_online()
            )

        # Triagem final: CSV do operador > rows online > PDFs cacheados
        # localmente. Só cai no parse local quando nenhum dos dois
        # caminhos de cima rendeu linhas (portal fora + sem CSV).
        if operator_csv is not None:
            self._raw_stats = operator_csv
        elif online_rows:
            self._raw_stats = pd.DataFrame(online_rows)
        else:
            local_rows = self._parse_local_bulletins(src_dir)
            if local_rows:
                self._raw_stats = pd.DataFrame(local_rows)

        if self._raw_stats.empty:
            logger.warning(
                "[ssp_go] nenhuma fonte de dados disponível em %s "
                "(sem ocorrencias.csv, sem PDFs locais, portal offline?)",
                src_dir,
            )

        if self.limit:
            self._raw_stats = self._raw_stats.head(self.limit)
        self.rows_in = len(self._raw_stats)

    def transform(self) -> None:
        for _, row in self._raw_stats.iterrows():
            municipio = normalize_name(
                row_pick(row, "municipio", "nome_municipio", "cidade"),
            )
            cod_ibge = row_pick(row, "cod_ibge", "codigo_ibge", "ibge")
            crime_type = normalize_name(
                row_pick(
                    row, "natureza", "tipo_ocorrencia", "crime", "classificacao",
                ),
            )
            periodo = row_pick(row, "periodo", "mes_ano", "data", "ano")
            count_raw = row_pick(row, "quantidade", "total", "count", "ocorrencias")
            try:
                count = int(float(str(count_raw).replace(",", ".")))
            except (TypeError, ValueError):
                count = 0
            if not municipio and not cod_ibge:
                continue
            stat_id = _hash_id(cod_ibge, municipio, crime_type, periodo)
            stat_record_id = f"{cod_ibge}|{crime_type}|{periodo}"
            # Resolve snapshot URI pela coluna ``periodo``: o bulletin do
            # SSP é anual, então usamos o prefixo ``YYYY`` pra casar com
            # o PDF arquivado. Sem PDF pro ano da row → ``None`` →
            # ``attach_provenance`` não injeta a chave (opt-in).
            snapshot_uri: str | None = None
            if self._pdf_snapshot_uris and periodo:
                year_prefix = str(periodo)[:4]
                snapshot_uri = self._pdf_snapshot_uris.get(year_prefix)
            self.stats.append(self.attach_provenance(
                {
                    "stat_id": stat_id,
                    "cod_ibge": cod_ibge,
                    "municipality": municipio,
                    "crime_type": crime_type,
                    "period": periodo,
                    "count": count,
                    "uf": "GO",
                    "source": "ssp_go",
                },
                record_id=stat_record_id,
                snapshot_uri=snapshot_uri,
            ))

        self.stats = deduplicate_rows(self.stats, ["stat_id"])
        self.rows_loaded = len(self.stats)

    def load(self) -> None:
        if not self.stats:
            logger.warning("[ssp_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        loader.load_nodes("GoSecurityStat", self.stats, key_field="stat_id")

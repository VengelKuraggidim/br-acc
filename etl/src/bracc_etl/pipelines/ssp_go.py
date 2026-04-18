"""ETL pipeline scaffold for Goias public-security statistics.

SSP-GO (Secretaria de Seguranca Publica de Goias, via goias.gov.br/seguranca)
publishes aggregate yearly statistics. This scaffold accepts
pre-downloaded CSV files under ``data/ssp_go/`` with the expected shape:

- ``ocorrencias.csv``  -> GoSecurityStat nodes (aggregate counts by
                          municipality / crime type / period)

Upstream availability (audited 2026-04-17):

- ``goias.gov.br/seguranca/estatisticas/`` publishes one PDF per year
  (``estatisticas_<YYYY>.pdf``, plus consolidated multi-year PDFs). No
  CSV/XLSX export is exposed.
- ``dadosabertos.go.gov.br`` (state CKAN) has **no** "ocorrencias by
  municipality" dataset. The only CSV owned by the SSP organization is
  ``doacoes-recebidas-ssp`` (donations received), unrelated to crime
  statistics. Police-civil exposes only a 14-row crime-type taxonomy
  (``crimes-registrados-pela-delegacia-virtual``).

``fetch_to_disk`` therefore downloads the yearly PDF bulletins (the real
machine-readable output SSP-GO publishes) plus the SSP donations CSV.
Extracting tabular occurrence counts from the PDFs is out of scope of
this fetch layer — the pipeline's ``extract`` still reads
``ocorrencias.csv`` when an operator provides one. Once a PDF parser is
added, drop it next to ``fetch_to_disk`` and materialize
``ocorrencias.csv`` from the yearly PDFs.

Human validation required:

1. Decide on the canonical crime-type taxonomy (SSP's own categories vs.
   a unified set used across Brazilian state security agencies).
2. Validate CSV schema once an operator exports a sample.
3. Build the PDF -> tabular extractor for ``estatisticas_<YYYY>.pdf``.

Data source: https://goias.gov.br/seguranca/
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

    def _archive_pdf_bulletins_online(self) -> dict[str, str]:
        """Baixa e arquiva os PDFs anuais de estatisticas (online-only).

        Retorna ``{YYYY: snapshot_uri}`` — chave é o ano extraído do nome
        do arquivo, valor é a URI relativa devolvida por
        :func:`bracc_etl.archival.archive_fetch`. PDFs cujo nome não tem
        ano reconhecível são arquivados mas ficam fora do dict (nenhuma
        row consegue casar com eles via ``periodo``).

        Falhas de HTTP são logadas e engolidas: o pipeline continua com
        o CSV offline mesmo se o portal do SSP estiver fora do ar.
        """
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
                    return uris

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
                    year = _year_from_pdf_slug(slug)
                    if year is not None:
                        # PDFs mais recentes sobrescrevem os antigos pra
                        # um mesmo ano — consistente com a ordem "newest
                        # first" do índice HTML.
                        uris.setdefault(year, uri)
                    logger.info(
                        "[ssp_go] archived PDF %s -> %s", slug, uri,
                    )
        except httpx.HTTPError as exc:
            logger.warning("[ssp_go] online archival aborted: %s", exc)
        return uris

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "ssp_go"
        if not src_dir.exists():
            logger.warning(
                "[ssp_go] expected directory %s missing; "
                "export SSP-GO aggregate CSVs there.",
                src_dir,
            )
            return
        self._raw_stats = self._read_csv_optional(src_dir / "ocorrencias.csv")
        if self.limit:
            self._raw_stats = self._raw_stats.head(self.limit)
        self.rows_in = len(self._raw_stats)

        # Online archival dos PDFs anuais do SSP. Rodando produção com
        # ``archive_pdfs=True`` (default), tenta scrape do índice HTML +
        # download de cada PDF pra gerar snapshot via
        # :func:`archive_fetch`. Falhas de HTTP são absorvidas — se o
        # portal estiver fora, o dict fica vazio e rows não ganham
        # ``source_snapshot_uri`` (opt-in preservado). Testes offline
        # passam ``archive_pdfs=False`` pra evitar network.
        if self._archive_pdfs_enabled:
            self._pdf_snapshot_uris = self._archive_pdf_bulletins_online()

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

"""ETL pipeline for TCM-GO (Tribunal de Contas dos Municipios de Goias).

TCM-GO publishes two public sanction-flavored lists at https://www.tcmgo.tc.br/:

* "Impedidos de licitar, contratar ou exercer cargo publico" — rendered only
  as an embedded Power BI report; no machine-readable export is available at
  time of writing. Operators may still drop a manually exported
  ``impedidos.csv`` under ``data/tcmgo_sancoes/`` if they obtain one via LAI.
* "Contas com Parecer Previo pela Rejeicao ou julgadas Irregulares" — exposed
  as an unauthenticated CSV via the TCM-GO Web Services catalog (service #31,
  https://ws.tcm.go.gov.br/api/rest/dados/contas-irregulares). CPFs arrive
  pre-masked; each row represents one agent x process with an "Assunto"
  (proceedings type) and a TipoLista category.

``fetch_to_disk`` hits that public CSV endpoint, normalises headers to the
aliases this pipeline already accepts, and writes ``impedidos.csv`` under the
target directory — so the same ingest code path used for LAI-derived exports
also handles the automated pull. Operators may still drop their own
``rejeitados.csv`` alongside it (expected layout is municipality x exercise
x parecer).

Pipeline outputs:

- ``impedidos.csv``  -> TcmGoImpedido nodes + IMPEDIDO_TCMGO rels (only when
  a row carries a CNPJ, which is uncommon for this source).
- ``rejeitados.csv`` -> TcmGoRejectedAccount nodes (optional, operator-fed).

Notes:

- This is separate from the ``tcm_go`` pipeline already in the registry,
  which ingests SICONFI fiscal data for GO municipalities (different source).
- The public CSV masks CPFs at the origin (``76***.***-***``), so the
  ``mask_cpf`` transform is a no-op defensive shim for this source.

Data source: https://www.tcmgo.tc.br/
API endpoint: https://ws.tcm.go.gov.br/api/rest/dados/contas-irregulares
"""

from __future__ import annotations

import csv
import hashlib
import logging
import re
import time as _time
from html.parser import HTMLParser
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd
from defusedxml import ElementTree as DefusedET

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    mask_cpf,
    normalize_name,
    parse_date,
    row_pick,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

CONTAS_IRREGULARES_URL = (
    "https://ws.tcm.go.gov.br/api/rest/dados/contas-irregulares"
)
_HTTP_TIMEOUT = 60.0
_USER_AGENT = "br-acc-etl/1.0 (+https://github.com/brunoclz/br-acc)"
# Fallback content-type quando o endpoint do TCM-GO omite o header (acontece
# ocasionalmente na API de dados abertos). Archival é content-addressed, então
# o único efeito do fallback é a extensão do arquivo gravado (.csv vs .bin).
_CONTAS_CONTENT_TYPE = "text/csv"

# Column map: TCM-GO Portal CSV header -> tcmgo_sancoes pipeline alias.
# The pipeline's ``transform`` step uses ``row_pick`` over these aliases,
# so we rewrite headers once at download time and keep the ETL schema stable.
_CONTAS_HEADER_MAP: dict[str, str] = {
    "CPF": "cpf_cnpj",
    "Nome": "nome",
    "Assunto": "motivo",
    "Processo/Fase": "processo",
    "Data Julgamento": "data_inicio",
    "Dt. Trânsito Julgado": "data_fim",
    "Município": "municipio",
    "Mês/Ano": "exercicio",
    "Acórdão/Resolução": "acordao",
    "Url": "url",
    "TipoLista": "tipo_lista",
}


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _is_premasked_cpf(raw: str) -> bool:
    """Match upstream TCM-GO's pre-masked CPF format (``NN***.***-***``).

    O TCM-GO usa o shape abreviado ``NN***.***-***`` (1 ponto + 1 traco,
    nao o CPF pontuado tradicional). Exige asterisco, ponto e traco pra
    nao colidir com strings aleatorias.
    """
    if not raw:
        return False
    s = raw.strip()
    return "*" in s and "." in s and "-" in s


def _rewrite_contas_csv(
    raw_text: str,
    out_path: Path,
    limit: int | None = None,
) -> int:
    """Rewrite the public TCM-GO CSV with headers the pipeline expects.

    The upstream file uses PT-BR column names with accents (``Município``,
    ``Mês/Ano``). We map them to the aliases ``row_pick`` looks for in
    :meth:`TcmgoSancoesPipeline.transform` and drop any column we don't know.
    Output is written semicolon-separated (matching the repo's CSV
    fixtures) so both the pipeline and manual operator exports share the
    same on-disk shape.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    reader = csv.reader(raw_text.splitlines())
    try:
        header = next(reader)
    except StopIteration as exc:
        msg = "empty CSV returned by TCM-GO contas-irregulares endpoint"
        raise RuntimeError(msg) from exc

    rewritten_header = [_CONTAS_HEADER_MAP.get(col, col) for col in header]

    rows_written = 0
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(rewritten_header)
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue
            writer.writerow(row)
            rows_written += 1
            if limit is not None and rows_written >= limit:
                break
    return rows_written


def fetch_to_disk(
    output_dir: Path | str,
    limit: int | None = None,
    url: str = CONTAS_IRREGULARES_URL,
    timeout: float = _HTTP_TIMEOUT,
) -> list[Path]:
    """Download the TCM-GO "contas irregulares" CSV and stage it on disk.

    Writes ``impedidos.csv`` under ``output_dir`` using semicolon-separated
    values with headers aliased to the names the pipeline's
    :meth:`transform` step already recognises (``cpf_cnpj``, ``nome``,
    ``motivo``, ``processo``, ``data_inicio``, ``data_fim``, plus auxiliary
    ``municipio``, ``exercicio``, ``acordao``, ``url``, ``tipo_lista``).

    Args:
        output_dir: Destination directory. Created if missing.
        limit: Optional cap on the number of data rows written (header
            always preserved). Useful for smoke tests.
        url: Override for the public API endpoint. Defaults to TCM-GO's
            ``contas-irregulares`` open-data service.
        timeout: HTTP timeout in seconds.

    Returns:
        List of file paths written (sorted).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "[tcmgo_sancoes] fetching %s (limit=%s) -> %s",
        url, limit, output_dir,
    )
    with httpx.Client(
        timeout=timeout,
        headers={"User-Agent": _USER_AGENT, "Accept": "text/csv,*/*"},
        follow_redirects=True,
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        # TCM-GO ships UTF-8 but doesn't always declare charset.
        text = response.content.decode(response.encoding or "utf-8")

    out_csv = output_dir / "impedidos.csv"
    rows = _rewrite_contas_csv(text, out_csv, limit=limit)
    logger.info(
        "[tcmgo_sancoes] wrote %s (%d data rows)", out_csv, rows,
    )
    return [out_csv]


# ──────────────────────────────────────────────────────────────────────
# JSF scraper — "Impedidos de licitar ou contratar" (widget PrimeFaces)
# ──────────────────────────────────────────────────────────────────────
#
# O TCM-GO publica a lista de impedidos de licitar num iframe PrimeFaces em
# https://tcmgo.tc.br/portalwidgets/xhtml/impedimento/impedimento.jsf que
# NAO tem endpoint REST correspondente (varredura completa do catalogo
# /api/rest/servicoRest/all confirmou). A unica forma de extrair e scrapear
# o DataTable via HTTP GET inicial (pra pegar ViewState) + POST JSF
# partial-ajax por pagina.
#
# Schema da tabela: Nome | CPF/CNPJ | Inicio | Termino | Orgao | Processo | Situacao
#
# Cuidados:
#   - robots.txt do subdominio tcmgo.tc.br precisa ser checado manualmente
#     antes do primeiro run em producao (o www.tcmgo.tc.br tem Disallow:/
#     mas o subdominio pode divergir).
#   - Rate limit: 1s entre requests (constante _JSF_RATE_LIMIT_SECONDS).
#   - User-Agent: identificado via _USER_AGENT.

_IMPEDIDOS_JSF_URL = (
    "https://tcmgo.tc.br/portalwidgets/xhtml/impedimento/impedimento.jsf"
)
_JSF_RATE_LIMIT_SECONDS = 1.0
_JSF_PAGE_SIZE = 20  # tamanho padrao da DataTable PrimeFaces.
_JSF_MAX_PAGES = 500  # defesa em profundidade; 500*20 = 10k rows max.
_IMPEDIDOS_CSV_NAME = "impedidos_licitar.csv"
_IMPEDIDOS_JSF_CONTENT_TYPE = "text/html"

_VIEWSTATE_RE = re.compile(
    r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"',
)


def _extract_viewstate(html: str) -> str | None:
    """Retorna o token javax.faces.ViewState do HTML inicial.

    PrimeFaces grava o token em um hidden input; capturamos por regex pra
    evitar parsear o HTML inteiro so pra 1 atributo. Ausencia devolve
    ``None`` — o scraper trata como "sessao expirada" e aborta o loop.
    """
    if not html:
        return None
    match = _VIEWSTATE_RE.search(html)
    return match.group(1) if match else None


class _JsfTableParser(HTMLParser):
    """HTMLParser que extrai linhas de DataTable PrimeFaces.

    Estrategia: rastreia entrada em <tbody>, acumula <td> de cada <tr>,
    emite linha no fechamento do <tr>. Ignora headers (<thead>) e
    qualquer conteudo fora do tbody.

    Usado em duas situacoes:
      1. HTML completo da pagina inicial (GET).
      2. Fragmento HTML dentro do <update><![CDATA[...]]></update> da resposta
         PrimeFaces partial-ajax (POST).
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._in_tbody = 0
        self._in_tr = False
        self._in_td = False
        self._current_row: list[str] = []
        self._current_cell: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag == "tbody":
            self._in_tbody += 1
        elif tag == "tr" and self._in_tbody:
            self._in_tr = True
            self._current_row = []
        elif tag == "td" and self._in_tr:
            self._in_td = True
            self._current_cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._in_td:
            self._current_row.append(
                "".join(self._current_cell).strip(),
            )
            self._in_td = False
        elif tag == "tr" and self._in_tr:
            if self._current_row:
                self.rows.append(self._current_row)
            self._in_tr = False
        elif tag == "tbody" and self._in_tbody:
            self._in_tbody -= 1

    def handle_data(self, data: str) -> None:
        if self._in_td:
            self._current_cell.append(data)


def _parse_jsf_table(html_fragment: str) -> list[dict[str, str]]:
    """Parsa linhas do widget JSF para dicts com as colunas da DataTable.

    Aceita tanto o HTML completo da pagina inicial quanto o fragmento
    CDATA-wrapped devolvido pelo partial-response. Assume schema fixo da
    tabela: Nome | CPF/CNPJ | Inicio | Termino | Orgao | Processo | Situacao.
    Linhas com menos colunas sao descartadas — protege contra paginas
    malformadas.
    """
    parser = _JsfTableParser()
    parser.feed(html_fragment)
    parser.close()
    out: list[dict[str, str]] = []
    for row in parser.rows:
        if len(row) < 7:
            continue
        out.append({
            "nome": row[0],
            "cpf_cnpj": row[1],
            "data_inicio": row[2],
            "data_fim": row[3],
            "orgao": row[4],
            "processo": row[5],
            "situacao": row[6],
        })
    return out


def _parse_partial_response(xml_text: str) -> tuple[list[dict[str, str]], str | None]:
    """Extrai linhas + ViewState atualizado de um partial-response PrimeFaces.

    PrimeFaces AJAX devolve XML ``<partial-response>`` com zero ou mais
    ``<update id="...">`` contendo HTML em CDATA. Procuramos pelo update
    da DataTable (``form:impedimentos``) pra linhas e pelo update do
    ViewState pro proximo POST. Ambos sao opcionais em teoria; na pratica
    o servidor sempre manda os dois.

    Retorna ``(rows, new_viewstate)``. ``new_viewstate`` pode ser ``None``
    se o servidor nao reemitiu o token (reusa o anterior).
    """
    try:
        root = DefusedET.fromstring(xml_text)
    except DefusedET.ParseError as exc:
        logger.warning("[tcmgo_sancoes] JSF partial parse falhou: %s", exc)
        return [], None

    rows: list[dict[str, str]] = []
    new_viewstate: str | None = None
    for update in root.iter("update"):
        update_id = update.attrib.get("id", "")
        text = update.text or ""
        if "javax.faces.ViewState" in update_id:
            new_viewstate = text.strip()
        elif "impedimentos" in update_id:
            rows.extend(_parse_jsf_table(text))
    return rows, new_viewstate


def _build_ajax_payload(
    view_state: str,
    first: int = 0,
    rows_per_page: int = _JSF_PAGE_SIZE,
) -> dict[str, str]:
    """Monta o payload POST do PrimeFaces partial-ajax pra paginacao.

    Os nomes dos campos seguem a convencao PrimeFaces: a fonte do evento
    (``javax.faces.source=form:impedimentos``), o flag partial-ajax,
    qual o execute/render, o offset (_first), o tamanho (_rows), e o
    token ViewState renovado a cada resposta.
    """
    return {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "form:impedimentos",
        "javax.faces.partial.execute": "form:impedimentos",
        "javax.faces.partial.render": "form:impedimentos",
        "form:impedimentos": "form:impedimentos",
        "form:impedimentos_pagination": "true",
        "form:impedimentos_first": str(first),
        "form:impedimentos_rows": str(rows_per_page),
        "form:impedimentos_encodeFeature": "true",
        "form": "form",
        "javax.faces.ViewState": view_state,
    }


def fetch_impedidos_jsf(
    output_dir: Path | str,
    *,
    limit: int | None = None,
    url: str = _IMPEDIDOS_JSF_URL,
    timeout: float = _HTTP_TIMEOUT,
    client: httpx.Client | None = None,
    rate_limit_seconds: float = _JSF_RATE_LIMIT_SECONDS,
) -> Path:
    """Scrapea a lista de impedidos-de-licitar do TCM-GO via JSF e grava CSV.

    Fluxo:
      1. GET inicial na pagina do iframe → coleta ViewState + 1a pagina.
      2. Loop: POST partial-ajax pra ``_first=N`` com pagina ``rows_per_page``
         ate receber pagina vazia (fim da lista) ou bater o cap de seguranca.

    Rate-limit: ``rate_limit_seconds`` entre cada POST (default 1s).

    Args:
      output_dir: destino; o arquivo e criado como
        ``<output_dir>/impedidos_licitar.csv`` no shape que o pipeline
        ``TcmgoSancoesPipeline`` le em ``extract``.
      limit: cap opcional no total de rows emitidas (testes smoke).
      url: override da URL do widget (pra apontar pra staging/fixture).
      timeout: timeout HTTP dos requests.
      client: httpx.Client injetavel (pra tests offline com MockTransport).
      rate_limit_seconds: pausa entre POSTs de paginacao (default 1.0s).

    Returns:
      Path do arquivo CSV escrito.

    Raises:
      RuntimeError: se o GET inicial nao retornar ViewState (servidor
        devolveu erro ou pagina mudou radicalmente).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = output_dir / _IMPEDIDOS_CSV_NAME

    owns_client = client is None
    if client is None:
        client = httpx.Client(
            timeout=timeout,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            follow_redirects=True,
        )

    all_rows: list[dict[str, str]] = []
    try:
        initial = client.get(url)
        initial.raise_for_status()
        view_state = _extract_viewstate(initial.text)
        if not view_state:
            msg = (
                f"[tcmgo_sancoes] JSF inicial sem ViewState em {url} — "
                "widget mudou o layout ou servidor devolveu erro."
            )
            raise RuntimeError(msg)
        first_page = _parse_jsf_table(initial.text)
        all_rows.extend(first_page)
        logger.info(
            "[tcmgo_sancoes] JSF pagina 0: %d linhas (viewstate=%s...)",
            len(first_page), view_state[:12],
        )

        first = len(first_page)
        for page_idx in range(1, _JSF_MAX_PAGES):
            if limit is not None and len(all_rows) >= limit:
                break
            _time.sleep(rate_limit_seconds)
            ajax_headers = {
                "Faces-Request": "partial/ajax",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": (
                    "application/x-www-form-urlencoded; charset=UTF-8"
                ),
                "Accept": "application/xml,text/xml,*/*;q=0.01",
            }
            payload = _build_ajax_payload(
                view_state, first=first, rows_per_page=_JSF_PAGE_SIZE,
            )
            resp = client.post(url, data=payload, headers=ajax_headers)
            resp.raise_for_status()
            rows, new_vs = _parse_partial_response(resp.text)
            if not rows:
                logger.info(
                    "[tcmgo_sancoes] JSF pagina %d: vazia — fim da lista",
                    page_idx,
                )
                break
            all_rows.extend(rows)
            first += len(rows)
            if new_vs:
                view_state = new_vs
            logger.info(
                "[tcmgo_sancoes] JSF pagina %d: %d linhas (total=%d)",
                page_idx, len(rows), len(all_rows),
            )
        else:
            logger.warning(
                "[tcmgo_sancoes] JSF atingiu cap de %d paginas — "
                "lista pode estar truncada; revisar _JSF_MAX_PAGES.",
                _JSF_MAX_PAGES,
            )
    finally:
        if owns_client:
            client.close()

    if limit is not None:
        all_rows = all_rows[:limit]

    with out_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            "nome", "cpf_cnpj", "data_inicio", "data_fim",
            "orgao", "processo", "situacao",
        ])
        for r in all_rows:
            writer.writerow([
                r["nome"], r["cpf_cnpj"], r["data_inicio"], r["data_fim"],
                r["orgao"], r["processo"], r["situacao"],
            ])

    logger.info(
        "[tcmgo_sancoes] JSF scraper concluido: %d linhas -> %s",
        len(all_rows), out_csv,
    )
    return out_csv


class TcmgoSancoesPipeline(Pipeline):
    """Scaffold pipeline for TCM-GO impedidos and rejected accounts."""

    name = "tcmgo_sancoes"
    source_id = "tcmgo_sancoes"

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
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_impedidos: pd.DataFrame = pd.DataFrame()
        self._raw_rejeitados: pd.DataFrame = pd.DataFrame()
        # Impedidos-de-licitar (widget JSF) — lista distinta das contas
        # irregulares (CSV REST). Arquivo em ``impedidos_licitar.csv``; shape
        # bate com o schema gravado por :func:`fetch_impedidos_jsf`.
        self._raw_impedidos_jsf: pd.DataFrame = pd.DataFrame()

        self.impedidos: list[dict[str, Any]] = []
        self.rejected_accounts: list[dict[str, Any]] = []
        self.impedido_rels: list[dict[str, Any]] = []
        # Opt-out switch pro fetch online do CSV de contas-irregulares. Testes
        # offline (fixtures CSV locais, sem mock de HTTP) desativam via
        # ``archive_online=False`` pra não hit network. Produção e testes com
        # ``MockTransport`` deixam ``True`` (default) — o CSV baixado é
        # persistido content-addressed via :func:`archive_fetch`, e a URI
        # resultante é carimbada em cada row derivada do endpoint.
        self._archive_online_enabled = archive_online
        # URI do snapshot do CSV de contas-irregulares (único endpoint
        # público do TCM-GO consumido pelo pipeline). Populada pelo fetch
        # online em ``extract`` e consumida em ``transform`` pra carimbar
        # ``source_snapshot_uri`` em cada impedido/rel. ``None`` no caminho
        # offline (fixture local sem HTTP) — preserva o contrato opt-in de
        # ``attach_provenance``. ``rejeitados.csv`` não tem fonte pública
        # correspondente, então permanece sem snapshot.
        self._impedidos_snapshot_uri: str | None = None

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
            logger.warning("[tcmgo_sancoes] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def _archive_contas_online(self) -> str | None:
        """Baixa e arquiva o CSV de contas-irregulares (TCM-GO, online).

        Retorna a URI relativa devolvida por
        :func:`bracc_etl.archival.archive_fetch` ou ``None`` em caso de falha
        de HTTP. Falhas são logadas e engolidas: o pipeline continua com o
        CSV offline mesmo se o endpoint do TCM-GO estiver fora do ar — nessa
        situação as rows simplesmente não ganham ``source_snapshot_uri``
        (opt-in preservado).

        Única fonte pública do pipeline é o CSV de contas-irregulares.
        ``rejeitados.csv`` depende de export manual via LAI e não tem URL
        estável pra arquivar — por isso fica fora.
        """
        try:
            with httpx.Client(
                timeout=_HTTP_TIMEOUT,
                headers={
                    "User-Agent": _USER_AGENT,
                    "Accept": "text/csv,*/*",
                },
                follow_redirects=True,
            ) as client:
                resp = client.get(CONTAS_IRREGULARES_URL)
                resp.raise_for_status()
                content = resp.content
                content_type = resp.headers.get(
                    "content-type", _CONTAS_CONTENT_TYPE,
                )
        except httpx.HTTPError as exc:
            logger.warning(
                "[tcmgo_sancoes] online archival falhou (%s): %s",
                CONTAS_IRREGULARES_URL, exc,
            )
            return None

        uri = archive_fetch(
            url=CONTAS_IRREGULARES_URL,
            content=content,
            content_type=content_type,
            run_id=self.run_id,
            source_id=self.source_id,
        )
        logger.info(
            "[tcmgo_sancoes] archived contas-irregulares -> %s (%d bytes)",
            uri, len(content),
        )
        return uri

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "tcmgo_sancoes"
        if not src_dir.exists():
            logger.warning(
                "[tcmgo_sancoes] expected directory %s missing; "
                "export TCM-GO portal CSVs there.",
                src_dir,
            )
            return
        self._raw_impedidos = self._read_csv_optional(src_dir / "impedidos.csv")
        self._raw_rejeitados = self._read_csv_optional(src_dir / "rejeitados.csv")
        # Impedidos-de-licitar (JSF scraper). Optional — pipeline nao quebra
        # se o CSV nao foi gerado; aceita tanto o novo nome quanto fallback
        # pra compat retroativa caso operador grave com outro rotulo.
        self._raw_impedidos_jsf = self._read_csv_optional(
            src_dir / _IMPEDIDOS_CSV_NAME,
        )

        if self.limit:
            self._raw_impedidos = self._raw_impedidos.head(self.limit)
            self._raw_rejeitados = self._raw_rejeitados.head(self.limit)
            self._raw_impedidos_jsf = self._raw_impedidos_jsf.head(self.limit)

        self.rows_in = (
            len(self._raw_impedidos)
            + len(self._raw_rejeitados)
            + len(self._raw_impedidos_jsf)
        )

        # Online archival do CSV de contas-irregulares. Rodando produção com
        # ``archive_online=True`` (default), baixa do endpoint público do
        # TCM-GO e grava snapshot content-addressed via :func:`archive_fetch`.
        # Falhas de HTTP são absorvidas — se o ws.tcm.go.gov.br estiver fora,
        # ``self._impedidos_snapshot_uri`` fica ``None`` e rows não ganham
        # ``source_snapshot_uri`` (opt-in preservado). Testes offline passam
        # ``archive_online=False`` pra evitar network.
        if self._archive_online_enabled:
            self._impedidos_snapshot_uri = self._archive_contas_online()

    def transform(self) -> None:
        # Fonte 1: CSV de contas-irregulares (REST ws.tcm.go.gov.br). Estas
        # linhas ficam carimbadas com ``list_kind='contas_irregulares'``
        # — apesar do arquivo legado chamar ``impedidos.csv``, o dado e
        # semanticamente "conta com parecer previo/irregular", NAO
        # "impedido de licitar". Mantemos o nome do label TcmGoImpedido pra
        # nao quebrar compat com deploys existentes; o ``list_kind``
        # desambigua na query de pesquisa.
        self._transform_impedidos_csv(
            self._raw_impedidos, list_kind="contas_irregulares",
        )
        # Fonte 2: scraper JSF (widget impedimento.jsf) — impedidos-de-licitar
        # real. Schema diferente do CSV REST (colunas orgao/situacao), mas
        # ambos fazem MERGE no mesmo :TcmGoImpedido + IMPEDIDO_TCMGO.
        if not self._raw_impedidos_jsf.empty:
            self._transform_impedidos_jsf(self._raw_impedidos_jsf)

        for _, row in self._raw_rejeitados.iterrows():
            municipio = normalize_name(
                row_pick(row, "municipio", "ente", "nome_ente"),
            )
            cod_ibge = row_pick(row, "cod_ibge", "codigo_ibge", "ibge")
            exercicio = row_pick(row, "exercicio", "ano", "ano_exercicio")
            processo = row_pick(row, "processo", "nr_processo")
            parecer = row_pick(row, "parecer", "julgamento", "decisao")
            relator = normalize_name(row_pick(row, "relator", "conselheiro"))
            if not municipio and not processo:
                continue
            record_id = _hash_id(cod_ibge, municipio, exercicio, processo)
            account_record_id = f"{cod_ibge}|{exercicio}|{processo}"
            self.rejected_accounts.append(self.attach_provenance(
                {
                    "account_id": record_id,
                    "cod_ibge": cod_ibge,
                    "municipality": municipio,
                    "exercicio": exercicio,
                    "processo": processo,
                    "parecer": parecer,
                    "relator": relator,
                    "uf": "GO",
                    "source": "tcmgo_sancoes",
                },
                record_id=account_record_id,
            ))

        self.impedidos = deduplicate_rows(self.impedidos, ["impedido_id"])
        self.rejected_accounts = deduplicate_rows(
            self.rejected_accounts, ["account_id"],
        )
        self.impedido_rels = deduplicate_rows(
            self.impedido_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = len(self.impedidos) + len(self.rejected_accounts)

    def _transform_impedidos_csv(
        self,
        df: pd.DataFrame,
        *,
        list_kind: str,
    ) -> None:
        """Transforma o CSV de contas-irregulares (REST) em :TcmGoImpedido.

        Extraido de :meth:`transform` pra permitir compartilhar a logica
        com o scraper JSF (:meth:`_transform_impedidos_jsf`) sem duplicar
        hashing, mask, dedupe rules.
        """
        for _, row in df.iterrows():
            doc_raw = row_pick(row, "cpf_cnpj", "documento", "cnpj", "cpf")
            doc_digits = strip_document(doc_raw)
            name = normalize_name(
                row_pick(row, "nome", "razao_social", "responsavel"),
            )
            motivo = normalize_name(
                row_pick(row, "motivo", "fundamento", "decisao"),
            )
            processo = row_pick(row, "processo", "nr_processo")
            inicio = row_pick(row, "data_inicio", "inicio_impedimento", "dt_inicio")
            fim = row_pick(row, "data_fim", "fim_impedimento", "dt_fim")
            if not doc_digits and not name:
                continue
            record_id = _hash_id(doc_digits, name, processo, inicio)
            doc_kind, doc_fmt = "", ""
            if len(doc_digits) == 14:
                doc_kind = "CNPJ"
                doc_fmt = format_cnpj(doc_raw)
            elif len(doc_digits) == 11:
                doc_kind = "CPF"
                doc_fmt = mask_cpf(doc_raw)
            elif _is_premasked_cpf(doc_raw):
                # Upstream TCM-GO ships CPFs pre-masked (``76***.***-***``);
                # strip_document() devolve 2 digitos, entao cai aqui. Nao
                # tem como unmask — preserva a mascara vinda da origem e
                # carimba kind=CPF pra validation queries acharem o row.
                doc_kind = "CPF"
                doc_fmt = doc_raw.strip()
            impedido_record_id = f"{doc_fmt}|{processo}"
            # Todos os impedidos derivam do mesmo CSV de contas-irregulares
            # — então compartilham a mesma URI de snapshot (quando o fetch
            # online rodou). ``None`` preserva o opt-in de attach_provenance.
            snapshot_uri = self._impedidos_snapshot_uri
            self.impedidos.append(self.attach_provenance(
                {
                    "impedido_id": record_id,
                    "document": doc_fmt,
                    "document_kind": doc_kind,
                    "name": name,
                    "motivo": motivo,
                    "processo": processo,
                    "data_inicio": parse_date(inicio) if inicio else "",
                    "data_fim": parse_date(fim) if fim else "",
                    "uf": "GO",
                    "source": "tcmgo_sancoes",
                    "list_kind": list_kind,
                },
                record_id=impedido_record_id,
                snapshot_uri=snapshot_uri,
            ))
            if doc_kind == "CNPJ":
                self.impedido_rels.append(self.attach_provenance(
                    {
                        "source_key": doc_fmt,
                        "target_key": record_id,
                        "list_kind": list_kind,
                    },
                    record_id=impedido_record_id,
                    snapshot_uri=snapshot_uri,
                ))

    def _transform_impedidos_jsf(self, df: pd.DataFrame) -> None:
        """Transforma o CSV do widget JSF em :TcmGoImpedido (impedidos-de-licitar).

        Shape do CSV (gerado por :func:`fetch_impedidos_jsf`):
          nome | cpf_cnpj | data_inicio | data_fim | orgao | processo | situacao

        Diferenca do CSV REST (contas-irregulares):
          - ``orgao`` + ``situacao`` substituem ``motivo`` semantico.
          - Documentos costumam vir plenos (CPF com pontuacao, CNPJ com mascara
            padrao) porque o widget apresenta o que o operador enxerga no
            portal. mask_cpf e format_cnpj normalizam.
          - list_kind='impedidos_licitar' (diferenciando da contas_irregulares
            na query da API).

        Nao tem fonte arquivada por linha (o scraper JSF nao roda archival
        por-pagina) — snapshot_uri fica None aqui.
        """
        for _, row in df.iterrows():
            doc_raw = row_pick(row, "cpf_cnpj", "documento", "cnpj", "cpf")
            doc_digits = strip_document(doc_raw)
            name = normalize_name(
                row_pick(row, "nome", "razao_social", "responsavel"),
            )
            orgao = normalize_name(
                row_pick(row, "orgao", "orgao_julgador", "ente"),
            )
            situacao = normalize_name(
                row_pick(row, "situacao", "status"),
            )
            # ``motivo`` da JSF e a concatenacao orgao+situacao (nao ha campo
            # dedicado); assim a query de perfil ainda tem 1 string semantica
            # curta sem precisar esquema novo.
            motivo = f"{situacao} — {orgao}" if orgao else situacao
            processo = row_pick(row, "processo", "nr_processo")
            inicio = row_pick(row, "data_inicio", "inicio")
            fim = row_pick(row, "data_fim", "termino", "fim")
            if not doc_digits and not name:
                continue
            record_id = _hash_id(
                doc_digits, name, processo, inicio, "jsf",
            )
            doc_kind, doc_fmt = "", ""
            if len(doc_digits) == 14:
                doc_kind = "CNPJ"
                doc_fmt = format_cnpj(doc_raw)
            elif len(doc_digits) == 11:
                doc_kind = "CPF"
                doc_fmt = mask_cpf(doc_raw)
            elif _is_premasked_cpf(doc_raw):
                doc_kind = "CPF"
                doc_fmt = doc_raw.strip()
            impedido_record_id = f"{doc_fmt}|{processo}|jsf"
            self.impedidos.append(self.attach_provenance(
                {
                    "impedido_id": record_id,
                    "document": doc_fmt,
                    "document_kind": doc_kind,
                    "name": name,
                    "motivo": motivo,
                    "orgao": orgao,
                    "situacao": situacao,
                    "processo": processo,
                    "data_inicio": parse_date(inicio) if inicio else "",
                    "data_fim": parse_date(fim) if fim else "",
                    "uf": "GO",
                    "source": "tcmgo_sancoes",
                    "list_kind": "impedidos_licitar",
                },
                record_id=impedido_record_id,
            ))
            if doc_kind == "CNPJ":
                self.impedido_rels.append(self.attach_provenance(
                    {
                        "source_key": doc_fmt,
                        "target_key": record_id,
                        "list_kind": "impedidos_licitar",
                    },
                    record_id=impedido_record_id,
                ))

    def load(self) -> None:
        if not (self.impedidos or self.rejected_accounts):
            logger.warning("[tcmgo_sancoes] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)
        if self.impedidos:
            loader.load_nodes(
                "TcmGoImpedido", self.impedidos, key_field="impedido_id",
            )
            # Company nodes derived from impedidos need provenance too. The
            # raw CNPJ digits are the natural record_id (deep-link is the
            # registry primary_url; no per-record URL available here).
            # Snapshot URI é o mesmo do impedido que originou cada Company
            # (todos saem do mesmo CSV de contas-irregulares). Preservar a
            # URI por-row permite que o retrofit seja verificado ponta a
            # ponta mesmo quando o pipeline escrita pra Company.
            companies = deduplicate_rows(
                [
                    self.attach_provenance(
                        {"cnpj": r["document"], "razao_social": r["name"]},
                        record_id=strip_document(str(r["document"])),
                        snapshot_uri=r.get("source_snapshot_uri"),
                    )
                    for r in self.impedidos
                    if r["document_kind"] == "CNPJ"
                ],
                ["cnpj"],
            )
            if companies:
                loader.load_nodes("Company", companies, key_field="cnpj")
        if self.rejected_accounts:
            loader.load_nodes(
                "TcmGoRejectedAccount",
                self.rejected_accounts,
                key_field="account_id",
            )
        if self.impedido_rels:
            loader.load_relationships(
                rel_type="IMPEDIDO_TCMGO",
                rows=self.impedido_rels,
                source_label="Company",
                source_key="cnpj",
                target_label="TcmGoImpedido",
                target_key="impedido_id",
                properties=["list_kind"],
            )

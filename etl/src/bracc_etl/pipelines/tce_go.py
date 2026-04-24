"""ETL pipeline scaffold for TCE Goias (Tribunal de Contas do Estado de Goias).

TCE-GO publishes several operational dashboards (fiscalizacoes em andamento,
contas irregulares, decisoes, diario eletronico) at https://portal.tce.go.gov.br/
but does not currently expose a public JSON API or bulk CSV export. This
pipeline follows the repo convention of accepting pre-downloaded CSV files
placed under ``data/tce_go/``:

- ``decisoes.csv``    -> TceGoDecision nodes
- ``irregulares.csv`` -> TceGoIrregularAccount nodes + IMPEDIDO_TCE_GO rels
- ``fiscalizacoes.csv`` -> TceGoAudit nodes

Human validation required before production use:

1. Confirm the CSV schema exported from each TCE-GO dashboard.
2. Verify legal terms of use for each dataset (some portals require a data
   request form rather than open redistribution).
3. Decide whether to fetch via scraping (respecting robots.txt) or to rely
   exclusively on operator-provided exports.

Data source: https://portal.tce.go.gov.br/

Archival (retrofit #5 do plano em
``todo-list-prompts/high_priority/11-archival-retrofit-go.md``):

TCE-GO é o único caso especial dos 10 pipelines GO — não expõe endpoint
público, então todo o fluxo de ingestão é **operator-fed** (CSVs colocados
manualmente sob ``data/tce_go/`` por quem exportou os dashboards). Como o
prompt do retrofit prevê: "pipeline sem fluxo online roda archival opt-out
por default". Ligando ``archive_local=True``, a camada lê os bytes do CSV
local, chama :func:`bracc_etl.archival.archive_fetch` com
``url="file://<abs>"``, e a URI content-addressed devolvida é carimbada em
cada row derivada daquele arquivo. Resultado: mesmo que o operador delete
os CSVs de ``data/``, a cópia imutável usada na ingestão sobrevive sob
``BRACC_ARCHIVAL_ROOT/tce_go/YYYY-MM/*.csv`` — satisfazendo o requisito de
proveniência rastreável do Fiscal Cidadão para uma fonte que só existe
como export manual.
"""

from __future__ import annotations

import csv
import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    parse_date,
    row_pick,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# Fallback content-type pros CSVs locais do TCE-GO (archival é content-
# addressed, então o único efeito é a extensão: ``.csv`` vs ``.bin``).
_CSV_CONTENT_TYPE = "text/csv"

# TCE-GO publica o acervo de decisões (acórdãos, despachos, resoluções) via
# API de busca JSON não documentada em ``iago-search-api.tce.go.gov.br``. O
# endpoint ``/decisions/search`` é open (sem auth) e retorna Spring Page.
# Cap do Elasticsearch backend: 10k rows totais (window). Page size máximo
# observado: 2000 — usamos 1000 (~15s pra puxar 10k em 10 requests).
DECISIONS_SEARCH_URL = "https://iago-search-api.tce.go.gov.br/decisions/search"
_DEFAULT_PAGE_SIZE = 1000
_DEFAULT_RATE_LIMIT_S = 0.2  # cortesia entre páginas
_HTTP_TIMEOUT = 60.0
_USER_AGENT = "br-acc-etl/1.0 (+https://github.com/brunoclz/br-acc)"

# Map do JSON da iago-search-api → colunas que ``_transform_decisions``
# aceita via ``row_pick``. Campos extras (``id``, ``process``, ``subject``,
# ``confidential``) ficam no CSV mas não têm alias mapeado — preservam
# contexto pra análises futuras sem quebrar o pipeline atual.
_DECISION_FIELD_ORDER = [
    "numero",       # ex.: "00837/2026"
    "tipo",         # ex.: "Acórdão"
    "data",         # ex.: "13/04/2026 11:00"
    "orgao",        # ex.: "Tribunal Pleno"
    "ementa",       # texto longo
    "relator",      # ex.: "CARLA CINTIA SANTILLO"
    "id",           # id interno TCE
    "processo",     # ex.: "202600047000261"
    "assunto",      # ex.: "004 - 47 - ATOS DE PESSOAL"
    "interessados", # partes interessadas
    "ano",
    "confidencial", # "true"/"false"
    "sumula",       # summary (texto curto)
]


def _hash_id(*parts: str, length: int = 20) -> str:
    raw = ":".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _decision_to_row(d: dict[str, Any]) -> dict[str, str]:
    """Map one iago-search-api decision dict into the CSV row shape.

    Preserva ``numero`` no formato ``<number>/<year>`` (ex.: ``00837/2026``) pra
    bater com o display que o TCE usa no título ("Acórdão 00837/2026").
    """
    number = str(d.get("number") or "").strip()
    year = str(d.get("year") or "").strip()
    numero = f"{number}/{year}" if number and year else (number or year)
    rapporteur = str(
        d.get("decision_rapporteur") or d.get("rapporteur") or "",
    ).strip()
    # Upstream envia ``13/04/2026 11:00`` (data + hora). O parse_date do repo
    # rejeita esse shape e devolve "" — split em whitespace preserva so a
    # parte DD/MM/YYYY que parse_date consome limpo.
    raw_date = str(d.get("date") or "").strip()
    data_only = raw_date.split(" ", 1)[0] if raw_date else ""
    return {
        "numero": numero,
        "tipo": str(d.get("type") or "").strip(),
        "data": data_only,
        "orgao": str(d.get("collegiate") or "").strip(),
        "ementa": str(d.get("ementa") or "").strip(),
        "relator": rapporteur,
        "id": str(d.get("id") or "").strip(),
        "processo": str(d.get("process") or "").strip(),
        "assunto": str(d.get("subject") or "").strip(),
        "interessados": str(d.get("interested") or "").strip(),
        "ano": year,
        "confidencial": "true" if d.get("confidential") else "false",
        "sumula": str(d.get("summary") or "").strip(),
    }


def _iter_decisions(
    client: httpx.Client,
    url: str = DECISIONS_SEARCH_URL,
    page_size: int = _DEFAULT_PAGE_SIZE,
    limit: int | None = None,
) -> Any:
    """Yield decision dicts from iago-search-api, paginando até acabar.

    Respeita o cap do backend (10k rows totais; page size >= 2000 devolve
    no máximo 2000). ``limit`` corta cedo pra smoke tests.
    """
    yielded = 0
    page = 0
    while True:
        resp = client.post(
            url,
            json={"term": ""},
            params={"page": page, "size": page_size},
            headers={
                "Accept": "application/json",
                "User-Agent": _USER_AGENT,
            },
        )
        resp.raise_for_status()
        body = resp.json()
        content = body.get("content") or []
        for d in content:
            yield d
            yielded += 1
            if limit is not None and yielded >= limit:
                return
        if body.get("last") or not content:
            return
        page += 1


def fetch_to_disk(
    output_dir: Path | str,
    limit: int | None = None,
    url: str = DECISIONS_SEARCH_URL,
    page_size: int = _DEFAULT_PAGE_SIZE,
    timeout: float = _HTTP_TIMEOUT,
    client: httpx.Client | None = None,
) -> list[Path]:
    """Download TCE-GO decisoes via iago-search-api and stage as CSV.

    Writes ``decisoes.csv`` under ``output_dir`` com as colunas aliases que
    :meth:`TceGoPipeline._transform_decisions` já aceita (``numero``,
    ``tipo``, ``data``, ``orgao``, ``ementa``, ``relator``) mais campos de
    contexto (``id``, ``processo``, ``assunto``, ``interessados``, ``ano``,
    ``confidencial``, ``sumula``) pra downstream.

    Args:
        output_dir: diretório destino. Criado se ausente.
        limit: cap opcional de rows (header sempre preservado). Util pra
            smoke tests.
        url: override do endpoint de busca (default
            ``iago-search-api.tce.go.gov.br/decisions/search``).
        page_size: tamanho da página por request (max efetivo: 2000).
        timeout: timeout HTTP em segundos.
        client: httpx.Client pré-configurado (pra testes mockados). Quando
            ``None``, cria um client próprio com timeout + User-Agent
            padrão.

    Returns:
        Lista de Path escritos.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = output_dir / "decisoes.csv"

    logger.info(
        "[tce_go] fetching decisoes from %s (limit=%s, page_size=%d) -> %s",
        url, limit, page_size, out_csv,
    )

    owns_client = client is None
    if owns_client:
        client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        )
    assert client is not None  # narrowed by both branches above
    try:
        rows_written = 0
        with out_csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=_DECISION_FIELD_ORDER,
                delimiter=";", quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()
            for decision in _iter_decisions(
                client, url=url, page_size=page_size, limit=limit,
            ):
                writer.writerow(_decision_to_row(decision))
                rows_written += 1
    finally:
        if owns_client:
            client.close()

    logger.info(
        "[tce_go] wrote %s (%d rows)", out_csv, rows_written,
    )
    return [out_csv]


class TceGoPipeline(Pipeline):
    """Scaffold pipeline for TCE Goias audit data.

    Reads pre-downloaded CSV files under ``data/tce_go/``. No remote API
    fallback because TCE-GO has not published a documented open data
    endpoint at time of writing.
    """

    name = "tce_go"
    source_id = "tce_go"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        *,
        archive_local: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw_decisions: pd.DataFrame = pd.DataFrame()
        self._raw_irregular: pd.DataFrame = pd.DataFrame()
        self._raw_audits: pd.DataFrame = pd.DataFrame()

        self.decisions: list[dict[str, Any]] = []
        self.irregular_accounts: list[dict[str, Any]] = []
        self.audits: list[dict[str, Any]] = []
        self.impedido_rels: list[dict[str, Any]] = []

        # Archival dos CSVs locais. TCE-GO não tem endpoint público, então
        # o único "fetch" é o operador-drop em ``data/tce_go/*.csv``.
        # ``archive_local=True`` liga a preservação content-addressed dos
        # bytes desses arquivos via :func:`bracc_etl.archival.archive_fetch`.
        # Default ``False`` segue a diretriz do prompt de retrofit #11
        # ("pipelines sem fluxo online roda archival opt-out por default")
        # e mantém os testes legados verdes — ``source_snapshot_uri`` é
        # opt-in em ``attach_provenance``, logo rows ficam sem a chave.
        self._archive_local_enabled = archive_local
        # URIs carimbadas em cada domínio. Três CSVs = três snapshots
        # distintos (content-addressed, então dedup acontece no archival
        # mesmo quando dois arquivos carregam o mesmo conteúdo).
        # ``None`` no caminho opt-out ou quando o arquivo não existe.
        self._decisions_snapshot_uri: str | None = None
        self._irregular_snapshot_uri: str | None = None
        self._audits_snapshot_uri: str | None = None

    # ------------------------------------------------------------------
    # Extract
    # ------------------------------------------------------------------

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
            logger.warning("[tce_go] failed to read %s: %s", path, exc)
            return pd.DataFrame()

    def _archive_local_csv(self, path: Path) -> str | None:
        """Arquiva os bytes crus de um CSV operator-fed via ``archive_fetch``.

        TCE-GO não tem fonte HTTP pública, então o "fetch" conceitual é o
        operador jogando o arquivo em ``data/tce_go/``. Preservar esses
        bytes content-addressed garante que, se o CSV for deletado ou
        sobrescrito, a cópia usada na ingestão ainda exista sob o root de
        archival — requisito de proveniência rastreável do Fiscal Cidadão.

        Falhas de I/O são absorvidas (log + ``None``): o pipeline continua
        carregando a partir do DataFrame já parseado; rows apenas não
        ganham ``source_snapshot_uri`` (opt-in preservado).
        """
        try:
            content = path.read_bytes()
        except OSError as exc:
            logger.warning(
                "[tce_go] failed to read %s for archival: %s", path, exc,
            )
            return None
        try:
            uri = archive_fetch(
                url=f"file://{path.resolve()}",
                content=content,
                content_type=_CSV_CONTENT_TYPE,
                run_id=self.run_id,
                source_id=self.source_id,
            )
        except (OSError, ValueError) as exc:
            logger.warning(
                "[tce_go] archive_fetch falhou para %s: %s", path, exc,
            )
            return None
        logger.info(
            "[tce_go] archived %s -> %s (%d bytes)",
            path.name, uri, len(content),
        )
        return uri

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "tce_go"
        if not src_dir.exists():
            logger.warning(
                "[tce_go] expected directory %s is missing; "
                "export TCE-GO dashboards to CSV there.",
                src_dir,
            )
            return

        decisions_path = src_dir / "decisoes.csv"
        irregular_path = src_dir / "irregulares.csv"
        audits_path = src_dir / "fiscalizacoes.csv"

        self._raw_decisions = self._read_csv_optional(decisions_path)
        self._raw_irregular = self._read_csv_optional(irregular_path)
        self._raw_audits = self._read_csv_optional(audits_path)

        # Archival do CSV operator-fed. Rodando com ``archive_local=True``
        # (opt-in), grava snapshot content-addressed via ``archive_fetch``
        # de cada CSV presente. O path offline default (``False``) mantém
        # o contrato opt-in de ``attach_provenance`` — rows continuam sem
        # ``source_snapshot_uri``, preservando os testes legados.
        if self._archive_local_enabled:
            if decisions_path.exists() and decisions_path.stat().st_size > 0:
                self._decisions_snapshot_uri = self._archive_local_csv(decisions_path)
            if irregular_path.exists() and irregular_path.stat().st_size > 0:
                self._irregular_snapshot_uri = self._archive_local_csv(irregular_path)
            if audits_path.exists() and audits_path.stat().st_size > 0:
                self._audits_snapshot_uri = self._archive_local_csv(audits_path)

        if self.limit:
            self._raw_decisions = self._raw_decisions.head(self.limit)
            self._raw_irregular = self._raw_irregular.head(self.limit)
            self._raw_audits = self._raw_audits.head(self.limit)

        self.rows_in = (
            len(self._raw_decisions)
            + len(self._raw_irregular)
            + len(self._raw_audits)
        )
        logger.info(
            "[tce_go] extracted decisions=%d irregular=%d audits=%d",
            len(self._raw_decisions),
            len(self._raw_irregular),
            len(self._raw_audits),
        )

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        self._transform_decisions()
        self._transform_irregular()
        self._transform_audits()

        self.decisions = deduplicate_rows(self.decisions, ["decision_id"])
        self.irregular_accounts = deduplicate_rows(
            self.irregular_accounts, ["account_id"],
        )
        self.audits = deduplicate_rows(self.audits, ["audit_id"])
        self.impedido_rels = deduplicate_rows(
            self.impedido_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = (
            len(self.decisions)
            + len(self.irregular_accounts)
            + len(self.audits)
        )

    def _transform_decisions(self) -> None:
        for _, row in self._raw_decisions.iterrows():
            numero = row_pick(row, "numero", "nr_processo", "acordao", "decisao").strip()
            tipo = row_pick(row, "tipo", "tipo_decisao", "modalidade")
            data = row_pick(row, "data", "dt_publicacao", "data_decisao")
            orgao = normalize_name(row_pick(row, "orgao", "unidade"))
            ementa = normalize_name(row_pick(row, "ementa", "resumo", "descricao"))
            relator = normalize_name(row_pick(row, "relator", "conselheiro"))
            if not numero and not ementa:
                continue
            decision_id = _hash_id(numero, data, tipo)
            decision_record_id = f"{numero}|{data}"
            self.decisions.append(self.attach_provenance(
                {
                    "decision_id": decision_id,
                    "numero": numero,
                    "tipo": tipo,
                    "orgao": orgao,
                    "relator": relator,
                    "ementa": ementa,
                    "published_at": parse_date(data) if data else "",
                    "uf": "GO",
                    "source": "tce_go",
                },
                record_id=decision_record_id,
                snapshot_uri=self._decisions_snapshot_uri,
            ))

    def _transform_irregular(self) -> None:
        for _, row in self._raw_irregular.iterrows():
            cnpj_raw = row_pick(row, "cnpj", "cpf_cnpj", "documento")
            cnpj_digits = strip_document(cnpj_raw)
            name = normalize_name(
                row_pick(row, "nome", "razao_social", "responsavel"),
            )
            processo = row_pick(row, "processo", "nr_processo")
            julgamento = row_pick(row, "julgamento", "data_julgamento", "data")
            motivo = normalize_name(row_pick(row, "motivo", "fundamento", "decisao"))
            if not cnpj_digits and not name:
                continue
            account_id = _hash_id(cnpj_digits, name, processo, julgamento)
            cnpj_fmt = format_cnpj(cnpj_raw) if len(cnpj_digits) == 14 else ""
            # Use the TCE-GO processo number as the natural record_id when
            # present; fall back to the composite of source fields when
            # the row lacks a numbered process (rare).
            account_record_id = (
                str(processo)
                if processo
                else f"{cnpj_digits}|{name}|{julgamento}"
            )
            self.irregular_accounts.append(self.attach_provenance(
                {
                    "account_id": account_id,
                    "cnpj": cnpj_fmt,
                    "name": name,
                    "processo": processo,
                    "motivo": motivo,
                    "julgamento": parse_date(julgamento) if julgamento else "",
                    "uf": "GO",
                    "source": "tce_go",
                },
                record_id=account_record_id,
                snapshot_uri=self._irregular_snapshot_uri,
            ))
            if cnpj_fmt:
                self.impedido_rels.append(self.attach_provenance(
                    {
                        "source_key": cnpj_fmt,
                        "target_key": account_id,
                    },
                    record_id=account_record_id,
                    snapshot_uri=self._irregular_snapshot_uri,
                ))

    def _transform_audits(self) -> None:
        for _, row in self._raw_audits.iterrows():
            numero = row_pick(row, "numero", "nr_processo", "processo").strip()
            titulo = normalize_name(row_pick(row, "titulo", "objeto", "descricao"))
            orgao = normalize_name(row_pick(row, "orgao", "unidade", "jurisdicionado"))
            status = row_pick(row, "status", "situacao", "fase")
            inicio = row_pick(row, "data_inicio", "dt_inicio", "inicio")
            if not numero and not titulo:
                continue
            audit_id = _hash_id(numero, titulo, inicio)
            audit_record_id = (
                str(numero) if numero else f"{titulo}|{inicio}"
            )
            self.audits.append(self.attach_provenance(
                {
                    "audit_id": audit_id,
                    "numero": numero,
                    "titulo": titulo,
                    "orgao": orgao,
                    "status": status,
                    "data_inicio": parse_date(inicio) if inicio else "",
                    "uf": "GO",
                    "source": "tce_go",
                },
                record_id=audit_record_id,
                snapshot_uri=self._audits_snapshot_uri,
            ))

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not (self.decisions or self.irregular_accounts or self.audits):
            logger.warning("[tce_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)

        if self.decisions:
            loader.load_nodes(
                "TceGoDecision", self.decisions, key_field="decision_id",
            )

        if self.irregular_accounts:
            loader.load_nodes(
                "TceGoIrregularAccount",
                self.irregular_accounts,
                key_field="account_id",
            )
            # Company nodes carry the raw CNPJ digits as record_id (natural
            # key for the cross-source Company entity). A URI de snapshot
            # vem do ``irregular_accounts`` de origem: toda Company derivada
            # neste pipeline sai do mesmo ``irregulares.csv`` arquivado,
            # então carimbar a URI no row de Company mantém a cadeia de
            # proveniência consistente (``None`` quando archive_local=False).
            companies = deduplicate_rows(
                [
                    self.attach_provenance(
                        {"cnpj": r["cnpj"], "razao_social": r["name"]},
                        record_id=strip_document(str(r["cnpj"])),
                        snapshot_uri=r.get("source_snapshot_uri"),
                    )
                    for r in self.irregular_accounts
                    if r["cnpj"]
                ],
                ["cnpj"],
            )
            if companies:
                loader.load_nodes("Company", companies, key_field="cnpj")

        if self.audits:
            loader.load_nodes("TceGoAudit", self.audits, key_field="audit_id")

        if self.impedido_rels:
            loader.load_relationships(
                rel_type="IMPEDIDO_TCE_GO",
                rows=self.impedido_rels,
                source_label="Company",
                source_key="cnpj",
                target_label="TceGoIrregularAccount",
                target_key="account_id",
            )

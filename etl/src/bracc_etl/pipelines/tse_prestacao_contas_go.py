"""TSE Prestação de Contas — Goiás scope (script_download / archival).

Fecha o gap ``validacao_tse`` do Flask (``backend/app.py::gerar_validacao_tse``),
que hoje lê direto do grafo propriedades agregadas de receitas/bens de
candidatos GO sem pipeline que as popule:

* ``total_tse_{YYYY}`` — soma bruta de receitas ingeridas do candidato.
* ``tse_{YYYY}_partido`` — soma de receitas de partido político (fundo
  partidário + FEFC).
* ``tse_{YYYY}_proprios`` — recursos próprios (autofinanciamento).
* ``tse_{YYYY}_pessoa_fisica`` — doações de pessoa física.
* ``tse_{YYYY}_pessoa_juridica`` — doações de pessoa jurídica (para
  eleições com PJ permitida; em 2018+ o STF baniu PJ mas o campo fica
  zerado pra manter contrato estável).
* ``tse_{YYYY}_fin_coletivo`` — financiamento coletivo (vaquinha).
* ``total_despesas_tse_{YYYY}`` — soma bruta de despesas pagas (cross-check
  com teto legal de gastos de campanha — consumido por
  :func:`bracc.services.teto_service.calcular_teto`).
* ``cargo_tse_{YYYY}`` — cargo do candidato na eleição ``{YYYY}`` (ex.:
  ``"DEPUTADO FEDERAL"``). Usado pra mapear teto de gastos sem depender
  de labels de outros pipelines.
* ``patrimonio_declarado`` / ``patrimonio_ano`` — soma VR_BEM_CANDIDATO
  do arquivo ``bens_candidato``.

Fonte (TSE CDN, pública, sem auth):

    ``https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/
    prestacao_de_contas_eleitorais_candidatos_{YYYY}.zip``

Ano default: 2022 (próxima eleição geral: 2026 — pipeline aceita o ano
por ``__init__`` pra permitir re-ingestão futura). O ZIP contém CSVs
separados por estado e por BRASIL. Usamos os três ``*_BRASIL.csv`` e
filtramos ``SG_UF == 'GO'`` em memória (streaming linha a linha via
``csv.DictReader`` pra não estourar RAM com ZIPs ~GB).

### Archival + provenance

Cada ano baixado chama ``archive_fetch`` **uma vez** sobre o ZIP
inteiro — content-addressed, então re-runs batem cache. Todas as rows
derivadas do ano carimbam ``source_snapshot_uri`` apontando pro mesmo
snapshot, preservando a rastreabilidade bruta mesmo se a URL mudar.

``source_id = 'tse_prestacao_contas'`` (distinto dos pipelines TSE
existentes — ``tse``, ``tse_bens``, ``tse_filiados`` — pra permitir
cadências / retenções separadas no registry).

### LGPD

``NR_CPF_CPF_CANDIDATO`` é público (o TSE publica) e vira a chave de
``:Person``. Já ``CPF_DOADOR`` (doador pessoa física) é mascarado via
``mask_cpf`` antes de qualquer write — consistente com o padrão do
ALEGO / Câmara, e alinhado ao audit de LGPD (Seção 7 do
``04-flask-orchestration-map.md``).

### Escopo

**GO only.** Filtro aplicado em ``extract()`` — linhas de outras UFs
são descartadas antes do load, então o grafo só carrega candidatos GO.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from bracc_etl.archival import archive_fetch
from bracc_etl.base import Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    mask_cpf,
    normalize_name,
    parse_date,
    parse_numeric_comma,
    strip_document,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from neo4j import Driver

logger = logging.getLogger(__name__)

# CDN do TSE — mesma base usada pelos pipelines ``tse`` e ``tse_bens``.
_TSE_CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas"
_ZIP_CONTENT_TYPE = "application/zip"

_DEFAULT_YEAR = 2022
_DEFAULT_UF = "GO"
_HTTP_TIMEOUT = 600.0

_SOURCE_ID = "tse_prestacao_contas"

# Mapeamento de DS_ORIGEM_RECEITA / DS_FONTE_RECEITA para o bucket
# usado em ``gerar_validacao_tse``. As strings são lowercased +
# ASCII-normalizadas antes da comparação (a TSE mistura acentos).
_BUCKET_PARTIDO = "partido"
_BUCKET_PROPRIOS = "proprios"
_BUCKET_PESSOA_FISICA = "pessoa_fisica"
_BUCKET_PESSOA_JURIDICA = "pessoa_juridica"
_BUCKET_FIN_COLETIVO = "fin_coletivo"
_BUCKET_OUTROS = "outros"

# Ordem importa pra match parcial (substring) — mais específicos primeiro.
_ORIGEM_KEYWORDS: tuple[tuple[str, str], ...] = (
    # Financiamento coletivo / vaquinha — tem que vir antes de "recursos"
    # porque a string canônica "Recursos de financiamento coletivo" tem
    # ambos.
    ("financiamento coletivo", _BUCKET_FIN_COLETIVO),
    ("vaquinha", _BUCKET_FIN_COLETIVO),
    # Partido / fundo / FEFC
    ("partido", _BUCKET_PARTIDO),
    ("fundo partidario", _BUCKET_PARTIDO),
    ("fundo especial", _BUCKET_PARTIDO),
    ("fefc", _BUCKET_PARTIDO),
    # Pessoa física / pessoa jurídica
    ("pessoa juridica", _BUCKET_PESSOA_JURIDICA),
    ("pessoa fisica", _BUCKET_PESSOA_FISICA),
    ("pessoas fisicas", _BUCKET_PESSOA_FISICA),
    # Recursos próprios / autofinanciamento
    ("proprios", _BUCKET_PROPRIOS),
    ("proprio candidato", _BUCKET_PROPRIOS),
    ("autofinanciamento", _BUCKET_PROPRIOS),
)


def _strip_accents(s: str) -> str:
    """Lowercase + remove acentos comuns, simples o suficiente pra bucket match."""
    table = str.maketrans(
        "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
        "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC",
    )
    return s.translate(table).lower()


def _classify_origem(raw: str) -> str:
    norm = _strip_accents(raw or "").strip()
    if not norm:
        return _BUCKET_OUTROS
    for kw, bucket in _ORIGEM_KEYWORDS:
        if kw in norm:
            return bucket
    return _BUCKET_OUTROS


def _donation_id(sq_candidato: str, year: int, cpf_cnpj: str, valor: str, seq: int) -> str:
    """Deterministic id para ``:CampaignDonation`` — estável entre re-runs.

    Inclui ``seq`` (índice na CSV row) pra diferenciar múltiplas doações
    idênticas do mesmo doador pro mesmo candidato (TSE permite várias
    linhas idênticas quando há parcelamento).
    """
    payload = f"{sq_candidato}|{year}|{cpf_cnpj}|{valor}|{seq}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _expense_id(sq_candidato: str, year: int, cnpj_cpf: str, valor: str, seq: int) -> str:
    payload = f"exp|{sq_candidato}|{year}|{cnpj_cpf}|{valor}|{seq}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _year_zip_url(year: int) -> str:
    return f"{_TSE_CDN}/prestacao_de_contas_eleitorais_candidatos_{year}.zip"


def _iter_csv_rows(
    zip_bytes: bytes,
    *,
    filename_prefix: str,
    year: int,
) -> Iterator[dict[str, str]]:
    """Yield rows from the ``*_BRASIL.csv`` member of the TSE ZIP.

    TSE publishes CSVs em ``latin-1`` com separador ``;``. Fazemos
    streaming via ``csv.DictReader`` em cima do bytes buffer pra não
    materializar o ZIP gigante em DataFrame na RAM.

    ``filename_prefix`` é o prefixo esperado (ex.: ``receitas_candidatos``).
    Aceita variações de caixa e fallback pra ``*_{year}_{UF}.csv`` se
    não encontrar o ``_BRASIL.csv`` (útil pra fixtures minimais).
    """
    target_patterns = (
        f"{filename_prefix}_{year}_brasil.csv",
        f"{filename_prefix}_{year}_BRASIL.csv",
    )
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Localiza o membro — case-insensitive.
        member_name: str | None = None
        for info in zf.infolist():
            name = Path(info.filename).name
            lower = name.lower()
            if lower in (p.lower() for p in target_patterns):
                member_name = info.filename
                break
        # Fallback: aceita qualquer CSV com o prefixo (ex.: por-UF em
        # fixtures pequenas).
        if member_name is None:
            for info in zf.infolist():
                name = Path(info.filename).name.lower()
                if name.startswith(filename_prefix.lower()) and name.endswith(".csv"):
                    member_name = info.filename
                    break
        if member_name is None:
            logger.warning(
                "[tse_prestacao_contas_go] CSV %s_%d_BRASIL.csv not found in ZIP",
                filename_prefix, year,
            )
            return
        with zf.open(member_name) as fh:
            text = io.TextIOWrapper(fh, encoding="latin-1", newline="")
            reader = csv.DictReader(text, delimiter=";")
            yield from reader


class TsePrestacaoContasGoPipeline(Pipeline):
    """Ingere receitas + despesas + bens de candidatos GO do TSE.

    Parâmetros suportados via ``__init__`` / ``kwargs``:

    * ``year`` — ano da eleição (default 2022). O pipeline é idempotente
      por ano, então rodar com ``year=2026`` depois da próxima eleição
      atualiza os campos ``tse_2026_*`` sem reabrir 2022.
    * ``uf`` — UF a filtrar (default ``GO``). Outras UFs são apenas
      pra teste — o produto fica em GO.
    * ``http_client_factory`` — injeta httpx.Client mockado em testes.
    """

    name = "tse_prestacao_contas_go"
    source_id = _SOURCE_ID

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        year = int(kwargs.pop("year", _DEFAULT_YEAR))
        uf = str(kwargs.pop("uf", _DEFAULT_UF)).upper()
        http_client_factory = kwargs.pop(
            "http_client_factory",
            lambda: httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True),
        )
        super().__init__(
            driver,
            data_dir,
            limit=limit,
            chunk_size=chunk_size,
            **kwargs,
        )
        self.year = year
        self.uf = uf
        self._http_client_factory = http_client_factory

        # Buffers preenchidos em extract():
        self._receitas_raw: list[dict[str, str]] = []
        self._despesas_raw: list[dict[str, str]] = []
        self._despesas_contratadas_raw: list[dict[str, str]] = []
        self._bens_raw: list[dict[str, str]] = []
        # Snapshot URI do ZIP baixado — carimbado em TODO row.
        self._snapshot_uri: str = ""
        self._zip_url: str = _year_zip_url(year)

        # Buffers preenchidos em transform():
        self.persons: list[dict[str, Any]] = []
        self.donations: list[dict[str, Any]] = []
        self.donation_rels: list[dict[str, Any]] = []
        self.expenses: list[dict[str, Any]] = []
        self.expense_rels: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # extract
    # ------------------------------------------------------------------

    def extract(self) -> None:
        """Baixa + arquiva o ZIP do ano, filtra GO streaming das 3 CSVs.

        Também aceita um ZIP pré-fornecido em ``{data_dir}/tse_prestacao_contas/
        prestacao_de_contas_eleitorais_candidatos_{year}.zip`` — útil pra
        fixtures + re-runs offline (o pipeline ``tse`` legado segue o
        mesmo padrão).
        """
        cached_zip = (
            Path(self.data_dir)
            / "tse_prestacao_contas"
            / f"prestacao_de_contas_eleitorais_candidatos_{self.year}.zip"
        )
        zip_bytes: bytes
        if cached_zip.exists():
            logger.info("[tse_prestacao_contas_go] using cached ZIP %s", cached_zip)
            zip_bytes = cached_zip.read_bytes()
        else:
            with self._http_client_factory() as client:
                logger.info("[tse_prestacao_contas_go] downloading %s", self._zip_url)
                resp = client.get(self._zip_url)
                resp.raise_for_status()
                zip_bytes = resp.content

        # Archival do ZIP inteiro — um snapshot por ano, reusado em N rows.
        self._snapshot_uri = archive_fetch(
            url=self._zip_url,
            content=zip_bytes,
            content_type=_ZIP_CONTENT_TYPE,
            run_id=self.run_id,
            source_id=_SOURCE_ID,
        )

        uf = self.uf

        # --- Receitas ---
        for row in _iter_csv_rows(
            zip_bytes, filename_prefix="receitas_candidatos", year=self.year,
        ):
            if (row.get("SG_UF") or row.get("UF_ELEICAO") or "").strip().upper() != uf:
                continue
            self._receitas_raw.append(row)
            if self.limit and len(self._receitas_raw) >= self.limit:
                break

        # --- Despesas (despesas_pagas) ---
        # A CSV ``despesas_pagas_candidatos_{year}_BRASIL.csv`` do TSE 2022+
        # NÃO contém CPF/nome/SQ_CANDIDATO/cargo do candidato nem
        # CPF/CNPJ/nome do fornecedor — apenas ``SQ_PRESTADOR_CONTAS``,
        # ``NR_DOCUMENTO``, ``SQ_DESPESA`` e o valor pago. A resolução
        # ocorre no ``transform`` via mapa ``SQ_PRESTADOR_CONTAS → candidato``
        # construído a partir de receitas e ``despesas_contratadas`` (que
        # carregam os metadados completos). Schemas mais velhos (2018/2020)
        # e fixtures minimais que incluem ``NR_CPF_CANDIDATO`` continuam
        # sendo resolvidos direto pelas colunas — fallback preservado.
        for row in _iter_csv_rows(
            zip_bytes, filename_prefix="despesas_pagas_candidatos", year=self.year,
        ):
            if (row.get("SG_UF") or row.get("UF_ELEICAO") or "").strip().upper() != uf:
                continue
            self._despesas_raw.append(row)
            if self.limit and len(self._despesas_raw) >= self.limit:
                break

        # --- Despesas contratadas (enriquecimento de fornecedor + CPF candidato) ---
        # Irmã da ``despesas_pagas``, mas com schema completo: CPF candidato,
        # CPF/CNPJ do fornecedor, nome do fornecedor, DS_CARGO. Usada em
        # ``transform`` pra construir (a) o mapa ``SQ_PRESTADOR_CONTAS →
        # candidato`` como fallback quando receitas não cobre o prestador
        # e (b) o mapa ``(SQ_PRESTADOR_CONTAS, SQ_DESPESA) → fornecedor``
        # pra hidratar os nós ``:CampaignExpense`` com fornecedor legível.
        for row in _iter_csv_rows(
            zip_bytes, filename_prefix="despesas_contratadas_candidatos",
            year=self.year,
        ):
            if (row.get("SG_UF") or row.get("UF_ELEICAO") or "").strip().upper() != uf:
                continue
            self._despesas_contratadas_raw.append(row)
            if self.limit and len(self._despesas_contratadas_raw) >= self.limit:
                break

        # --- Bens ---
        for row in _iter_csv_rows(
            zip_bytes, filename_prefix="bens_candidato", year=self.year,
        ):
            if (row.get("SG_UF") or row.get("UF_ELEICAO") or "").strip().upper() != uf:
                continue
            self._bens_raw.append(row)
            if self.limit and len(self._bens_raw) >= self.limit:
                break

        self.rows_in = (
            len(self._receitas_raw)
            + len(self._despesas_raw)
            + len(self._despesas_contratadas_raw)
            + len(self._bens_raw)
        )
        logger.info(
            "[tse_prestacao_contas_go] uf=%s year=%d receitas=%d despesas_pagas=%d "
            "despesas_contratadas=%d bens=%d",
            uf, self.year, len(self._receitas_raw),
            len(self._despesas_raw), len(self._despesas_contratadas_raw),
            len(self._bens_raw),
        )

    # ------------------------------------------------------------------
    # transform
    # ------------------------------------------------------------------

    def transform(self) -> None:
        year = self.year
        # Indexa candidatos encontrados em qualquer uma das 4 CSVs —
        # chave primária é o CPF formatado (11 dígitos). ``SQ_CANDIDATO``
        # vira ``numero_candidato`` secundário pra debugging.
        # Agregados por CPF:
        by_cpf: dict[str, dict[str, Any]] = {}

        # Mapa auxiliar: ``SQ_PRESTADOR_CONTAS`` → metadados do candidato.
        # Construído a partir de receitas (primário) e contratadas
        # (fallback). Usado pra resolver o candidato em ``despesas_pagas``,
        # que no schema TSE 2022+ não traz CPF/nome diretamente. Sem esse
        # mapa, 100% das despesas seriam descartadas pelo filtro
        # ``len(cpf_digits) != 11``.
        prestador_map: dict[str, dict[str, str]] = {}

        # Mapa auxiliar pra enriquecer fornecedor da despesa paga quando
        # o schema da CSV pagas não trazer os campos (TSE 2022+). Chave:
        # ``(SQ_PRESTADOR_CONTAS, SQ_DESPESA)``.
        fornecedor_map: dict[tuple[str, str], dict[str, str]] = {}

        def _ensure(cpf_formatted: str, cpf_digits: str, nome: str, sq: str) -> dict[str, Any]:
            entry = by_cpf.get(cpf_formatted)
            if entry is None:
                entry = {
                    "cpf": cpf_formatted,
                    "cpf_digits": cpf_digits,
                    "name": nome,
                    "sq_candidato": sq,
                    "uf": self.uf,
                    "ano": year,
                    "total_receitas": 0.0,
                    _BUCKET_PARTIDO: 0.0,
                    _BUCKET_PROPRIOS: 0.0,
                    _BUCKET_PESSOA_FISICA: 0.0,
                    _BUCKET_PESSOA_JURIDICA: 0.0,
                    _BUCKET_FIN_COLETIVO: 0.0,
                    _BUCKET_OUTROS: 0.0,
                    "patrimonio_declarado": 0.0,
                    "total_despesas": 0.0,
                    "cargo": "",
                }
                by_cpf[cpf_formatted] = entry
            elif nome and not entry["name"]:
                entry["name"] = nome
            return entry

        # --- Receitas (doações) ---
        for idx, row in enumerate(self._receitas_raw):
            cpf_raw = (
                row.get("NR_CPF_CANDIDATO")
                or row.get("CPF_CANDIDATO")
                or ""
            )
            cpf_digits = strip_document(cpf_raw)
            if len(cpf_digits) != 11:
                continue
            cpf_formatted = format_cpf(cpf_raw)
            nome = normalize_name(row.get("NM_CANDIDATO") or row.get("NOME_CANDIDATO") or "")
            sq = (row.get("SQ_CANDIDATO") or "").strip()
            valor_raw = row.get("VR_RECEITA") or row.get("VALOR_RECEITA") or "0"
            valor = parse_numeric_comma(valor_raw)

            # Classificação do bucket: DS_ORIGEM_RECEITA + fallback DS_FONTE_RECEITA.
            origem = (
                row.get("DS_ORIGEM_RECEITA")
                or row.get("DS_FONTE_RECEITA")
                or row.get("DS_ESPECIE_RECEITA")
                or ""
            )
            bucket = _classify_origem(origem)

            entry = _ensure(cpf_formatted, cpf_digits, nome, sq)
            entry["total_receitas"] += valor
            entry[bucket] += valor

            # Cargo — capturamos da CSV de receitas (DS_CARGO) pra popular
            # ``cargo_tse_{YYYY}`` no Person. A CSV de despesas também tem
            # o campo, mas a de receitas é mais consistente (sempre
            # presente quando há doação). Primeira ocorrência vence.
            cargo_raw = (
                row.get("DS_CARGO")
                or row.get("CARGO")
                or ""
            ).strip()
            if cargo_raw and not entry["cargo"]:
                entry["cargo"] = cargo_raw

            # Popula o mapa SQ_PRESTADOR_CONTAS → candidato, usado em
            # seguida pra resolver o CPF em ``despesas_pagas`` (TSE 2022+
            # não publica CPF na CSV de pagas — só SQ_PRESTADOR_CONTAS).
            sq_prestador = (row.get("SQ_PRESTADOR_CONTAS") or "").strip()
            if sq_prestador and sq_prestador not in prestador_map:
                prestador_map[sq_prestador] = {
                    "cpf_formatted": cpf_formatted,
                    "cpf_digits": cpf_digits,
                    "name": nome,
                    "sq_candidato": sq,
                    "cargo": cargo_raw,
                }

            # Node :CampaignDonation (opcional — mantido pra phase-2 tie-in).
            cpf_cnpj_doador_raw = (
                row.get("NR_CPF_CNPJ_DOADOR")
                or row.get("CPF_CNPJ_DOADOR")
                or ""
            )
            doador_digits = strip_document(cpf_cnpj_doador_raw)
            doador_is_cpf = len(doador_digits) == 11
            doador_is_cnpj = len(doador_digits) == 14
            if doador_is_cpf:
                doador_id = mask_cpf(cpf_cnpj_doador_raw)
            elif doador_is_cnpj:
                doador_id = doador_digits
            else:
                doador_id = ""
            doador_nome = normalize_name(
                row.get("NM_DOADOR") or row.get("NOME_DOADOR") or "",
            )
            # DT_RECEITA do TSE chega como DD/MM/YYYY; ``parse_date``
            # devolve "" quando ausente/inválida (Neo4j aceita string
            # vazia na rel).
            donated_at = parse_date((row.get("DT_RECEITA") or "").strip())
            did = _donation_id(sq, year, doador_id or "anon", f"{valor:.2f}", idx)
            donation_node = self.attach_provenance(
                {
                    "donation_id": did,
                    "valor": valor,
                    "ano": year,
                    "donated_at": donated_at,
                    "origem_receita": origem.strip(),
                    "bucket": bucket,
                    "doador_id": doador_id,
                    "doador_tipo": (
                        "pf" if doador_is_cpf
                        else ("pj" if doador_is_cnpj else "desconhecido")
                    ),
                    "doador_nome": doador_nome,
                    "candidato_cpf": cpf_formatted,
                    "candidato_nome": nome,
                    "uf": self.uf,
                    "source": _SOURCE_ID,
                },
                record_id=f"{sq}:{year}:receita:{idx}",
                record_url=self._zip_url,
                snapshot_uri=self._snapshot_uri,
            )
            self.donations.append(donation_node)

            rel_row = self.attach_provenance(
                {
                    "source_key": doador_id or f"anon:{did}",
                    "target_key": cpf_formatted,
                    "valor": valor,
                    "ano": year,
                    "donated_at": donated_at,
                    "donation_id": did,
                    "doador_tipo": (
                        "pf" if doador_is_cpf
                        else ("pj" if doador_is_cnpj else "desconhecido")
                    ),
                },
                record_id=did,
                record_url=self._zip_url,
                snapshot_uri=self._snapshot_uri,
            )
            self.donation_rels.append(rel_row)

        # --- Despesas contratadas (enriquecimento) ---
        # Percorremos antes de ``despesas_pagas`` pra:
        #   1. Cobrir prestadores que não apareceram em receitas (raro mas
        #      possível — candidato que contratou sem declarar receita).
        #   2. Construir o mapa ``(SQ_PRESTADOR_CONTAS, SQ_DESPESA) →
        #      fornecedor`` usado pra hidratar as pagas.
        # Não somamos ``VR_DESPESA_CONTRATADA`` em ``total_despesas`` —
        # seguimos o contrato de ``total_despesas_tse_{year}`` como "pago".
        for row in self._despesas_contratadas_raw:
            cpf_raw = (
                row.get("NR_CPF_CANDIDATO")
                or row.get("CPF_CANDIDATO")
                or ""
            )
            cpf_digits = strip_document(cpf_raw)
            nome = normalize_name(
                row.get("NM_CANDIDATO") or row.get("NOME_CANDIDATO") or "",
            )
            sq_candidato = (row.get("SQ_CANDIDATO") or "").strip()
            sq_prestador = (row.get("SQ_PRESTADOR_CONTAS") or "").strip()
            cargo_raw = (
                row.get("DS_CARGO") or row.get("CARGO") or ""
            ).strip()

            # Preenche ``prestador_map`` se receitas não cobriu (fallback).
            if (
                sq_prestador
                and len(cpf_digits) == 11
                and sq_prestador not in prestador_map
            ):
                prestador_map[sq_prestador] = {
                    "cpf_formatted": format_cpf(cpf_raw),
                    "cpf_digits": cpf_digits,
                    "name": nome,
                    "sq_candidato": sq_candidato,
                    "cargo": cargo_raw,
                }

            # Mapa de fornecedor por (prestador, SQ_DESPESA).
            sq_despesa = (row.get("SQ_DESPESA") or "").strip()
            if sq_prestador and sq_despesa:
                fornecedor_raw = (
                    row.get("NR_CPF_CNPJ_FORNECEDOR")
                    or row.get("CPF_CNPJ_FORNECEDOR")
                    or ""
                )
                fornecedor_digits = strip_document(fornecedor_raw)
                fornecedor_nome = normalize_name(
                    row.get("NM_FORNECEDOR") or row.get("NOME_FORNECEDOR") or "",
                )
                tipo_despesa = (
                    row.get("DS_DESPESA")
                    or row.get("DS_NATUREZA_DESPESA")
                    or row.get("DS_ORIGEM_DESPESA")
                    or row.get("DS_TIPO_DESPESA")
                    or ""
                ).strip()
                fornecedor_map.setdefault(
                    (sq_prestador, sq_despesa),
                    {
                        "fornecedor_digits": fornecedor_digits,
                        "fornecedor_nome": fornecedor_nome,
                        "tipo_despesa": tipo_despesa,
                    },
                )

        # --- Despesas (despesas_pagas) ---
        # Resolução de candidato: tenta CPF direto primeiro (schemas antigos
        # + fixtures minimais), depois cai no ``prestador_map`` via
        # ``SQ_PRESTADOR_CONTAS`` (schema TSE 2022+). Fornecedor idem —
        # direto, com fallback no ``fornecedor_map`` via
        # ``(SQ_PRESTADOR_CONTAS, SQ_DESPESA)``.
        for idx, row in enumerate(self._despesas_raw):
            cpf_raw = (
                row.get("NR_CPF_CANDIDATO")
                or row.get("CPF_CANDIDATO")
                or ""
            )
            cpf_digits = strip_document(cpf_raw)
            nome = normalize_name(row.get("NM_CANDIDATO") or row.get("NOME_CANDIDATO") or "")
            sq = (row.get("SQ_CANDIDATO") or "").strip()
            cargo_desp = (
                row.get("DS_CARGO") or row.get("CARGO") or ""
            ).strip()

            if len(cpf_digits) != 11:
                # Schema TSE 2022+: resolve via SQ_PRESTADOR_CONTAS.
                sq_prestador = (row.get("SQ_PRESTADOR_CONTAS") or "").strip()
                resolved = prestador_map.get(sq_prestador)
                if resolved is None:
                    continue
                cpf_formatted = resolved["cpf_formatted"]
                cpf_digits = resolved["cpf_digits"]
                if not nome:
                    nome = resolved["name"]
                if not sq:
                    sq = resolved["sq_candidato"]
                if not cargo_desp:
                    cargo_desp = resolved["cargo"]
            else:
                cpf_formatted = format_cpf(cpf_raw)

            valor_raw = row.get("VR_PAGTO_DESPESA") or row.get("VR_DESPESA_CONTRATADA") or "0"
            valor = parse_numeric_comma(valor_raw)
            if valor <= 0:
                continue
            entry = _ensure(cpf_formatted, cpf_digits, nome, sq)
            entry["total_despesas"] += valor
            # Fallback de cargo: usa a CSV de despesas se receitas não veio
            # (caso raro — candidato com despesa paga mas sem receita
            # registrada, provavelmente financiou do próprio bolso antes
            # da declaração).
            if cargo_desp and not entry["cargo"]:
                entry["cargo"] = cargo_desp
            cnpj_cpf_fornecedor_raw = (
                row.get("NR_CPF_CNPJ_FORNECEDOR")
                or row.get("CPF_CNPJ_FORNECEDOR")
                or ""
            )
            fornecedor_digits = strip_document(cnpj_cpf_fornecedor_raw)
            fornecedor_nome = normalize_name(
                row.get("NM_FORNECEDOR") or row.get("NOME_FORNECEDOR") or "",
            )
            tipo_despesa = (
                row.get("DS_TIPO_DESPESA")
                or row.get("DS_NATUREZA_DESPESA")
                or row.get("DS_DESPESA")
                or row.get("DESC_DESPESA")
                or ""
            ).strip()

            # Fallback de fornecedor via contratadas quando pagas não traz.
            if not fornecedor_digits and not fornecedor_nome:
                sq_prestador = (row.get("SQ_PRESTADOR_CONTAS") or "").strip()
                sq_despesa = (row.get("SQ_DESPESA") or "").strip()
                fornecedor_info = fornecedor_map.get((sq_prestador, sq_despesa))
                if fornecedor_info is not None:
                    fornecedor_digits = fornecedor_info["fornecedor_digits"]
                    fornecedor_nome = fornecedor_info["fornecedor_nome"]
                    if not tipo_despesa:
                        tipo_despesa = fornecedor_info["tipo_despesa"]
            eid = _expense_id(sq, year, fornecedor_digits or "sem_doc", f"{valor:.2f}", idx)
            expense_node = self.attach_provenance(
                {
                    "expense_id": eid,
                    "valor": valor,
                    "ano": year,
                    "tipo_despesa": tipo_despesa,
                    "fornecedor_documento": fornecedor_digits,
                    "fornecedor_nome": fornecedor_nome,
                    "candidato_cpf": cpf_formatted,
                    "uf": self.uf,
                    "source": _SOURCE_ID,
                },
                record_id=f"{sq}:{year}:despesa:{idx}",
                record_url=self._zip_url,
                snapshot_uri=self._snapshot_uri,
            )
            self.expenses.append(expense_node)

            rel_row = self.attach_provenance(
                {
                    "source_key": cpf_formatted,
                    "target_key": eid,
                    "valor": valor,
                    "ano": year,
                    "tipo_despesa": tipo_despesa,
                },
                record_id=eid,
                record_url=self._zip_url,
                snapshot_uri=self._snapshot_uri,
            )
            self.expense_rels.append(rel_row)

        # --- Bens (patrimônio declarado) ---
        for row in self._bens_raw:
            cpf_raw = (
                row.get("NR_CPF_CANDIDATO")
                or row.get("CPF_CANDIDATO")
                or ""
            )
            cpf_digits = strip_document(cpf_raw)
            if len(cpf_digits) != 11:
                continue
            cpf_formatted = format_cpf(cpf_raw)
            nome = normalize_name(row.get("NM_CANDIDATO") or row.get("NOME_CANDIDATO") or "")
            sq = (row.get("SQ_CANDIDATO") or "").strip()
            valor_raw = row.get("VR_BEM_CANDIDATO") or row.get("VALOR_BEM") or "0"
            valor = parse_numeric_comma(valor_raw)
            entry = _ensure(cpf_formatted, cpf_digits, nome, sq)
            entry["patrimonio_declarado"] += valor

        # --- Consolidar :Person rows com proveniência ---
        for cpf_formatted, entry in by_cpf.items():
            sq = str(entry["sq_candidato"] or "")
            props: dict[str, Any] = {
                "cpf": cpf_formatted,
                "name": entry["name"],
                "uf": self.uf,
                "numero_candidato": sq,
                "patrimonio_declarado": round(entry["patrimonio_declarado"], 2),
                "patrimonio_ano": year,
                f"total_tse_{year}": round(entry["total_receitas"], 2),
                f"tse_{year}_partido": round(entry[_BUCKET_PARTIDO], 2),
                f"tse_{year}_proprios": round(entry[_BUCKET_PROPRIOS], 2),
                f"tse_{year}_pessoa_fisica": round(entry[_BUCKET_PESSOA_FISICA], 2),
                f"tse_{year}_pessoa_juridica": round(entry[_BUCKET_PESSOA_JURIDICA], 2),
                f"tse_{year}_fin_coletivo": round(entry[_BUCKET_FIN_COLETIVO], 2),
                f"tse_{year}_outros": round(entry[_BUCKET_OUTROS], 2),
                f"total_despesas_tse_{year}": round(entry["total_despesas"], 2),
                "source": _SOURCE_ID,
            }
            # Cargo é nullable — só adiciona se veio alguma CSV com DS_CARGO.
            # Facilita cross-check com ``total_despesas_tse_{year}`` vs
            # teto legal no service :mod:`bracc.services.teto_service`.
            if entry["cargo"]:
                props[f"cargo_tse_{year}"] = entry["cargo"]
            node_row = self.attach_provenance(
                props,
                record_id=f"{sq}:{year}",
                record_url=self._zip_url,
                snapshot_uri=self._snapshot_uri,
            )
            self.persons.append(node_row)

        self.persons = deduplicate_rows(self.persons, ["cpf"])
        self.donations = deduplicate_rows(self.donations, ["donation_id"])
        self.donation_rels = deduplicate_rows(
            self.donation_rels, ["source_key", "target_key", "donation_id"],
        )
        self.expenses = deduplicate_rows(self.expenses, ["expense_id"])
        self.expense_rels = deduplicate_rows(
            self.expense_rels, ["source_key", "target_key"],
        )
        self.rows_loaded = (
            len(self.persons)
            + len(self.donations) + len(self.donation_rels)
            + len(self.expenses) + len(self.expense_rels)
        )
        logger.info(
            "[tse_prestacao_contas_go] transformed persons=%d donations=%d "
            "expenses=%d rels=%d",
            len(self.persons),
            len(self.donations),
            len(self.expenses),
            len(self.donation_rels) + len(self.expense_rels),
        )

        # Guard contra falha silenciosa de schema — se o extract trouxe
        # linhas de despesas mas nenhuma sobreviveu ao transform, algum
        # mapeamento de coluna quebrou (ex.: TSE renomeou campos). Sem
        # esse guard, ``total_despesas_tse_{year}`` fica 0.0 e o
        # ``teto_service`` reporta "0% do teto utilizado" pra todo
        # candidato — o que é um claim factualmente errado. Prefere falhar
        # explicitamente em prod (``BRACC_STRICT_TRANSFORM=1``) a deixar
        # o dado errado vazar pro grafo.
        if self._despesas_raw and not self.expenses:
            msg = (
                f"[tse_prestacao_contas_go] extracted {len(self._despesas_raw)} "
                "despesas rows but transform produced 0 expenses — schema "
                "drift suspeito (colunas CPF/valor/SQ_PRESTADOR_CONTAS mudaram)"
            )
            logger.error(msg)
            if os.environ.get("BRACC_STRICT_TRANSFORM") == "1":
                raise RuntimeError(msg)
        # Análogo pra receitas — se 100% das linhas foram descartadas,
        # algum campo-chave (NR_CPF_CANDIDATO / VR_RECEITA) quebrou.
        if self._receitas_raw and not self.donations:
            msg = (
                f"[tse_prestacao_contas_go] extracted {len(self._receitas_raw)} "
                "receitas rows but transform produced 0 donations — schema "
                "drift suspeito"
            )
            logger.error(msg)
            if os.environ.get("BRACC_STRICT_TRANSFORM") == "1":
                raise RuntimeError(msg)

    # ------------------------------------------------------------------
    # load
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not self.persons:
            logger.warning("[tse_prestacao_contas_go] nothing to load")
            return
        loader = Neo4jBatchLoader(self.driver)

        # ``:Person`` — MERGE by ``cpf``. Já existente no grafo via outros
        # pipelines (TSE, CNJ, camara_politicos_go) ganha as propriedades
        # novas por cima; ausente vira nó novo pra não bloquear validação.
        loader.load_nodes("Person", self.persons, key_field="cpf")

        # Campaign donation nodes + rels — fase opcional, mas já viabilizada.
        if self.donations:
            loader.load_nodes(
                "CampaignDonation", self.donations, key_field="donation_id",
            )
        # Ensure :CampaignDonor-like stubs for CNPJ and anon targets? We
        # route the rel via Person(cpf) pro alvo (candidato). Fonte é
        # mascarada/CNPJ — o product pipeline CNPJ (receita federal)
        # resolve os nomes legíveis posteriormente.
        if self.donation_rels:
            # MERGE genérico: doador → candidato. source_key pode ser CPF
            # mascarado ou CNPJ digits; não forçamos um label específico
            # aqui (deixamos a rel "DOOU" apontando pra :Person do
            # candidato, que é o caminho que o Flask consome).
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.target_key}) "
                "MERGE (p)<-[r:DOOU {donation_id: row.donation_id}]-(d) "
                "ON CREATE SET d:CampaignDonor, "
                "   d.doador_id = row.source_key, "
                "   d.doador_tipo = row.doador_tipo "
                "SET r.valor = row.valor, "
                "    r.ano = row.ano, "
                "    r.donated_at = row.donated_at, "
                "    r.source_id = row.source_id, "
                "    r.source_record_id = row.source_record_id, "
                "    r.source_url = row.source_url, "
                "    r.source_snapshot_uri = row.source_snapshot_uri, "
                "    r.ingested_at = row.ingested_at, "
                "    r.run_id = row.run_id"
            )
            # Fallback simplificado: ``d`` sem ``:CampaignDonor`` MERGE-
            # key explícito geraria novo nó por re-run. Usamos o pattern
            # (p)<-[r:DOOU]-(d) com MERGE pela relação + ``donation_id``
            # único pra idempotência.
            loader.run_query_with_retry(query, self.donation_rels)

        if self.expenses:
            loader.load_nodes(
                "CampaignExpense", self.expenses, key_field="expense_id",
            )
        if self.expense_rels:
            loader.load_relationships(
                rel_type="GASTOU_CAMPANHA",
                rows=self.expense_rels,
                source_label="Person",
                source_key="cpf",
                target_label="CampaignExpense",
                target_key="expense_id",
            )

        logger.info(
            "[tse_prestacao_contas_go] loaded persons=%d donations=%d expenses=%d",
            len(self.persons), len(self.donations), len(self.expenses),
        )

"""Tests for the ``tse_prestacao_contas_go`` pipeline.

Covers:

* ZIP fixture built em memória com as 4 CSVs (receitas, despesas_pagas,
  despesas_contratadas, bens) usando o **schema real TSE 2022** — não
  sintético. Despesas pagas só trazem ``SQ_PRESTADOR_CONTAS`` (sem
  CPF/nome candidato nem fornecedor), exercitando o lookup via mapa
  construído em ``transform``. Sem isso, a regressão de 2026-04-18
  (113k rows extraídas → 0 sobreviviam) não é capturada.
* Archival retrofit — ZIP inteiro vira snapshot content-addressed e toda
  row carimba ``source_snapshot_uri`` apontando pra ele.
* Propriedades atualizadas em ``:Person`` com o shape esperado pelo Flask
  (``gerar_validacao_tse``): ``total_tse_2022``, ``tse_2022_partido``,
  ``tse_2022_pessoa_fisica``, ``tse_2022_proprios``, ``tse_2022_fin_coletivo``
  + ``patrimonio_declarado`` / ``patrimonio_ano`` + ``total_despesas_tse_2022``.
* Filtro UF=GO — rows de outras UFs são descartadas antes do load.
* LGPD — CPF de doador pessoa física é mascarado em todas as
  estruturas expostas.
* Provenance — 6 campos + ``source_snapshot_uri``.
"""

from __future__ import annotations

import csv as _csv
import io
import zipfile
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.tse_prestacao_contas_go import (
    _SOURCE_ID,
    TsePrestacaoContasGoPipeline,
    _classify_origem,
)
from tests._mock_helpers import mock_driver, mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixture ZIP — 3 candidatos GO + 1 candidato SP (pra testar filtro UF),
# usando o schema REAL publicado pelo TSE 2022 (headers completos, CPF
# só em receitas/contratadas/bens; pagas trazem apenas SQ_PRESTADOR_CONTAS).
# ---------------------------------------------------------------------------

_YEAR = 2022

# Identificação dos candidatos — as mesmas constantes são reaproveitadas
# nos rows de receitas/contratadas/bens e nos asserts. Os CPFs são
# sintéticos (dígitos verificadores válidos pra passar em qualquer
# validador, porém os titulares não existem na RFB).
_C1 = {
    "sq_prestador": "3000000001",
    "sq_candidato": "GO0001",
    "nr_candidato": "55001",
    "cpf": "11144477735",
    "nome": "CANDIDATO UM",
    "cargo": "Deputado Estadual",
    "cd_cargo": "7",
    "partido": "18",
    "sg_partido": "REDE",
    "nm_partido": "Rede Sustentabilidade",
    "cnpj_prestador": "47574422000134",
}
_C2 = {
    "sq_prestador": "3000000002",
    "sq_candidato": "GO0002",
    "nr_candidato": "55002",
    "cpf": "52998224725",
    "nome": "CANDIDATA DOIS",
    "cargo": "Deputado Estadual",
    "cd_cargo": "7",
    "partido": "55",
    "sg_partido": "PSD",
    "nm_partido": "Partido Social Democrático",
    "cnpj_prestador": "47574723000168",
}
_C3 = {
    "sq_prestador": "3000000003",
    "sq_candidato": "GO0003",
    "nr_candidato": "55003",
    "cpf": "22233344456",
    "nome": "CANDIDATO TRES",
    "cargo": "Deputado Estadual",
    "cd_cargo": "7",
    "partido": "11",
    "sg_partido": "PP",
    "nm_partido": "Progressistas",
    "cnpj_prestador": "47574999000199",
}
_SP = {
    "sq_prestador": "4000000001",
    "sq_candidato": "SP0001",
    "nr_candidato": "22001",
    "cpf": "99988877766",
    "nome": "CANDIDATO SP",
    "cargo": "Deputado Estadual",
    "cd_cargo": "7",
    "partido": "13",
    "sg_partido": "PT",
    "nm_partido": "Partido dos Trabalhadores",
    "cnpj_prestador": "47574555000155",
}

_RECEITAS_FIELDS = [
    "DT_GERACAO", "HH_GERACAO", "AA_ELEICAO", "CD_TIPO_ELEICAO",
    "NM_TIPO_ELEICAO", "CD_ELEICAO", "DS_ELEICAO", "DT_ELEICAO",
    "ST_TURNO", "TP_PRESTACAO_CONTAS", "DT_PRESTACAO_CONTAS",
    "SQ_PRESTADOR_CONTAS", "SG_UF", "SG_UE", "NM_UE",
    "NR_CNPJ_PRESTADOR_CONTA", "CD_CARGO", "DS_CARGO", "SQ_CANDIDATO",
    "NR_CANDIDATO", "NM_CANDIDATO", "NR_CPF_CANDIDATO",
    "NR_CPF_VICE_CANDIDATO", "NR_PARTIDO", "SG_PARTIDO", "NM_PARTIDO",
    "CD_FONTE_RECEITA", "DS_FONTE_RECEITA", "CD_ORIGEM_RECEITA",
    "DS_ORIGEM_RECEITA", "CD_NATUREZA_RECEITA", "DS_NATUREZA_RECEITA",
    "CD_ESPECIE_RECEITA", "DS_ESPECIE_RECEITA", "CD_CNAE_DOADOR",
    "DS_CNAE_DOADOR", "NR_CPF_CNPJ_DOADOR", "NM_DOADOR", "NM_DOADOR_RFB",
    "CD_ESFERA_PARTIDARIA_DOADOR", "DS_ESFERA_PARTIDARIA_DOADOR",
    "SG_UF_DOADOR", "CD_MUNICIPIO_DOADOR", "NM_MUNICIPIO_DOADOR",
    "SQ_CANDIDATO_DOADOR", "NR_CANDIDATO_DOADOR",
    "CD_CARGO_CANDIDATO_DOADOR", "DS_CARGO_CANDIDATO_DOADOR",
    "NR_PARTIDO_DOADOR", "SG_PARTIDO_DOADOR", "NM_PARTIDO_DOADOR",
    "NR_RECIBO_DOACAO", "NR_DOCUMENTO_DOACAO", "SQ_RECEITA", "DT_RECEITA",
    "DS_RECEITA", "VR_RECEITA", "DS_NATUREZA_RECURSO_ESTIMAVEL",
    "DS_GENERO", "DS_COR_RACA",
]

# Schema REAL TSE 2022: ``despesas_pagas`` NÃO tem NR_CPF_CANDIDATO,
# SQ_CANDIDATO, NM_CANDIDATO, DS_CARGO nem fornecedor. Só SQ_PRESTADOR_CONTAS
# como chave de candidato + SQ_DESPESA pra joinar com contratadas.
_DESPESAS_PAGAS_FIELDS = [
    "DT_GERACAO", "HH_GERACAO", "AA_ELEICAO", "CD_TIPO_ELEICAO",
    "NM_TIPO_ELEICAO", "CD_ELEICAO", "DS_ELEICAO", "DT_ELEICAO",
    "ST_TURNO", "TP_PRESTACAO_CONTAS", "DT_PRESTACAO_CONTAS",
    "SQ_PRESTADOR_CONTAS", "SG_UF", "DS_TIPO_DOCUMENTO", "NR_DOCUMENTO",
    "CD_FONTE_DESPESA", "DS_FONTE_DESPESA", "CD_ORIGEM_DESPESA",
    "DS_ORIGEM_DESPESA", "CD_NATUREZA_DESPESA", "DS_NATUREZA_DESPESA",
    "CD_ESPECIE_RECURSO", "DS_ESPECIE_RECURSO", "SQ_DESPESA",
    "SQ_PARCELAMENTO_DESPESA", "DT_PAGTO_DESPESA", "DS_DESPESA",
    "VR_PAGTO_DESPESA",
]

_DESPESAS_CONTRATADAS_FIELDS = [
    "DT_GERACAO", "HH_GERACAO", "AA_ELEICAO", "CD_TIPO_ELEICAO",
    "NM_TIPO_ELEICAO", "CD_ELEICAO", "DS_ELEICAO", "DT_ELEICAO",
    "ST_TURNO", "TP_PRESTACAO_CONTAS", "DT_PRESTACAO_CONTAS",
    "SQ_PRESTADOR_CONTAS", "SG_UF", "SG_UE", "NM_UE",
    "NR_CNPJ_PRESTADOR_CONTA", "CD_CARGO", "DS_CARGO", "SQ_CANDIDATO",
    "NR_CANDIDATO", "NM_CANDIDATO", "NR_CPF_CANDIDATO",
    "NR_CPF_VICE_CANDIDATO", "NR_PARTIDO", "SG_PARTIDO", "NM_PARTIDO",
    "CD_TIPO_FORNECEDOR", "DS_TIPO_FORNECEDOR", "CD_CNAE_FORNECEDOR",
    "DS_CNAE_FORNECEDOR", "NR_CPF_CNPJ_FORNECEDOR", "NM_FORNECEDOR",
    "NM_FORNECEDOR_RFB", "CD_ESFERA_PART_FORNECEDOR",
    "DS_ESFERA_PART_FORNECEDOR", "SG_UF_FORNECEDOR",
    "CD_MUNICIPIO_FORNECEDOR", "NM_MUNICIPIO_FORNECEDOR",
    "SQ_CANDIDATO_FORNECEDOR", "NR_CANDIDATO_FORNECEDOR",
    "CD_CARGO_FORNECEDOR", "DS_CARGO_FORNECEDOR", "NR_PARTIDO_FORNECEDOR",
    "SG_PARTIDO_FORNECEDOR", "NM_PARTIDO_FORNECEDOR", "DS_TIPO_DOCUMENTO",
    "NR_DOCUMENTO", "CD_ORIGEM_DESPESA", "DS_ORIGEM_DESPESA", "SQ_DESPESA",
    "DT_DESPESA", "DS_DESPESA", "VR_DESPESA_CONTRATADA",
]

# Bens — o pipeline lê NR_CPF_CANDIDATO + VR_BEM_CANDIDATO + filtro UF.
# O arquivo real vive em ``bem_candidato_{year}.zip`` irmão, mas o
# pipeline localiza dentro do ZIP principal se presente (fixture OK).
_BENS_FIELDS = [
    "ANO_ELEICAO", "SG_UF", "SQ_CANDIDATO", "NR_CPF_CANDIDATO",
    "NM_CANDIDATO", "DS_TIPO_BEM_CANDIDATO", "DS_BEM_CANDIDATO",
    "VR_BEM_CANDIDATO",
]

_FIXED_META_RECEITA = {
    "DT_GERACAO": "12/04/2026",
    "HH_GERACAO": "15:00:11",
    "AA_ELEICAO": str(_YEAR),
    "CD_TIPO_ELEICAO": "2",
    "NM_TIPO_ELEICAO": "ORDINÁRIA",
    "CD_ELEICAO": "546",
    "DS_ELEICAO": "Eleições Gerais Estaduais 2022",
    "DT_ELEICAO": "02/10/2022",
    "ST_TURNO": "1",
    "TP_PRESTACAO_CONTAS": "FINAL",
    "DT_PRESTACAO_CONTAS": "16/11/2022",
}

_FIXED_META_PAGAS = {
    "DT_GERACAO": "12/04/2026",
    "HH_GERACAO": "15:00:31",
    "AA_ELEICAO": str(_YEAR),
    "CD_TIPO_ELEICAO": "2",
    "NM_TIPO_ELEICAO": "Ordinária",
    "CD_ELEICAO": "546",
    "DS_ELEICAO": "Eleições Gerais Estaduais 2022",
    "DT_ELEICAO": "02/10/2022",
    "ST_TURNO": "1",
    "TP_PRESTACAO_CONTAS": "Final",
    "DT_PRESTACAO_CONTAS": "05/12/2022",
}


def _receita_row(cand: dict[str, str], *, ue: str, sq_receita: str,
                 origem: str, valor: str, doador_cpf_cnpj: str,
                 doador_nome: str, dt_receita: str = "15/09/2022",
                 ds_receita: str = "Doação",
                 ds_genero: str = "Masculino",
                 ds_cor: str = "Parda") -> dict[str, str]:
    return {
        **_FIXED_META_RECEITA,
        "SQ_PRESTADOR_CONTAS": cand["sq_prestador"],
        "SG_UF": cand.get("uf", ue),
        "SG_UE": ue,
        "NM_UE": {"GO": "GOIÁS", "SP": "SÃO PAULO"}.get(ue, ue),
        "NR_CNPJ_PRESTADOR_CONTA": cand["cnpj_prestador"],
        "CD_CARGO": cand["cd_cargo"],
        "DS_CARGO": cand["cargo"],
        "SQ_CANDIDATO": cand["sq_candidato"],
        "NR_CANDIDATO": cand["nr_candidato"],
        "NM_CANDIDATO": cand["nome"],
        "NR_CPF_CANDIDATO": cand["cpf"],
        "NR_CPF_VICE_CANDIDATO": "",
        "NR_PARTIDO": cand["partido"],
        "SG_PARTIDO": cand["sg_partido"],
        "NM_PARTIDO": cand["nm_partido"],
        "CD_FONTE_RECEITA": "1",
        "DS_FONTE_RECEITA": "OUTROS RECURSOS",
        "CD_ORIGEM_RECEITA": "10010200",
        "DS_ORIGEM_RECEITA": origem,
        "CD_NATUREZA_RECEITA": "1",
        "DS_NATUREZA_RECEITA": "Financeiro",
        "CD_ESPECIE_RECEITA": "0",
        "DS_ESPECIE_RECEITA": "Dinheiro",
        "CD_CNAE_DOADOR": "-1",
        "DS_CNAE_DOADOR": "#NULO",
        "NR_CPF_CNPJ_DOADOR": doador_cpf_cnpj,
        "NM_DOADOR": doador_nome,
        "NM_DOADOR_RFB": doador_nome,
        "CD_ESFERA_PARTIDARIA_DOADOR": "-1",
        "DS_ESFERA_PARTIDARIA_DOADOR": "#NULO",
        "SG_UF_DOADOR": "#NULO#",
        "CD_MUNICIPIO_DOADOR": "-1",
        "NM_MUNICIPIO_DOADOR": "#NULO",
        "SQ_CANDIDATO_DOADOR": "-1",
        "NR_CANDIDATO_DOADOR": "-1",
        "CD_CARGO_CANDIDATO_DOADOR": "-1",
        "DS_CARGO_CANDIDATO_DOADOR": "#NULO",
        "NR_PARTIDO_DOADOR": "-1",
        "SG_PARTIDO_DOADOR": "#NULO",
        "NM_PARTIDO_DOADOR": "#NULO",
        "NR_RECIBO_DOACAO": "#NULO#",
        "NR_DOCUMENTO_DOACAO": "#NULO#",
        "SQ_RECEITA": sq_receita,
        "DT_RECEITA": dt_receita,
        "DS_RECEITA": ds_receita,
        "VR_RECEITA": valor,
        "DS_NATUREZA_RECURSO_ESTIMAVEL": "",
        "DS_GENERO": ds_genero,
        "DS_COR_RACA": ds_cor,
    }


def _pagas_row(cand: dict[str, str], *, sq_despesa: str, nr_documento: str,
               valor: str, ds_despesa: str = "Serviços gerais",
               dt_pagto: str = "30/09/2022") -> dict[str, str]:
    return {
        **_FIXED_META_PAGAS,
        "SQ_PRESTADOR_CONTAS": cand["sq_prestador"],
        "SG_UF": cand.get("uf", "GO" if cand is not _SP else "SP"),
        "DS_TIPO_DOCUMENTO": "Nota Fiscal",
        "NR_DOCUMENTO": nr_documento,
        "CD_FONTE_DESPESA": "1",
        "DS_FONTE_DESPESA": "Outros Recursos",
        "CD_ORIGEM_DESPESA": "20010000",
        "DS_ORIGEM_DESPESA": "Despesas diversas",
        "CD_NATUREZA_DESPESA": "1",
        "DS_NATUREZA_DESPESA": "Financeiro",
        "CD_ESPECIE_RECURSO": "0",
        "DS_ESPECIE_RECURSO": "Dinheiro",
        "SQ_DESPESA": sq_despesa,
        "SQ_PARCELAMENTO_DESPESA": "0",
        "DT_PAGTO_DESPESA": dt_pagto,
        "DS_DESPESA": ds_despesa,
        "VR_PAGTO_DESPESA": valor,
    }


def _contratada_row(cand: dict[str, str], *, sq_despesa: str,
                    nr_documento: str, valor: str,
                    fornecedor_doc: str, fornecedor_nome: str,
                    ds_despesa: str = "Serviços gerais",
                    ue: str = "GO") -> dict[str, str]:
    return {
        **_FIXED_META_PAGAS,
        "SQ_PRESTADOR_CONTAS": cand["sq_prestador"],
        "SG_UF": cand.get("uf", ue),
        "SG_UE": ue,
        "NM_UE": {"GO": "GOIÁS", "SP": "SÃO PAULO"}.get(ue, ue),
        "NR_CNPJ_PRESTADOR_CONTA": cand["cnpj_prestador"],
        "CD_CARGO": cand["cd_cargo"],
        "DS_CARGO": cand["cargo"],
        "SQ_CANDIDATO": cand["sq_candidato"],
        "NR_CANDIDATO": cand["nr_candidato"],
        "NM_CANDIDATO": cand["nome"],
        "NR_CPF_CANDIDATO": cand["cpf"],
        "NR_CPF_VICE_CANDIDATO": "",
        "NR_PARTIDO": cand["partido"],
        "SG_PARTIDO": cand["sg_partido"],
        "NM_PARTIDO": cand["nm_partido"],
        "CD_TIPO_FORNECEDOR": "1",
        "DS_TIPO_FORNECEDOR": "PESSOA JURÍDICA",
        "CD_CNAE_FORNECEDOR": "18130",
        "DS_CNAE_FORNECEDOR": "Impressão",
        "NR_CPF_CNPJ_FORNECEDOR": fornecedor_doc,
        "NM_FORNECEDOR": fornecedor_nome,
        "NM_FORNECEDOR_RFB": fornecedor_nome,
        "CD_ESFERA_PART_FORNECEDOR": "-1",
        "DS_ESFERA_PART_FORNECEDOR": "#NULO",
        "SG_UF_FORNECEDOR": "#NULO#",
        "CD_MUNICIPIO_FORNECEDOR": "-1",
        "NM_MUNICIPIO_FORNECEDOR": "#NULO",
        "SQ_CANDIDATO_FORNECEDOR": "-1",
        "NR_CANDIDATO_FORNECEDOR": "-1",
        "CD_CARGO_FORNECEDOR": "-1",
        "DS_CARGO_FORNECEDOR": "#NULO",
        "NR_PARTIDO_FORNECEDOR": "-1",
        "SG_PARTIDO_FORNECEDOR": "#NULO",
        "NM_PARTIDO_FORNECEDOR": "#NULO",
        "DS_TIPO_DOCUMENTO": "Nota Fiscal",
        "NR_DOCUMENTO": nr_documento,
        "CD_ORIGEM_DESPESA": "20010000",
        "DS_ORIGEM_DESPESA": "Despesas diversas",
        "SQ_DESPESA": sq_despesa,
        "DT_DESPESA": "05/09/2022",
        "DS_DESPESA": ds_despesa,
        "VR_DESPESA_CONTRATADA": valor,
    }


def _bem_row(cand: dict[str, str], *, ue: str, tipo: str, ds: str,
             valor: str) -> dict[str, str]:
    return {
        "ANO_ELEICAO": str(_YEAR),
        "SG_UF": ue,
        "SQ_CANDIDATO": cand["sq_candidato"],
        "NR_CPF_CANDIDATO": cand["cpf"],
        "NM_CANDIDATO": cand["nome"],
        "DS_TIPO_BEM_CANDIDATO": tipo,
        "DS_BEM_CANDIDATO": ds,
        "VR_BEM_CANDIDATO": valor,
    }


# 3 candidatos GO com 5 doações cada + 1 candidato SP (5 linhas pra
# garantir que o filtro UF=GO exclui o SP inteiro).
_RECEITAS_ROWS_GO_C1 = [
    _receita_row(_C1, ue="GO", sq_receita="RC1001",
                 origem="Recursos de partido político",
                 valor="1000,00", doador_cpf_cnpj="12345678000100",
                 doador_nome="PARTIDO X"),
    _receita_row(_C1, ue="GO", sq_receita="RC1002",
                 origem="Recursos próprios",
                 valor="500,50", doador_cpf_cnpj="11144477735",
                 doador_nome="CANDIDATO UM"),
    _receita_row(_C1, ue="GO", sq_receita="RC1003",
                 origem="Recursos de pessoas físicas",
                 valor="200,00", doador_cpf_cnpj="22233344456",
                 doador_nome="ZE DA SILVA"),
    _receita_row(_C1, ue="GO", sq_receita="RC1004",
                 origem="Recursos de financiamento coletivo",
                 valor="300,00", doador_cpf_cnpj="", doador_nome=""),
    _receita_row(_C1, ue="GO", sq_receita="RC1005",
                 origem="", valor="50,00",
                 doador_cpf_cnpj="", doador_nome=""),
]
_RECEITAS_ROWS_GO_C2 = [
    _receita_row(
        _C2, ue="GO", sq_receita=f"RC200{n}",
        origem="Recursos de pessoas físicas", valor="100,00",
        doador_cpf_cnpj=f"555666777{n:02d}", doador_nome=f"DOADOR {n}",
    )
    for n in range(88, 93)
]
_RECEITAS_ROWS_GO_C3 = [
    _receita_row(_C3, ue="GO", sq_receita=f"RC300{i}",
                 origem="Recursos próprios", valor="400,00",
                 doador_cpf_cnpj="22233344456",
                 doador_nome="CANDIDATO TRES")
    for i in range(5)
]
_RECEITAS_ROWS_SP = [
    _receita_row(_SP, ue="SP", sq_receita=f"RS100{i}",
                 origem="Recursos de pessoas físicas", valor="9999,00",
                 doador_cpf_cnpj="11122233344",
                 doador_nome="DOADOR SP")
    for i in range(5)
]

# Despesas pagas — schema TSE 2022 REAL: SEM CPF candidato, SEM fornecedor.
# C1: 1 despesa de R$300 (prestador 3000000001, SQ_DESPESA 500001)
# C2: 1 despesa de R$150 (prestador 3000000002, SQ_DESPESA 500002)
# SP: 1 despesa R$5000 (descartada por filtro UF)
_DESPESAS_PAGAS_ROWS = [
    _pagas_row(_C1, sq_despesa="500001", nr_documento="NF001",
               valor="300,00", ds_despesa="Publicidade"),
    _pagas_row(_C2, sq_despesa="500002", nr_documento="NF002",
               valor="150,00", ds_despesa="Combustível"),
    _pagas_row(_SP, sq_despesa="500099", nr_documento="NF099",
               valor="5000,00", ds_despesa="Aluguel"),
]
# Os rows SP precisam de SG_UF='SP' — o helper detecta via `cand is _SP`
# (os dicts C1/C2/C3 caem no default 'GO').
_DESPESAS_PAGAS_ROWS[-1]["SG_UF"] = "SP"

# Despesas contratadas — parear (SQ_PRESTADOR_CONTAS, SQ_DESPESA) com
# pagas pra testar o enriquecimento de fornecedor.
_DESPESAS_CONTRATADAS_ROWS = [
    _contratada_row(_C1, sq_despesa="500001", nr_documento="NF001",
                    valor="300,00", fornecedor_doc="22222222000100",
                    fornecedor_nome="GRAFICA GO", ds_despesa="Publicidade"),
    _contratada_row(_C2, sq_despesa="500002", nr_documento="NF002",
                    valor="150,00", fornecedor_doc="33333333000100",
                    fornecedor_nome="POSTO GO", ds_despesa="Combustível"),
    _contratada_row(_SP, sq_despesa="500099", nr_documento="NF099",
                    valor="5000,00", fornecedor_doc="44444444000100",
                    fornecedor_nome="IMOBILIARIA SP", ue="SP"),
]

_BENS_ROWS = [
    _bem_row(_C1, ue="GO", tipo="Apartamento",
             ds="Apartamento em Goiânia", valor="250000,00"),
    _bem_row(_C1, ue="GO", tipo="Veículo",
             ds="Carro popular", valor="30000,00"),
    _bem_row(_C2, ue="GO", tipo="Casa",
             ds="Residência principal", valor="450000,00"),
    _bem_row(_C2, ue="GO", tipo="Quotas",
             ds="Quotas empresa", valor="50000,00"),
    _bem_row(_C3, ue="GO", tipo="Terreno",
             ds="Terreno rural", valor="100000,00"),
    _bem_row(_C3, ue="GO", tipo="Veículo",
             ds="Motocicleta", valor="15000,00"),
    _bem_row(_SP, ue="SP", tipo="Imóvel",
             ds="Mansão em SP", valor="9999999,00"),
]


def _dicts_to_csv(fields: list[str], rows: list[dict[str, str]]) -> bytes:
    """Serialize rows to CSV bytes em latin-1/; (como TSE publica)."""
    buf = io.StringIO(newline="")
    writer = _csv.DictWriter(
        buf, fieldnames=fields, delimiter=";",
        extrasaction="ignore", lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("latin-1")


def _build_zip_bytes() -> bytes:
    """Monta ZIP mínimo usando o **schema real** TSE 2022."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            f"receitas_candidatos_{_YEAR}_BRASIL.csv",
            _dicts_to_csv(
                _RECEITAS_FIELDS,
                _RECEITAS_ROWS_GO_C1
                + _RECEITAS_ROWS_GO_C2
                + _RECEITAS_ROWS_GO_C3
                + _RECEITAS_ROWS_SP,
            ),
        )
        zf.writestr(
            f"despesas_pagas_candidatos_{_YEAR}_BRASIL.csv",
            _dicts_to_csv(_DESPESAS_PAGAS_FIELDS, _DESPESAS_PAGAS_ROWS),
        )
        zf.writestr(
            f"despesas_contratadas_candidatos_{_YEAR}_BRASIL.csv",
            _dicts_to_csv(
                _DESPESAS_CONTRATADAS_FIELDS, _DESPESAS_CONTRATADAS_ROWS,
            ),
        )
        zf.writestr(
            f"bens_candidato_{_YEAR}_BRASIL.csv",
            _dicts_to_csv(_BENS_FIELDS, _BENS_ROWS),
        )
    return buf.getvalue()


_ZIP_BYTES = _build_zip_bytes()


def _build_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        # Devolve o mesmo ZIP pra qualquer GET da CDN TSE.
        return httpx.Response(
            200,
            content=_ZIP_BYTES,
            headers={"content-type": "application/zip"},
        )

    return httpx.MockTransport(handler)


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


@pytest.fixture()
def pipeline(
    archival_root: Path,  # noqa: ARG001 — fixture só ativa o env var
    tmp_path: Path,
) -> TsePrestacaoContasGoPipeline:
    driver = MagicMock()
    transport = _build_transport()

    def factory() -> httpx.Client:
        return httpx.Client(transport=transport, follow_redirects=True)

    return TsePrestacaoContasGoPipeline(
        driver=driver,
        data_dir=str(tmp_path),
        http_client_factory=factory,
        year=_YEAR,
        uf="GO",
    )


# ---------------------------------------------------------------------------
# Metadata / registry
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert TsePrestacaoContasGoPipeline.name == "tse_prestacao_contas_go"

    def test_source_id(self) -> None:
        assert TsePrestacaoContasGoPipeline.source_id == _SOURCE_ID
        assert _SOURCE_ID == "tse_prestacao_contas"

    def test_year_parametrizable(self, archival_root: Path) -> None:  # noqa: ARG002
        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            year=2026,
        )
        assert p.year == 2026
        # URL points to 2026 zip.
        assert str(2026) in p._zip_url


# ---------------------------------------------------------------------------
# _classify_origem — unit test dos buckets
# ---------------------------------------------------------------------------


class TestClassifyOrigem:
    @pytest.mark.parametrize(("raw", "expected"), [
        ("Recursos de partido político", "partido"),
        ("Fundo Partidário", "partido"),
        ("Fundo Especial de Financiamento de Campanha (FEFC)", "partido"),
        ("Recursos próprios", "proprios"),
        ("Autofinanciamento", "proprios"),
        ("Recursos de pessoas físicas", "pessoa_fisica"),
        ("Recursos de pessoa jurídica", "pessoa_juridica"),
        ("Recursos de financiamento coletivo", "fin_coletivo"),
        ("Vaquinha online", "fin_coletivo"),
        ("", "outros"),
        ("Categoria desconhecida", "outros"),
    ])
    def test_mapping(self, raw: str, expected: str) -> None:
        assert _classify_origem(raw) == expected


# ---------------------------------------------------------------------------
# Extract — download + archival + UF filter
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_filters_uf_go(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        # Só rows GO — SP é descartado.
        assert all(
            (r.get("SG_UF") or "").upper() == "GO"
            for r in pipeline._receitas_raw
        )
        assert all(
            (r.get("SG_UF") or "").upper() == "GO"
            for r in pipeline._despesas_raw
        )
        assert all(
            (r.get("SG_UF") or "").upper() == "GO"
            for r in pipeline._despesas_contratadas_raw
        )
        assert all(
            (r.get("SG_UF") or "").upper() == "GO"
            for r in pipeline._bens_raw
        )

    def test_extract_reads_all_go_rows(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        # 3 candidatos × 5 receitas cada = 15
        assert len(pipeline._receitas_raw) == 15
        # 2 despesas pagas GO (SP filtrado)
        assert len(pipeline._despesas_raw) == 2
        # 2 despesas contratadas GO (SP filtrado)
        assert len(pipeline._despesas_contratadas_raw) == 2
        # 3 candidatos × 2 bens cada = 6
        assert len(pipeline._bens_raw) == 6

    def test_extract_archives_zip(
        self,
        pipeline: TsePrestacaoContasGoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        # Snapshot URI setado
        assert pipeline._snapshot_uri
        assert pipeline._snapshot_uri.startswith(f"{_SOURCE_ID}/")
        # Arquivo gravado no archival (extensão é .bin pois ``application/
        # zip`` não mapeia em ``_CONTENT_TYPE_EXTENSIONS`` — conteúdo é
        # idêntico, só muda o hint no nome).
        snapshot_dir = archival_root / _SOURCE_ID
        files = [p for p in snapshot_dir.rglob("*") if p.is_file()]
        assert len(files) == 1
        # Conteúdo preservado byte-a-byte.
        assert files[0].read_bytes() == _ZIP_BYTES

    def test_extract_uses_cached_zip_when_present(
        self,
        tmp_path: Path,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """Se ZIP já existe em ``{data_dir}/tse_prestacao_contas/...``,
        pipeline usa ele em vez de baixar."""
        cache_dir = tmp_path / "tse_prestacao_contas"
        cache_dir.mkdir(parents=True)
        (cache_dir / f"prestacao_de_contas_eleitorais_candidatos_{_YEAR}.zip").write_bytes(
            _ZIP_BYTES,
        )

        def factory() -> httpx.Client:
            # Transport que SEMPRE retorna 500 — se o pipeline tentar baixar
            # o teste quebra.
            return httpx.Client(
                transport=httpx.MockTransport(
                    lambda r: httpx.Response(500),  # noqa: ARG005
                ),
            )

        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir=str(tmp_path),
            http_client_factory=factory,
            year=_YEAR,
        )
        p.extract()
        assert len(p._receitas_raw) == 15


# ---------------------------------------------------------------------------
# Transform — properties, buckets, LGPD, provenance
# ---------------------------------------------------------------------------


class TestTransform:
    def test_produces_three_go_candidates(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.persons) == 3
        cpfs = {p["cpf"] for p in pipeline.persons}
        assert cpfs == {
            "111.444.777-35",
            "529.982.247-25",
            "222.333.444-56",
        }

    def test_person_has_expected_tse_properties(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # Candidato 1 breakdown: 1000 partido + 500,50 proprios +
        # 200 pf + 300 fin_coletivo + 50 outros = 2050,50 total
        c1 = next(p for p in pipeline.persons if p["cpf"] == "111.444.777-35")
        assert c1["total_tse_2022"] == pytest.approx(2050.50)
        assert c1["tse_2022_partido"] == pytest.approx(1000.00)
        assert c1["tse_2022_proprios"] == pytest.approx(500.50)
        assert c1["tse_2022_pessoa_fisica"] == pytest.approx(200.00)
        assert c1["tse_2022_fin_coletivo"] == pytest.approx(300.00)
        assert c1["tse_2022_outros"] == pytest.approx(50.00)
        # Despesa paga (resolvida via SQ_PRESTADOR_CONTAS=3000000001)
        assert c1["total_despesas_tse_2022"] == pytest.approx(300.00)
        # Patrimônio = 250000 + 30000 = 280000
        assert c1["patrimonio_declarado"] == pytest.approx(280000.00)
        assert c1["patrimonio_ano"] == 2022
        assert c1["uf"] == "GO"
        assert c1["name"]  # preenchido
        assert c1["numero_candidato"] == "GO0001"

    def test_keys_match_flask_gerar_validacao_tse(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """Guard contra regressão das keys consumidas pelo Flask.

        ``backend/app.py::gerar_validacao_tse`` lê as chaves literais:
        ``total_tse_2022``, ``tse_2022_partido``,
        ``tse_2022_pessoa_fisica``, ``tse_2022_proprios``,
        ``tse_2022_fin_coletivo``. Quebrar essas chaves quebra
        ``/politico``.
        """
        pipeline.extract()
        pipeline.transform()
        required = {
            "total_tse_2022",
            "tse_2022_partido",
            "tse_2022_pessoa_fisica",
            "tse_2022_proprios",
            "tse_2022_fin_coletivo",
            "patrimonio_declarado",
            "patrimonio_ano",
        }
        for p in pipeline.persons:
            missing = required - p.keys()
            assert not missing, f"missing {missing} em {p['cpf']}"

    def test_provenance_on_every_person(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for p in pipeline.persons:
            assert p["source_id"] == _SOURCE_ID
            assert p["source_record_id"].endswith(":2022")
            assert p["source_url"].startswith(
                "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/",
            )
            assert p["source_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
            assert p["run_id"].startswith(f"{_SOURCE_ID}_")
            assert p["ingested_at"].startswith("20")

    def test_donation_nodes_produced(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # 5 + 5 + 5 = 15 doações (todas GO — SP foi filtrado).
        assert len(pipeline.donations) == 15
        # Tipagem do doador
        pf = [d for d in pipeline.donations if d["doador_tipo"] == "pf"]
        pj = [d for d in pipeline.donations if d["doador_tipo"] == "pj"]
        assert pf, "esperava pelo menos uma doação PF"
        assert pj, "esperava pelo menos uma doação PJ"

    def test_cpf_doador_is_masked(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """LGPD — CPF do doador PF nunca aparece cru nos dicts produzidos."""
        pipeline.extract()
        pipeline.transform()
        # Coletar valores que possam conter CPF cru
        raw_cpfs_expostos = {
            "22233344456",  # ZE DA SILVA (doador de C1)
            "55566677788",
            "55566677789",
            "55566677790",
            "55566677791",
            "55566677792",
        }
        for donation in pipeline.donations:
            if donation["doador_tipo"] != "pf":
                continue
            doador_id = str(donation["doador_id"])
            for crua in raw_cpfs_expostos:
                # doador_id não pode ser o CPF completo sem máscara.
                assert doador_id != crua
                # O valor armazenado tem que ter "*" (máscara aplicada).
            assert "*" in doador_id
        # Rels também
        for rel in pipeline.donation_rels:
            if rel.get("doador_tipo") != "pf":
                continue
            src = str(rel["source_key"])
            for crua in raw_cpfs_expostos:
                assert src != crua

    def test_donation_provenance_with_snapshot(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for d in pipeline.donations:
            assert d["source_id"] == _SOURCE_ID
            assert d["source_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
            assert d["source_url"].startswith("https://cdn.tse.jus.br")

    def test_committees_produced(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """Fase 1 do TODO 07 — comites de campanha CNAE 9492-8/00.

        Espera 1 commitee por candidato GO (3 candidatos -> 3 CNPJs unicos),
        dedupe por CNPJ mesmo com N linhas de receitas no mesmo comite, e
        candidatos SP ficam de fora pelo filtro UF.
        """
        pipeline.extract()
        pipeline.transform()

        cnpjs_go = {_C1["cnpj_prestador"], _C2["cnpj_prestador"], _C3["cnpj_prestador"]}
        cnpjs_produced = {c["cnpj"] for c in pipeline.committees}

        assert cnpjs_produced == cnpjs_go
        assert _SP["cnpj_prestador"] not in cnpjs_produced  # filtro UF aplica

        # Campos estruturais presentes em todo committee + provenance.
        by_cnpj = {c["cnpj"]: c for c in pipeline.committees}
        for cand in (_C1, _C2, _C3):
            c = by_cnpj[cand["cnpj_prestador"]]
            assert c["cargo_candidatura"] == cand["cargo"]
            assert c["ano_eleicao"] == 2022
            assert c["nome_candidato"] == cand["nome"]
            assert c["source_id"] == _SOURCE_ID
            assert c["source_snapshot_uri"].startswith(f"{_SOURCE_ID}/")

    def test_expenses_produced(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # Só as 2 despesas GO
        assert len(pipeline.expenses) == 2
        for e in pipeline.expenses:
            assert e["uf"] == "GO"
            assert e["valor"] > 0
            assert e["ano"] == 2022

    def test_expense_rels_route_to_person(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.expense_rels) == 2
        for rel in pipeline.expense_rels:
            # source_key é CPF formatado do candidato.
            assert "." in rel["source_key"]
            assert "-" in rel["source_key"]

    def test_despesas_resolve_candidate_via_sq_prestador_contas(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """Regressão bug 2026-04-18.

        O schema TSE 2022+ de ``despesas_pagas_candidatos_{year}_BRASIL.csv``
        **não publica** ``NR_CPF_CANDIDATO`` — só ``SQ_PRESTADOR_CONTAS``.
        A fixture atual reflete esse schema real (sem CPF nas pagas); se o
        transform quebrar e voltar a filtrar por ``len(cpf_digits) != 11``
        direto na row da pagas, todas as despesas são descartadas e
        ``total_despesas_tse_2022`` vira 0.0 pra todo candidato.
        """
        pipeline.extract()
        pipeline.transform()
        # Confirma que nenhuma row de despesas pagas tem CPF explícito
        # (garantia do schema real).
        for row in pipeline._despesas_raw:
            assert not row.get("NR_CPF_CANDIDATO")
            assert not row.get("CPF_CANDIDATO")
            # SQ_PRESTADOR_CONTAS é o único link pro candidato.
            assert row.get("SQ_PRESTADOR_CONTAS")
        # Resolução bem-sucedida: os CPFs dos candidatos GO aparecem nos
        # rels mesmo sem estar na row.
        cpfs_nas_rels = {r["source_key"] for r in pipeline.expense_rels}
        assert "111.444.777-35" in cpfs_nas_rels
        assert "529.982.247-25" in cpfs_nas_rels
        # total_despesas_tse_2022 > 0 no Person correspondente.
        c1 = next(p for p in pipeline.persons if p["cpf"] == "111.444.777-35")
        c2 = next(p for p in pipeline.persons if p["cpf"] == "529.982.247-25")
        assert c1["total_despesas_tse_2022"] > 0
        assert c2["total_despesas_tse_2022"] > 0

    def test_fornecedor_enriched_from_contratadas(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """Despesas pagas não trazem fornecedor em 2022+; transform hidrata
        via mapa (SQ_PRESTADOR_CONTAS, SQ_DESPESA) construído a partir de
        ``despesas_contratadas_candidatos_{year}_BRASIL.csv``."""
        pipeline.extract()
        pipeline.transform()
        fornecedores = {
            (e["fornecedor_documento"], e["fornecedor_nome"])
            for e in pipeline.expenses
        }
        assert ("22222222000100", "GRAFICA GO") in fornecedores
        assert ("33333333000100", "POSTO GO") in fornecedores

    def test_guard_raises_when_strict_and_zero_expenses(
        self,
        archival_root: Path,  # noqa: ARG002
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Se ``BRACC_STRICT_TRANSFORM=1`` e 100% das despesas são
        descartadas (schema drift), transform levanta ``RuntimeError``.

        Protege contra o gap silencioso que deixou o teto_service
        reportando "0% utilizado" em 2026-04-18.
        """
        monkeypatch.setenv("BRACC_STRICT_TRANSFORM", "1")

        # Build fixture com schema que NÃO bate: remove SQ_PRESTADOR_CONTAS
        # da row de despesas pagas (simula coluna renomeada).
        pagas_quebradas = [dict(r) for r in _DESPESAS_PAGAS_ROWS[:2]]
        for r in pagas_quebradas:
            r["SQ_PRESTADOR_CONTAS"] = ""  # rompe o lookup

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                f"receitas_candidatos_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(
                    _RECEITAS_FIELDS,
                    _RECEITAS_ROWS_GO_C1 + _RECEITAS_ROWS_GO_C2,
                ),
            )
            zf.writestr(
                f"despesas_pagas_candidatos_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(_DESPESAS_PAGAS_FIELDS, pagas_quebradas),
            )
            zf.writestr(
                f"despesas_contratadas_candidatos_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(
                    _DESPESAS_CONTRATADAS_FIELDS,
                    [],  # também vazio pra não salvar via fallback
                ),
            )
            zf.writestr(
                f"bens_candidato_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(_BENS_FIELDS, _BENS_ROWS[:2]),
            )
        zip_bytes = buf.getvalue()

        def factory() -> httpx.Client:
            return httpx.Client(transport=httpx.MockTransport(
                lambda _req: httpx.Response(
                    200, content=zip_bytes,
                    headers={"content-type": "application/zip"},
                ),
            ))

        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir=str(tmp_path),
            http_client_factory=factory,
            year=_YEAR,
            uf="GO",
        )
        p.extract()
        with pytest.raises(RuntimeError, match="0 expenses"):
            p.transform()

    def test_guard_logs_without_strict_env(
        self,
        archival_root: Path,  # noqa: ARG002
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Sem ``BRACC_STRICT_TRANSFORM=1``, o guard apenas loga ERROR —
        não quebra o pipeline (comportamento default, preserva UX dev)."""
        monkeypatch.delenv("BRACC_STRICT_TRANSFORM", raising=False)

        # Zip com despesas pagas órfãs de SQ_PRESTADOR_CONTAS.
        pagas_quebradas = [dict(r) for r in _DESPESAS_PAGAS_ROWS[:1]]
        pagas_quebradas[0]["SQ_PRESTADOR_CONTAS"] = ""

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                f"receitas_candidatos_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(_RECEITAS_FIELDS, _RECEITAS_ROWS_GO_C1),
            )
            zf.writestr(
                f"despesas_pagas_candidatos_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(_DESPESAS_PAGAS_FIELDS, pagas_quebradas),
            )
            zf.writestr(
                f"despesas_contratadas_candidatos_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(_DESPESAS_CONTRATADAS_FIELDS, []),
            )
            zf.writestr(
                f"bens_candidato_{_YEAR}_BRASIL.csv",
                _dicts_to_csv(_BENS_FIELDS, _BENS_ROWS[:1]),
            )
        zip_bytes = buf.getvalue()

        def factory() -> httpx.Client:
            return httpx.Client(transport=httpx.MockTransport(
                lambda _req: httpx.Response(
                    200, content=zip_bytes,
                    headers={"content-type": "application/zip"},
                ),
            ))

        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir=str(tmp_path),
            http_client_factory=factory,
            year=_YEAR,
            uf="GO",
        )
        p.extract()
        import logging as _logging
        with caplog.at_level(_logging.ERROR):
            p.transform()  # não levanta
        assert any(
            "0 expenses" in rec.message and rec.levelname == "ERROR"
            for rec in caplog.records
        )


# ---------------------------------------------------------------------------
# Load — smoke (mock driver)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_noop_without_persons(self, archival_root: Path) -> None:  # noqa: ARG002
        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
        )
        # não extraímos — persons está vazio
        p.load()
        mock_driver(p).session.assert_not_called()

    def test_load_hits_session_multiple_times(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        # Person + donation nodes + donation rels + expense nodes +
        # expense rels — mínimo 3 chamadas ao session.
        assert mock_session(pipeline).run.call_count >= 3

    def test_load_marks_committees_with_cnae(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """Fase 1 do TODO 07 — Cypher MERGE carimba Company com CNAE 9492-8/00."""
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

        session = mock_session(pipeline)
        committee_calls = [
            call for call in session.run.call_args_list
            if "tipo_entidade = 'comite_campanha'" in str(call)
        ]
        assert committee_calls, "Cypher MERGE de committee nao foi disparado"
        query_str = str(committee_calls[0][0][0])
        assert "MERGE (c:Company {cnpj: row.cnpj})" in query_str
        assert "c.cnae_principal = '9492-8/00'" in query_str
        assert "c.cargo_candidatura = row.cargo_candidatura" in query_str
        assert "c.ano_eleicao = row.ano_eleicao" in query_str


class TestDonatedAt:
    """DT_RECEITA vira donated_at (ISO) na donation_rel + SET cypher."""

    def test_donation_rels_carry_iso_date(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # Fixture default usa dt_receita="15/09/2022" em todos os rows.
        assert pipeline.donation_rels
        for rel in pipeline.donation_rels:
            assert rel["donated_at"] == "2022-09-15"

    def test_donation_node_carries_iso_date(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert pipeline.donations
        for d in pipeline.donations:
            assert d["donated_at"] == "2022-09-15"

    def test_load_sets_donated_at_in_merge(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        doou_calls = [
            call for call in session.run.call_args_list
            if "[r:DOOU" in str(call)
        ]
        assert doou_calls
        query_str = str(doou_calls[0][0][0])
        assert "r.donated_at = row.donated_at" in query_str


# ---------------------------------------------------------------------------
# Masked CPF (TSE 2024+) — Person vai pra bucket sq-keyed, sem surrogate no cpf
# ---------------------------------------------------------------------------

_C_MASKED = {
    "sq_prestador": "5000000001",
    "sq_candidato": "GO9024001",
    "nr_candidato": "55024",
    "cpf": "-4",  # _MASKED_CPF_SENTINEL — TSE 2024 publica CPFs mascarados
    "nome": "CANDIDATO MASCARADO",
    "cargo": "Vereador",
    "cd_cargo": "13",
    "partido": "10",
    "sg_partido": "REP",
    "nm_partido": "Republicanos",
    "cnpj_prestador": "47574000000100",
}


class TestMaskedCpfDoesNotContaminateNode:
    """Regressão: ``cpf_formatted`` (dict key em ``by_cpf``) passava
    "sq:<X>" pro node quando CPF vinha mascarado, criando :Person
    {cpf:"sq:X"} em paralelo ao :Person {sq_candidato:X} do pipeline
    tse_bens (19k pares duplicados observados no grafo). O fix trocou
    a iteração pra usar ``entry["cpf"]``, que carrega ""/empty pro
    caso mascarado e roteia o row via _persons_nocpf → MERGE por
    sq_candidato.
    """

    @pytest.fixture()
    def pipeline_with_masked_row(
        self,
        archival_root: Path,  # noqa: ARG002
        tmp_path: Path,
    ) -> TsePrestacaoContasGoPipeline:
        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(), data_dir=str(tmp_path), year=2024, uf="GO",
        )
        # Bypass extract() — 1 row mascarado basta pra exercitar o path.
        p._receitas_raw = [
            _receita_row(
                _C_MASKED, ue="GO", sq_receita="RM0001",
                origem="Recursos próprios", valor="250,00",
                doador_cpf_cnpj="-4", doador_nome=_C_MASKED["nome"],
            ),
        ]
        p._despesas_raw = []
        p._despesas_contratadas_raw = []
        p._bens_raw = []
        p._zip_url = "https://example/fake.zip"
        p._snapshot_uri = "tse_prestacao_contas/fake/snapshot.bin"
        return p

    def test_person_cpf_is_empty_not_sq_surrogate(
        self, pipeline_with_masked_row: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline_with_masked_row.transform()
        # Row mascarado NÃO entra em self.persons (filtro `p.get("cpf")`
        # precisa cair fora — se caísse dentro, vinha com cpf="sq:X" e
        # seria MERGEd como node paralelo).
        assert pipeline_with_masked_row.persons == []
        nocpf = pipeline_with_masked_row._persons_nocpf
        assert len(nocpf) == 1
        # O cpf foi pra "" (sem surrogate). numero_candidato carrega o sq
        # pra o load() poder MERGEar por sq_candidato.
        assert nocpf[0].get("cpf", "") == ""
        assert nocpf[0]["numero_candidato"] == _C_MASKED["sq_candidato"]
        assert nocpf[0]["name"] == _C_MASKED["nome"]

    def test_load_merges_masked_by_sq_candidato_not_cpf(
        self, pipeline_with_masked_row: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline_with_masked_row.transform()
        pipeline_with_masked_row.load()
        session = mock_session(pipeline_with_masked_row)
        person_calls = [
            call for call in session.run.call_args_list
            if "MERGE (n:Person" in str(call)
        ]
        # Exatamente 1 call pro batch nocpf; sem real-CPF rows no fixture
        # não há call keyed por :Person{cpf:...}.
        assert len(person_calls) == 1
        query_str = str(person_calls[0][0][0])
        assert "MERGE (n:Person {sq_candidato: row.sq_candidato})" in query_str
        # Props enviadas no batch não devem carregar cpf="sq:X" nem cpf="".
        # loader.py invoca ``session.run(query, {"rows": batch})`` — args
        # posicionais, não kwargs.
        call_args = person_calls[0][0]
        assert len(call_args) == 2
        rows = call_args[1]["rows"]
        assert len(rows) == 1
        assert "cpf" not in rows[0]
        assert rows[0]["sq_candidato"] == _C_MASKED["sq_candidato"]

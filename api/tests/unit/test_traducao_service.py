"""Tests para bracc.services.traducao_service.

Valida que cada dict de tradução tem as chaves esperadas e que as
funções aplicam substring match (case-insensitive, acento-insensitive
quando aplicável).
"""

from __future__ import annotations

import pytest

from bracc.services.traducao_service import (
    CARGOS,
    FUNCOES_EMENDA,
    RELACOES,
    TIPOS_DESPESA,
    TIPOS_EMENDA,
    _sem_acento,
    traduzir_cargo,
    traduzir_despesa,
    traduzir_funcao_emenda,
    traduzir_relacao,
    traduzir_tipo_emenda,
)


class TestDictsBasico:
    def test_cargos_tem_pep_roles(self) -> None:
        for chave in ("deputado federal", "senador", "governador", "vereador"):
            assert chave in CARGOS

    def test_funcoes_emenda_tem_categorias_principais(self) -> None:
        for chave in ("saude", "educacao", "seguranca publica", "habitacao"):
            assert chave in FUNCOES_EMENDA

    def test_tipos_despesa_tem_ceap_principais(self) -> None:
        for chave in ("combustivel", "telefonia", "hospedagem", "alimentacao"):
            assert chave in TIPOS_DESPESA

    def test_tipos_emenda_cinco_tipos(self) -> None:
        assert set(TIPOS_EMENDA.keys()) == {
            "individual", "bancada", "comissao", "relator", "pix",
        }

    def test_relacoes_tem_bracc_rels_principais(self) -> None:
        for chave in ("SOCIO_DE", "DOOU", "CONJUGE_DE", "AUTOR_EMENDA"):
            assert chave in RELACOES


class TestTraduzirCargo:
    def test_exato(self) -> None:
        assert traduzir_cargo("deputado federal") == "Deputado(a) Federal"

    def test_case_insensitive(self) -> None:
        assert traduzir_cargo("DEPUTADO FEDERAL") == "Deputado(a) Federal"

    def test_substring(self) -> None:
        # "deputado federal de algum lugar" deve casar com "deputado federal"
        assert traduzir_cargo("deputado federal por GO") == "Deputado(a) Federal"

    def test_sem_match_retorna_title(self) -> None:
        assert traduzir_cargo("assessor parlamentar") == "Assessor Parlamentar"

    def test_none_retorna_vazio(self) -> None:
        assert traduzir_cargo(None) == ""

    def test_vazio_retorna_vazio(self) -> None:
        assert traduzir_cargo("") == ""


class TestTraduzirFuncaoEmenda:
    def test_saude(self) -> None:
        assert traduzir_funcao_emenda("saude") == "Saude publica"

    def test_none_retorna_default(self) -> None:
        assert traduzir_funcao_emenda(None) == "Nao informada"

    def test_vazio_retorna_default(self) -> None:
        assert traduzir_funcao_emenda("") == "Nao informada"

    def test_sem_match_title(self) -> None:
        assert traduzir_funcao_emenda("xyz desconhecida") == "Xyz Desconhecida"


class TestTraduzirTipoEmenda:
    def test_relator(self) -> None:
        assert "orcamento secreto" in traduzir_tipo_emenda("relator").lower()

    def test_individual(self) -> None:
        assert "individual" in traduzir_tipo_emenda("individual").lower()

    def test_none(self) -> None:
        assert traduzir_tipo_emenda(None) == "Nao informado"


class TestTraduzirDespesa:
    def test_combustivel(self) -> None:
        assert traduzir_despesa("combustivel") == "Combustivel"

    def test_com_acento(self) -> None:
        """Acento-insensitive: 'combustíveis e lubrificantes' casa."""
        assert traduzir_despesa("Combustíveis e Lubrificantes") == "Combustivel"

    def test_passagem_aerea_com_acento(self) -> None:
        assert traduzir_despesa("PASSAGEM AÉREA") == "Passagem aerea"

    def test_none(self) -> None:
        assert traduzir_despesa(None) == "Despesa nao especificada"

    def test_desconhecida(self) -> None:
        assert traduzir_despesa("categoria xyz") == "Categoria Xyz"


class TestTraduzirRelacao:
    @pytest.mark.parametrize(
        ("rel", "esperado_contem"),
        [
            ("SOCIO_DE", "Socio"),
            ("DOOU", "Doou"),
            ("CONJUGE_DE", "Conjuge"),
            ("PARENTE_DE", "Parente"),
            ("AUTOR_EMENDA", "emenda"),
        ],
    )
    def test_relacoes_conhecidas(self, rel: str, esperado_contem: str) -> None:
        assert esperado_contem.lower() in traduzir_relacao(rel).lower()

    def test_rel_desconhecida_fallback(self) -> None:
        assert traduzir_relacao("MINHA_REL_NOVA") == "Minha rel nova"


class TestSemAcento:
    def test_vogais_acentuadas(self) -> None:
        assert _sem_acento("saúde") == "saude"
        assert _sem_acento("educação") == "educacao"

    def test_sem_acentos_passa_direto(self) -> None:
        assert _sem_acento("alimentacao") == "alimentacao"

    def test_vazio(self) -> None:
        assert _sem_acento("") == ""

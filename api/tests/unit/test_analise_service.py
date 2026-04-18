"""Tests para bracc.services.analise_service."""

from __future__ import annotations

from bracc.services.analise_service import (
    FAIXA_ELEVADO,
    FAIXA_NORMAL,
    REFERENCIA_CIDADA_MENSAL,
    analisar_despesas_vs_cidadao,
    gerar_resumo_politico,
)


class TestFaixas:
    def test_faixas_corretas(self) -> None:
        assert FAIXA_NORMAL == 3
        assert FAIXA_ELEVADO == 8

    def test_referencia_tem_categorias_principais(self) -> None:
        for chave in ("combustivel", "telefonia", "alimentacao", "hospedagem"):
            assert chave in REFERENCIA_CIDADA_MENSAL


class TestAnalisarDespesasVsCidadao:
    def test_vazio(self) -> None:
        resultado = analisar_despesas_vs_cidadao([])
        assert resultado == {"comparacoes": [], "alertas": [], "resumo": ""}

    def test_classificacao_normal(self) -> None:
        """Combustivel referencia 415/mes. 24 meses * 415 * 2x = 19920 → normal."""
        despesas = [{"tipoDespesa": "COMBUSTIVEIS E LUBRIFICANTES", "valorLiquido": 19_920}]
        resultado = analisar_despesas_vs_cidadao(despesas, num_meses=24)
        assert len(resultado["comparacoes"]) == 1
        assert resultado["comparacoes"][0]["classificacao"] == "normal"
        assert resultado["alertas"] == []

    def test_classificacao_elevado(self) -> None:
        """Telefonia referencia 55/mes. 24 * 55 * 5x = 6600 → elevado (5x > 3x)."""
        despesas = [{"tipoDespesa": "TELEFONIA", "valorLiquido": 6_600}]
        resultado = analisar_despesas_vs_cidadao(despesas, num_meses=24)
        assert resultado["comparacoes"][0]["classificacao"] == "elevado"
        assert any(a["tipo"] == "atencao" for a in resultado["alertas"])

    def test_classificacao_abusivo(self) -> None:
        """Telefonia 24 * 55 * 15x = 19800 → abusivo (15x > 8x)."""
        despesas = [{"tipoDespesa": "TELEFONIA", "valorLiquido": 19_800}]
        resultado = analisar_despesas_vs_cidadao(despesas, num_meses=24)
        assert resultado["comparacoes"][0]["classificacao"] == "abusivo"
        assert any(a["tipo"] == "grave" for a in resultado["alertas"])

    def test_categoria_que_cidadao_nao_tem_gera_abusivo(self) -> None:
        """Segurança: referência 0 — qualquer gasto vira abusivo."""
        despesas = [{"tipoDespesa": "SERVICOS DE SEGURANCA", "valorLiquido": 50_000}]
        resultado = analisar_despesas_vs_cidadao(despesas, num_meses=24)
        assert resultado["comparacoes"][0]["classificacao"] == "abusivo"
        assert resultado["comparacoes"][0]["razao"] is None
        assert any("cidadao comum nao tem" in a["texto"] for a in resultado["alertas"])

    def test_resumo_reflete_severidades(self) -> None:
        despesas = [
            {"tipoDespesa": "TELEFONIA", "valorLiquido": 19_800},     # abusivo
            {"tipoDespesa": "COMBUSTIVEL", "valorLiquido": 19_920},   # normal
        ]
        resultado = analisar_despesas_vs_cidadao(despesas, num_meses=24)
        assert "ABUSIVO" in resultado["resumo"]

    def test_resumo_todos_normais(self) -> None:
        despesas = [{"tipoDespesa": "COMBUSTIVEL", "valorLiquido": 19_920}]
        resultado = analisar_despesas_vs_cidadao(despesas, num_meses=24)
        assert "aceitaveis" in resultado["resumo"]

    def test_categoria_desconhecida_usa_fallback(self) -> None:
        """Categoria fora do dict usa ref ~R$185/mes; 24*185*2 = 8880 → normal."""
        despesas = [{"tipoDespesa": "CATEGORIA INEXISTENTE XYZ", "valorLiquido": 8_880}]
        resultado = analisar_despesas_vs_cidadao(despesas, num_meses=24)
        assert resultado["comparacoes"][0]["classificacao"] == "normal"

    def test_comparacao_tem_campos_obrigatorios(self) -> None:
        despesas = [{"tipoDespesa": "COMBUSTIVEL", "valorLiquido": 10_000}]
        resultado = analisar_despesas_vs_cidadao(despesas)
        comp = resultado["comparacoes"][0]
        for campo in (
            "categoria",
            "categoria_original",
            "total_politico",
            "total_politico_fmt",
            "media_mensal_politico",
            "media_mensal_politico_fmt",
            "referencia_cidadao",
            "referencia_cidadao_fmt",
            "razao",
            "razao_texto",
            "classificacao",
        ):
            assert campo in comp


class TestGerarResumoPolitico:
    def test_resumo_completo(self) -> None:
        resumo = gerar_resumo_politico(
            nome="joao silva",
            cargo="deputado federal",
            patrimonio=1_500_000,
            num_emendas=5,
            total_emendas=2_000_000,
            num_conexoes=12,
        )
        assert "Joao Silva" in resumo
        assert "Deputado(a) Federal" in resumo
        assert "Patrimonio" in resumo
        assert "5 emenda" in resumo
        assert "12 conexao" in resumo

    def test_sem_cargo(self) -> None:
        resumo = gerar_resumo_politico("Ana", None, None, 0, 0, 0)
        assert "politico" in resumo.lower()

    def test_sem_patrimonio_nao_cita(self) -> None:
        resumo = gerar_resumo_politico("Ana", "vereador", None, 0, 0, 0)
        assert "Patrimonio" not in resumo

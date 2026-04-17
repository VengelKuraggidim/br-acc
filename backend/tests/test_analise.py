"""Testes do modulo de analise inteligente (analise.py)."""

from __future__ import annotations

import sys
from pathlib import Path

# Garantir que o backend esta no path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analise import (
    _sem_acento,
    analisar_conexoes,
    analisar_despesas_gabinete,
    analisar_despesas_vs_media,
    analisar_emendas,
    analisar_picos_mensais,
    analisar_patrimonio,
    gerar_resumo_politico,
    traduzir_cargo,
    traduzir_despesa,
    traduzir_funcao_emenda,
    traduzir_tipo_emenda,
)


# === traduzir_cargo ===


class TestTraduzirCargo:
    def test_deputado_federal(self):
        assert traduzir_cargo("deputado federal") == "Deputado(a) Federal"

    def test_senador_maiusculo(self):
        assert traduzir_cargo("SENADOR") == "Senador(a)"

    def test_cargo_desconhecido_vira_title(self):
        assert traduzir_cargo("ministro") == "Ministro"

    def test_cargo_vazio(self):
        assert traduzir_cargo("") == ""

    def test_cargo_none(self):
        # A funcao recebe str, mas deve lidar com empty
        assert traduzir_cargo("") == ""


# === traduzir_funcao_emenda ===


class TestTraduzirFuncaoEmenda:
    def test_saude(self):
        assert traduzir_funcao_emenda("Saude") == "Saude publica"

    def test_urbanismo(self):
        assert traduzir_funcao_emenda("URBANISMO") == "Obras e melhorias urbanas"

    def test_vazio(self):
        assert traduzir_funcao_emenda("") == "Nao informada"

    def test_desconhecido_vira_title(self):
        assert traduzir_funcao_emenda("outra coisa") == "Outra Coisa"


# === traduzir_tipo_emenda ===


class TestTraduzirTipoEmenda:
    def test_individual(self):
        result = traduzir_tipo_emenda("Individual")
        assert "individual" in result.lower() or "unico parlamentar" in result.lower()

    def test_relator_orcamento_secreto(self):
        result = traduzir_tipo_emenda("Relator")
        assert "relator" in result.lower()
        assert "secreto" in result.lower()

    def test_vazio(self):
        assert traduzir_tipo_emenda("") == "Nao informado"


# === traduzir_despesa (com acentos) ===


class TestTraduzirDespesa:
    def test_combustivel_com_acento(self):
        assert traduzir_despesa("COMBUSTÍVEIS E LUBRIFICANTES.") == "Combustivel"

    def test_divulgacao_com_acento(self):
        assert traduzir_despesa("DIVULGAÇÃO DA ATIVIDADE PARLAMENTAR.") == "Divulgacao/propaganda"

    def test_manutencao_escritorio_com_acento(self):
        assert traduzir_despesa("MANUTENÇÃO DE ESCRITÓRIO DE APOIO À ATIVIDADE PARLAMENTAR") == "Escritorio"

    def test_passagem_aerea_sigepa(self):
        result = traduzir_despesa("PASSAGEM AÉREA - SIGEPA")
        assert result == "Passagem aerea"

    def test_locacao_veiculos_com_acento(self):
        result = traduzir_despesa("LOCAÇÃO OU FRETAMENTO DE VEÍCULOS AUTOMOTORES")
        assert result == "Aluguel de veiculo"

    def test_vazio(self):
        assert traduzir_despesa("") == "Despesa nao especificada"

    def test_desconhecido_vira_title(self):
        assert traduzir_despesa("algo novo") == "Algo Novo"


# === _sem_acento ===


class TestSemAcento:
    def test_acentos_removidos(self):
        assert _sem_acento("ção") == "cao"
        assert _sem_acento("MANUTENÇÃO") == "MANUTENCAO"
        assert _sem_acento("VEÍCULOS") == "VEICULOS"

    def test_sem_acento_inalterado(self):
        assert _sem_acento("teste") == "teste"


# === analisar_patrimonio ===


class TestAnalisarPatrimonio:
    def test_patrimonio_none_retorna_none(self):
        assert analisar_patrimonio(None, "deputado federal") is None

    def test_patrimonio_zero_retorna_none(self):
        assert analisar_patrimonio(0, "deputado federal") is None

    def test_patrimonio_acima_limite_deputado(self):
        result = analisar_patrimonio(15_000_000, "deputado federal")
        assert result is not None
        assert result["tipo"] == "atencao"
        assert "patrimonio" in result["icone"]

    def test_patrimonio_abaixo_limite_deputado(self):
        result = analisar_patrimonio(5_000_000, "deputado federal")
        assert result is None

    def test_patrimonio_muito_alto_sem_cargo(self):
        result = analisar_patrimonio(60_000_000, None)
        assert result is not None
        assert result["tipo"] == "info"

    def test_patrimonio_moderado_sem_cargo_retorna_none(self):
        assert analisar_patrimonio(1_000_000, None) is None


# === analisar_emendas ===


class TestAnalisarEmendas:
    def test_lista_vazia(self):
        assert analisar_emendas([]) == []

    def test_total_acima_10mi(self):
        emendas = [
            {"value_paid": 6_000_000, "municipality": "SP"},
            {"value_paid": 5_000_000, "municipality": "RJ"},
        ]
        alertas = analisar_emendas(emendas)
        assert len(alertas) >= 1
        assert any("emendas parlamentares" in a["texto"].lower() for a in alertas)

    def test_total_abaixo_10mi_sem_alerta(self):
        emendas = [{"value_paid": 1_000_000, "municipality": "SP"}]
        assert analisar_emendas(emendas) == []

    def test_concentracao_municipio(self):
        emendas = [
            {"value_paid": 900_000, "municipality": "GOIANIA"},
            {"value_paid": 200_000, "municipality": "ANAPOLIS"},
        ]
        alertas = analisar_emendas(emendas)
        assert any("concentradas" in a["texto"].lower() for a in alertas)

    def test_emenda_relator(self):
        emendas = [{"value_paid": 500_000, "type": "Relator", "municipality": ""}]
        alertas = analisar_emendas(emendas)
        assert any("relator" in a["texto"].lower() for a in alertas)
        assert any(a["tipo"] == "grave" for a in alertas)


# === analisar_conexoes ===


class TestAnalisarConexoes:
    def test_sem_conexoes(self):
        assert analisar_conexoes([], {}) == []

    def test_familiar_com_empresa(self):
        conexoes = [
            {"target_id": "t1", "relationship_type": "CONJUGE_DE"},
        ]
        entidades = {
            "t1": {"type": "person", "properties": {"name": "MARIA SILVA"}},
        }
        alertas = analisar_conexoes(conexoes, entidades)
        assert len(alertas) >= 1
        assert any("familiar" in a["texto"].lower() for a in alertas)

    def test_entidade_sancionada(self):
        conexoes = [
            {"target_id": "t1", "relationship_type": "VENCEU"},
        ]
        entidades = {
            "t1": {"type": "sanction", "properties": {}},
        }
        alertas = analisar_conexoes(conexoes, entidades)
        assert any(a["tipo"] == "grave" for a in alertas)

    def test_muitas_empresas(self):
        conexoes = [{"target_id": f"c{i}", "relationship_type": "SOCIO_DE"} for i in range(7)]
        entidades = {f"c{i}": {"type": "company", "properties": {}} for i in range(7)}
        alertas = analisar_conexoes(conexoes, entidades)
        assert any("empresas conectadas" in a["texto"].lower() for a in alertas)


# === gerar_resumo_politico ===


class TestGerarResumo:
    def test_resumo_basico(self):
        resumo = gerar_resumo_politico(
            nome="JOAO SILVA",
            cargo="deputado federal",
            patrimonio=5_000_000,
            num_emendas=3,
            total_emendas=1_500_000,
            num_conexoes=10,
        )
        assert "Joao Silva" in resumo
        assert "Deputado(a) Federal" in resumo
        assert "emenda" in resumo.lower()

    def test_resumo_sem_emendas(self):
        resumo = gerar_resumo_politico(
            nome="MARIA", cargo=None, patrimonio=None,
            num_emendas=0, total_emendas=0, num_conexoes=0,
        )
        assert "Maria" in resumo
        assert "politico(a)" in resumo
        assert "emenda" not in resumo.lower()


# === analisar_despesas_gabinete ===


class TestAnalisarDespesasGabinete:
    def test_lista_vazia(self):
        assert analisar_despesas_gabinete([]) == []

    def test_gasto_alto_vs_cota(self):
        # Cota GO mensal = 46980, em 24 meses = 1127520
        # Gasto de 950000 = ~84% da cota -> deve alertar (>80%)
        despesas = [{"valorLiquido": 950_000, "tipoDespesa": "Combustivel"}]
        alertas = analisar_despesas_gabinete(despesas, uf="GO", num_meses=24)
        assert any("cota parlamentar" in a["texto"].lower() for a in alertas)

    def test_gasto_baixo_vs_cota_sem_alerta(self):
        # 200k de 1127k = ~18% -> sem alerta
        despesas = [{"valorLiquido": 200_000, "tipoDespesa": "Combustivel"}]
        alertas = analisar_despesas_gabinete(despesas, uf="GO", num_meses=24)
        assert not any("cota parlamentar" in a["texto"].lower() for a in alertas)

    def test_categoria_dominante(self):
        # 80% em combustivel -> deve alertar (>40%)
        despesas = [
            {"valorLiquido": 80_000, "tipoDespesa": "COMBUSTIVEIS E LUBRIFICANTES"},
            {"valorLiquido": 20_000, "tipoDespesa": "TELEFONIA"},
        ]
        alertas = analisar_despesas_gabinete(despesas, uf=None)
        assert any("concentrados" in a["texto"].lower() for a in alertas)

    def test_categorias_equilibradas_sem_alerta(self):
        despesas = [
            {"valorLiquido": 30_000, "tipoDespesa": "COMBUSTIVEL"},
            {"valorLiquido": 35_000, "tipoDespesa": "TELEFONIA"},
            {"valorLiquido": 35_000, "tipoDespesa": "PASSAGEM AEREA"},
        ]
        alertas = analisar_despesas_gabinete(despesas, uf=None)
        assert not any("concentrados" in a["texto"].lower() for a in alertas)

    def test_sem_uf_nao_gera_alerta_cota(self):
        despesas = [{"valorLiquido": 900_000, "tipoDespesa": "Combustivel"}]
        alertas = analisar_despesas_gabinete(despesas, uf=None)
        assert not any("cota" in a["texto"].lower() for a in alertas)


# === analisar_despesas_vs_media ===


class TestAnalisarDespesasVsMedia:
    def test_gasto_acima_media(self):
        result = analisar_despesas_vs_media(300_000, 150_000, uf="GO")
        assert result is not None
        assert result["tipo"] == "atencao"
        assert "2.0x" in result["texto"]

    def test_gasto_normal_sem_alerta(self):
        result = analisar_despesas_vs_media(150_000, 150_000, uf="GO")
        assert result is None

    def test_gasto_abaixo_media_sem_alerta(self):
        result = analisar_despesas_vs_media(50_000, 150_000, uf="GO")
        assert result is None

    def test_media_zero_sem_alerta(self):
        result = analisar_despesas_vs_media(150_000, 0)
        assert result is None

    def test_sem_uf(self):
        result = analisar_despesas_vs_media(300_000, 100_000)
        assert result is not None
        assert "media dos deputados" in result["texto"]


# === analisar_picos_mensais ===


class TestAnalisarPicosMensais:
    def test_lista_vazia(self):
        assert analisar_picos_mensais([]) == []

    def test_poucos_meses_sem_alerta(self):
        despesas = [
            {"valorLiquido": 50_000, "ano": 2025, "mes": 1},
            {"valorLiquido": 50_000, "ano": 2025, "mes": 2},
        ]
        assert analisar_picos_mensais(despesas) == []

    def test_pico_detectado(self):
        # Media = ~23k, pico em marco = 100k (>2.5x e >20k)
        despesas = [
            {"valorLiquido": 10_000, "ano": 2025, "mes": 1},
            {"valorLiquido": 10_000, "ano": 2025, "mes": 2},
            {"valorLiquido": 100_000, "ano": 2025, "mes": 3},
            {"valorLiquido": 10_000, "ano": 2025, "mes": 4},
            {"valorLiquido": 10_000, "ano": 2025, "mes": 5},
        ]
        alertas = analisar_picos_mensais(despesas)
        assert len(alertas) >= 1
        assert any("pico" in a["texto"].lower() or "Mar" in a["texto"] for a in alertas)

    def test_gastos_uniformes_sem_pico(self):
        despesas = [
            {"valorLiquido": 30_000, "ano": 2025, "mes": m}
            for m in range(1, 7)
        ]
        alertas = analisar_picos_mensais(despesas)
        assert alertas == []

    def test_pico_pequeno_ignorado(self):
        # Pico existe mas valor absoluto < 20k -> ignorado
        despesas = [
            {"valorLiquido": 1_000, "ano": 2025, "mes": 1},
            {"valorLiquido": 1_000, "ano": 2025, "mes": 2},
            {"valorLiquido": 15_000, "ano": 2025, "mes": 3},
            {"valorLiquido": 1_000, "ano": 2025, "mes": 4},
        ]
        alertas = analisar_picos_mensais(despesas)
        assert alertas == []

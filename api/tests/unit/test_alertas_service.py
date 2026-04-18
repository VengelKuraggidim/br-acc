"""Tests para bracc.services.alertas_service.

Testa cada regra de alerta individualmente + orquestração.
"""

from __future__ import annotations

from bracc.services.alertas_service import (
    COTA_CEAP_MENSAL,
    analisar_conexoes,
    analisar_despesas_gabinete,
    analisar_despesas_vs_media,
    analisar_emendas,
    analisar_patrimonio,
    analisar_picos_mensais,
    gerar_alertas_completos,
)


class TestAnalisarPatrimonio:
    def test_patrimonio_none(self) -> None:
        assert analisar_patrimonio(None, "deputado federal") is None

    def test_patrimonio_zero(self) -> None:
        assert analisar_patrimonio(0, "deputado federal") is None

    def test_deputado_federal_acima_limite(self) -> None:
        alerta = analisar_patrimonio(15_000_000, "deputado federal")
        assert alerta is not None
        assert alerta["tipo"] == "atencao"
        assert "Deputado(a) Federal" in alerta["texto"]

    def test_vereador_acima_limite(self) -> None:
        alerta = analisar_patrimonio(3_000_000, "vereador")
        assert alerta is not None
        assert alerta["tipo"] == "atencao"

    def test_vereador_dentro_limite(self) -> None:
        assert analisar_patrimonio(1_500_000, "vereador") is None

    def test_absurdo_sem_cargo(self) -> None:
        """Patrimônio > 50M sem cargo vira alerta info."""
        alerta = analisar_patrimonio(100_000_000, None)
        assert alerta is not None
        assert alerta["tipo"] == "info"

    def test_dentro_faixa_sem_alerta(self) -> None:
        assert analisar_patrimonio(500_000, "deputado federal") is None


class TestAnalisarEmendas:
    def test_lista_vazia(self) -> None:
        assert analisar_emendas([]) == []

    def test_concentracao_municipio(self) -> None:
        emendas = [
            {"municipality": "Goiania", "value_paid": 800_000, "value_committed": 800_000},
            {"municipality": "Goiania", "value_paid": 400_000, "value_committed": 400_000},
            {"municipality": "Anapolis", "value_paid": 100_000, "value_committed": 100_000},
        ]
        alertas = analisar_emendas(emendas)
        texto_concat = " ".join(a["texto"] for a in alertas)
        assert "concentradas em Goiania" in texto_concat

    def test_relator_gera_alerta_grave(self) -> None:
        emendas = [
            {"type": "relator", "value_paid": 1_000_000, "value_committed": 1_000_000},
        ]
        alertas = analisar_emendas(emendas)
        assert any(a["tipo"] == "grave" and "relator" in a["texto"] for a in alertas)

    def test_empenhadas_nao_pagas(self) -> None:
        emendas = [
            {"municipality": "Goiania", "value_committed": 500_000, "value_paid": 0},
        ]
        alertas = analisar_emendas(emendas)
        assert any("nao paga" in a["texto"] for a in alertas)

    def test_pagas_parcialmente(self) -> None:
        emendas = [
            {"municipality": "Goiania", "value_committed": 1_000_000, "value_paid": 500_000},
        ]
        alertas = analisar_emendas(emendas)
        assert any("parcial" in a["texto"] for a in alertas)

    def test_municipio_multiplo_ignorado_na_concentracao(self) -> None:
        """'Múltiplo' é marcador SIOP, não localidade real."""
        emendas = [
            {"municipality": "Múltiplo", "value_paid": 5_000_000, "value_committed": 5_000_000},
        ]
        alertas = analisar_emendas(emendas)
        # Não deve acionar alerta de concentração em "Múltiplo".
        assert not any("Múltiplo" in a["texto"] or "Multiplo" in a["texto"] for a in alertas)


class TestAnalisarConexoes:
    def test_lista_vazia(self) -> None:
        assert analisar_conexoes([], {}) == []

    def test_familiar_com_vinculo(self) -> None:
        conexoes = [{"target_id": "p1", "relationship_type": "CONJUGE_DE"}]
        entidades = {"p1": {"type": "person", "properties": {"name": "Maria Silva"}}}
        alertas = analisar_conexoes(conexoes, entidades)
        assert any("Familiar" in a["texto"] for a in alertas)

    def test_muitas_empresas(self) -> None:
        conexoes = [
            {"target_id": f"e{i}", "relationship_type": "SOCIO_DE"} for i in range(7)
        ]
        entidades = {f"e{i}": {"type": "company"} for i in range(7)}
        alertas = analisar_conexoes(conexoes, entidades)
        assert any("empresas conectadas" in a["texto"] for a in alertas)

    def test_poucas_empresas_sem_alerta(self) -> None:
        conexoes = [
            {"target_id": f"e{i}", "relationship_type": "SOCIO_DE"} for i in range(3)
        ]
        entidades = {f"e{i}": {"type": "company"} for i in range(3)}
        alertas = analisar_conexoes(conexoes, entidades)
        assert not any("empresas conectadas" in a["texto"] for a in alertas)

    def test_entidade_sancionada(self) -> None:
        conexoes = [{"target_id": "s1", "relationship_type": "SANCIONADA"}]
        entidades = {"s1": {"type": "sanction"}}
        alertas = analisar_conexoes(conexoes, entidades)
        assert any(a["tipo"] == "grave" and "sancionada" in a["texto"] for a in alertas)


class TestAnalisarDespesasGabinete:
    def test_despesas_vazias(self) -> None:
        assert analisar_despesas_gabinete([], uf="GO") == []

    def test_gastos_acima_cota_alertam(self) -> None:
        # GO cota mensal 46_980 * 24 meses = ~1.12M; alertar se > 80% = ~903K.
        total_esperado = int(COTA_CEAP_MENSAL["GO"] * 24 * 0.85)
        despesas = [{"tipoDespesa": "combustivel", "valorLiquido": total_esperado}]
        alertas = analisar_despesas_gabinete(despesas, uf="GO", num_meses=24)
        assert any("cota parlamentar" in a["texto"] for a in alertas)

    def test_categoria_dominante(self) -> None:
        despesas = [
            {"tipoDespesa": "combustivel", "valorLiquido": 100_000},
            {"tipoDespesa": "telefonia", "valorLiquido": 10_000},
        ]
        alertas = analisar_despesas_gabinete(despesas, uf="GO", num_meses=24)
        assert any("concentrados em" in a["texto"] for a in alertas)


class TestAnalisarDespesasVsMedia:
    def test_deputado_acima_media(self) -> None:
        alerta = analisar_despesas_vs_media(
            total_deputado=2_000_000, media_estado=1_000_000, uf="GO",
        )
        assert alerta is not None
        assert "GO" in alerta["texto"]

    def test_dentro_media(self) -> None:
        assert analisar_despesas_vs_media(1_100_000, 1_000_000, "GO") is None

    def test_zero(self) -> None:
        assert analisar_despesas_vs_media(0, 1_000_000, "GO") is None


class TestAnalisarPicosMensais:
    def test_vazio(self) -> None:
        assert analisar_picos_mensais([]) == []

    def test_com_pico(self) -> None:
        # Média de ~10k, um mês com 100k = pico (10x).
        despesas = [
            {"ano": 2024, "mes": 1, "valorLiquido": 10_000},
            {"ano": 2024, "mes": 2, "valorLiquido": 10_000},
            {"ano": 2024, "mes": 3, "valorLiquido": 10_000},
            {"ano": 2024, "mes": 4, "valorLiquido": 100_000},
        ]
        alertas = analisar_picos_mensais(despesas)
        assert any("Pico" in a["texto"] for a in alertas)


class TestGerarAlertasCompletos:
    def test_perfil_vazio_gera_aviso(self) -> None:
        alertas = gerar_alertas_completos({"properties": {}}, [], {}, [])
        assert len(alertas) == 1
        assert alertas[0]["tipo"] == "info"
        assert "indisponível" in alertas[0]["texto"]

    def test_patrimonio_alto(self) -> None:
        entidade = {
            "properties": {
                "patrimonio_declarado": 20_000_000,
                "cargo": "deputado federal",
            },
        }
        alertas = gerar_alertas_completos(entidade, [], {}, [])
        assert any("Patrimonio" in a["texto"] for a in alertas)

    def test_muitas_empresas(self) -> None:
        conexoes = [
            {"target_id": f"e{i}", "relationship_type": "SOCIO_DE"} for i in range(8)
        ]
        entidades = {f"e{i}": {"type": "company"} for i in range(8)}
        alertas = gerar_alertas_completos({"properties": {}}, conexoes, entidades, [])
        assert any("empresas conectadas" in a["texto"] for a in alertas)

    def test_sem_emendas_nao_gera_alertas_de_emenda(self) -> None:
        alertas = gerar_alertas_completos({"properties": {}}, [], {}, [])
        assert not any("emenda" in a.get("texto", "").lower() for a in alertas)

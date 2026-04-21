"""Tests para bracc.services.alertas_service.

Testa cada regra de alerta individualmente + orquestração.
"""

from __future__ import annotations

from datetime import date

from bracc.models.perfil import DoadorEmpresa, Emenda, SocioConectado, TetoGastos
from bracc.services.alertas_service import (
    BENEFICIARIO_NOVO_VALOR_MIN,
    COTA_CEAP_MENSAL,
    analisar_beneficiario_novo,
    analisar_cnpj_baixados,
    analisar_conexoes,
    analisar_despesas_gabinete,
    analisar_despesas_vs_media,
    analisar_doador_beneficiario,
    analisar_emendas,
    analisar_patrimonio,
    analisar_picos_mensais,
    analisar_teto_gastos,
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
        assert any(
            a["tipo"] == "atencao" and "demoram" in a["texto"]
            for a in alertas
        )

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


def _doador(
    cnpj: str, situacao: str | None, valor: float = 10_000.0,
) -> DoadorEmpresa:
    """Factory mínima pra :class:`DoadorEmpresa` nos tests de alerta."""
    return DoadorEmpresa(
        nome=f"Empresa {cnpj}",
        cnpj=cnpj,
        valor_total=valor,
        valor_total_fmt=f"R$ {valor:.2f}",
        n_doacoes=1,
        situacao=situacao,
        situacao_fmt=situacao.capitalize() if situacao else None,
        situacao_verified_at="2026-04-15T10:00:00+00:00",
    )


def _socio(cnpj: str, situacao: str | None) -> SocioConectado:
    return SocioConectado(
        nome=f"Socio {cnpj}",
        cnpj=cnpj,
        situacao=situacao,
        situacao_fmt=situacao.capitalize() if situacao else None,
        situacao_verified_at="2026-04-15T10:00:00+00:00",
    )


class _PerfilDuck:
    """Duck-typed stand-in pro ``PerfilPolitico`` com só os 2 campos usados."""

    def __init__(
        self,
        doadores_empresa: list[DoadorEmpresa] | None = None,
        socios: list[SocioConectado] | None = None,
    ) -> None:
        self.doadores_empresa = doadores_empresa or []
        self.socios = socios or []


class TestAnalisarCnpjBaixados:
    def test_sem_empresas_vazio(self) -> None:
        perfil = _PerfilDuck()
        assert analisar_cnpj_baixados(perfil) == []

    def test_todas_ativas_sem_alerta(self) -> None:
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "ATIVA")],
            socios=[_socio("22222222000102", "ATIVA")],
        )
        assert analisar_cnpj_baixados(perfil) == []

    def test_empresas_nao_verificadas_sem_alerta(self) -> None:
        """Situacao None (pipeline ainda nao rodou) nao dispara ruido."""
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", None)],
            socios=[_socio("22222222000102", None)],
        )
        assert analisar_cnpj_baixados(perfil) == []

    def test_doador_baixada_dispara_grave(self) -> None:
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "BAIXADA")],
        )
        alertas = analisar_cnpj_baixados(perfil)
        assert len(alertas) == 1
        alerta = alertas[0]
        assert alerta["tipo"] == "grave"
        assert "baixada" in alerta["texto"].lower()
        assert "doadora" in alerta["texto"].lower()

    def test_socio_inapta_dispara_grave(self) -> None:
        perfil = _PerfilDuck(
            socios=[_socio("22222222000102", "INAPTA")],
        )
        alertas = analisar_cnpj_baixados(perfil)
        assert len(alertas) == 1
        assert alertas[0]["tipo"] == "grave"
        assert "socio" in alertas[0]["texto"].lower()

    def test_doador_suspensa_dispara_grave(self) -> None:
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "SUSPENSA")],
        )
        alertas = analisar_cnpj_baixados(perfil)
        assert len(alertas) == 1
        assert alertas[0]["tipo"] == "grave"

    def test_multiplas_empresas_contam_no_total(self) -> None:
        perfil = _PerfilDuck(
            doadores_empresa=[
                _doador("11111111000101", "BAIXADA"),
                _doador("22222222000102", "SUSPENSA"),
                _doador("33333333000103", "ATIVA"),  # Nao conta.
            ],
            socios=[
                _socio("44444444000104", "INAPTA"),
                _socio("55555555000105", "ATIVA"),  # Nao conta.
            ],
        )
        alertas = analisar_cnpj_baixados(perfil)
        assert len(alertas) == 1
        # 3 graves: 2 doadores + 1 socio.
        assert "total: 3" in alertas[0]["texto"]
        assert "doadora" in alertas[0]["texto"].lower()
        assert "socio" in alertas[0]["texto"].lower()

    def test_nula_nao_dispara(self) -> None:
        """NULA e um estado limbo RFB mais raro; mantemos fora do grave."""
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "NULA")],
        )
        assert analisar_cnpj_baixados(perfil) == []


def _emenda(
    amendment_id: str,
    beneficiario_cnpj: str | None,
    valor_pago: float = 0.0,
    valor_empenhado: float = 0.0,
    beneficiario_data_abertura: str | None = None,
) -> Emenda:
    return Emenda(
        id=amendment_id,
        tipo="Individual",
        funcao="Saude",
        municipio="Goiania",
        uf="GO",
        valor_empenhado=valor_empenhado,
        valor_empenhado_fmt=f"R$ {valor_empenhado:.2f}",
        valor_pago=valor_pago,
        valor_pago_fmt=f"R$ {valor_pago:.2f}",
        beneficiario_cnpj=beneficiario_cnpj,
        beneficiario_nome="Empresa Teste LTDA" if beneficiario_cnpj else None,
        beneficiario_data_abertura=beneficiario_data_abertura,
    )


class TestAnalisarDoadorBeneficiario:
    def test_sem_emendas_vazio(self) -> None:
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "ATIVA")],
        )
        assert analisar_doador_beneficiario(perfil, []) == []

    def test_sem_doadores_vazio(self) -> None:
        emendas = [_emenda("E1", "11111111000101", valor_pago=500_000)]
        assert analisar_doador_beneficiario(_PerfilDuck(), emendas) == []

    def test_doador_sem_cruzamento_vazio(self) -> None:
        """Doador e beneficiario com CNPJs diferentes nao disparam."""
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "ATIVA")],
        )
        emendas = [_emenda("E1", "99999999000199", valor_pago=500_000)]
        assert analisar_doador_beneficiario(perfil, emendas) == []

    def test_doador_sem_cnpj_nao_cruza(self) -> None:
        """Doador com CNPJ None nao gera match espurio com beneficiario None."""
        doador = _doador("00000000000000", "ATIVA")
        doador.cnpj = None
        perfil = _PerfilDuck(doadores_empresa=[doador])
        emendas = [_emenda("E1", None, valor_pago=500_000)]
        assert analisar_doador_beneficiario(perfil, emendas) == []

    def test_match_simples_dispara_grave(self) -> None:
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "ATIVA", valor=50_000)],
        )
        emendas = [_emenda("E1", "11111111000101", valor_pago=500_000)]
        alertas = analisar_doador_beneficiario(perfil, emendas)
        assert len(alertas) == 1
        alerta = alertas[0]
        assert alerta["tipo"] == "grave"
        assert "doou" in alerta["texto"].lower()
        assert "emenda" in alerta["texto"].lower()

    def test_cnpj_formatado_casa_com_cru(self) -> None:
        """CNPJ com mascara no doador x cru no beneficiario tem que bater."""
        doador = _doador("11111111000101", "ATIVA")
        doador.cnpj = "11.111.111/0001-01"
        perfil = _PerfilDuck(doadores_empresa=[doador])
        emendas = [_emenda("E1", "11111111000101", valor_pago=500_000)]
        alertas = analisar_doador_beneficiario(perfil, emendas)
        assert len(alertas) == 1

    def test_multiplas_emendas_mesmo_doador_agregam(self) -> None:
        """Varias emendas p/ mesmo CNPJ somam num unico alerta com total."""
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "ATIVA")],
        )
        emendas = [
            _emenda("E1", "11111111000101", valor_pago=300_000),
            _emenda("E2", "11111111000101", valor_pago=200_000),
        ]
        alertas = analisar_doador_beneficiario(perfil, emendas)
        assert len(alertas) == 1  # Um alerta agregado, nao dois.

    def test_fallback_valor_empenhado_quando_sem_pago(self) -> None:
        """Sem valor_pago, usa valor_empenhado pra somar."""
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "ATIVA")],
        )
        emendas = [_emenda("E1", "11111111000101", valor_empenhado=800_000)]
        alertas = analisar_doador_beneficiario(perfil, emendas)
        assert len(alertas) == 1


class TestAnalisarBeneficiarioNovo:
    REF = date(2026, 4, 20)

    def test_sem_emendas_vazio(self) -> None:
        assert analisar_beneficiario_novo([], referencia=self.REF) == []

    def test_cnpj_velho_sem_alerta(self) -> None:
        emendas = [_emenda(
            "E1", "11111111000101",
            valor_pago=BENEFICIARIO_NOVO_VALOR_MIN + 100_000,
            beneficiario_data_abertura="2010-01-15",
        )]
        assert analisar_beneficiario_novo(emendas, referencia=self.REF) == []

    def test_cnpj_sem_data_nao_alerta(self) -> None:
        """Data ausente (pipeline nao rodou) nao dispara ruido."""
        emendas = [_emenda(
            "E1", "11111111000101",
            valor_pago=BENEFICIARIO_NOVO_VALOR_MIN + 100_000,
            beneficiario_data_abertura=None,
        )]
        assert analisar_beneficiario_novo(emendas, referencia=self.REF) == []

    def test_cnpj_novo_valor_alto_dispara(self) -> None:
        emendas = [_emenda(
            "E1", "11111111000101",
            valor_pago=BENEFICIARIO_NOVO_VALOR_MIN + 500_000,
            beneficiario_data_abertura="2025-06-15",  # <1 ano
        )]
        alertas = analisar_beneficiario_novo(emendas, referencia=self.REF)
        assert len(alertas) == 1
        alerta = alertas[0]
        assert alerta["tipo"] == "atencao"
        assert "cnpj" in alerta["texto"].lower()
        assert "aberto" in alerta["texto"].lower()

    def test_cnpj_novo_mas_valor_pequeno_nao_alerta(self) -> None:
        """Abaixo do valor minimo: nao alerta (ruido de servico pontual)."""
        emendas = [_emenda(
            "E1", "11111111000101",
            valor_pago=50_000.0,
            beneficiario_data_abertura="2025-06-15",
        )]
        assert analisar_beneficiario_novo(emendas, referencia=self.REF) == []

    def test_data_mal_formada_silenciosa(self) -> None:
        """Data invalida nao crasha, so vira None-like e pula."""
        emendas = [_emenda(
            "E1", "11111111000101",
            valor_pago=BENEFICIARIO_NOVO_VALOR_MIN + 100_000,
            beneficiario_data_abertura="invalid-date",
        )]
        assert analisar_beneficiario_novo(emendas, referencia=self.REF) == []

    def test_multiplas_emendas_mesmo_cnpj_agregam(self) -> None:
        emendas = [
            _emenda("E1", "11111111000101", valor_pago=300_000,
                    beneficiario_data_abertura="2025-06-15"),
            _emenda("E2", "11111111000101", valor_pago=400_000,
                    beneficiario_data_abertura="2025-06-15"),
        ]
        alertas = analisar_beneficiario_novo(emendas, referencia=self.REF)
        assert len(alertas) == 1  # Agregado.


class TestGerarAlertasCompletosComPerfil:
    def test_perfil_none_nao_quebra_compat(self) -> None:
        """Sem perfil: funcao antiga continua funcionando igual."""
        alertas = gerar_alertas_completos({"properties": {}}, [], {}, [])
        assert alertas[0]["tipo"] == "info"

    def test_perfil_com_doador_baixada_injeta_alerta_grave(self) -> None:
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "BAIXADA")],
        )
        alertas = gerar_alertas_completos(
            {"properties": {}}, [], {}, [], perfil=perfil,
        )
        assert any(
            a["tipo"] == "grave" and "baixada" in a["texto"].lower()
            for a in alertas
        )

    def test_perfil_com_doador_beneficiario_injeta_alerta_grave(self) -> None:
        """Cross-check TSE-doador x Transferegov-beneficiario entra no output."""
        perfil = _PerfilDuck(
            doadores_empresa=[_doador("11111111000101", "ATIVA")],
        )
        emendas = [_emenda("E1", "11111111000101", valor_pago=500_000)]
        alertas = gerar_alertas_completos(
            {"properties": {}}, [], {}, [],
            perfil=perfil, emendas_tipadas=emendas,
        )
        assert any(
            a["tipo"] == "grave" and "doou" in a["texto"].lower()
            for a in alertas
        )


def _teto(classificacao: str, pct: float = 50.0) -> TetoGastos:
    """Factory pra ``TetoGastos`` com classificação pré-definida."""
    valor_limite = 2_100_000.0
    valor_gasto = valor_limite * (pct / 100)
    return TetoGastos(
        valor_limite=valor_limite,
        valor_limite_fmt="R$ 2.10 mi",
        valor_gasto=valor_gasto,
        valor_gasto_fmt="R$ 1.05 mi",
        pct_usado=pct,
        pct_usado_fmt=f"{pct:.0f}%",
        cargo="DEPUTADO FEDERAL",
        ano_eleicao=2022,
        classificacao=classificacao,
        fonte_legal="Resolução TSE nº 23.607/2019 (Eleições 2022)",
    )


class TestAnalisarTetoGastos:
    def test_none_retorna_lista_vazia(self) -> None:
        assert analisar_teto_gastos(None) == []

    def test_ok_sem_alerta(self) -> None:
        """Abaixo de 70% do teto não gera ruído."""
        assert analisar_teto_gastos(_teto("ok", pct=50.0)) == []

    def test_alto_gera_info(self) -> None:
        alertas = analisar_teto_gastos(_teto("alto", pct=85.0))
        assert len(alertas) == 1
        assert alertas[0]["tipo"] == "info"
        assert "teto legal" in alertas[0]["texto"].lower()

    def test_limite_gera_atencao(self) -> None:
        alertas = analisar_teto_gastos(_teto("limite", pct=95.0))
        assert len(alertas) == 1
        assert alertas[0]["tipo"] == "atencao"
        assert "limite" in alertas[0]["texto"].lower()

    def test_ultrapassou_gera_grave(self) -> None:
        alertas = analisar_teto_gastos(_teto("ultrapassou", pct=119.0))
        assert len(alertas) == 1
        assert alertas[0]["tipo"] == "grave"
        assert "ultrapassou" in alertas[0]["texto"].lower()
        # Cita a fonte legal pro usuário verificar.
        assert "23.607/2019" in alertas[0]["texto"]

"""Testes do modulo principal (app.py).

Testa helpers, modelos e geracao de alertas.
Usa respx para mockar chamadas a APIs externas.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import (
    DespesaGabinete,
    Emenda,
    EmpresaConectada,
    LicitacaoGO,
    MunicipioResumo,
    NomeacaoGO,
    PerfilPolitico,
    PoliticoResumo,
    ServidorResumo,
    StatusResponse,
    VereadorResumo,
    fmt_brl,
    gerar_alertas_completos,
    traduzir_relacao,
)


# === fmt_brl ===


class TestFmtBrl:
    def test_bilhoes(self):
        assert "bi" in fmt_brl(2_500_000_000)

    def test_milhoes(self):
        assert "mi" in fmt_brl(3_500_000)

    def test_milhares(self):
        assert "mil" in fmt_brl(45_000)

    def test_valor_pequeno(self):
        result = fmt_brl(123.45)
        assert "R$" in result

    def test_zero(self):
        result = fmt_brl(0)
        assert "R$" in result


# === traduzir_relacao ===


class TestTraduzirRelacao:
    def test_socio(self):
        assert traduzir_relacao("SOCIO_DE") == "Socio(a) de"

    def test_conjuge(self):
        assert traduzir_relacao("CONJUGE_DE") == "Conjuge de"

    def test_desconhecido(self):
        result = traduzir_relacao("ALGO_NOVO")
        assert result == "Algo novo"


# === gerar_alertas_completos ===


class TestGerarAlertasCompletos:
    def test_sem_dados_retorna_ok(self):
        entidade = {"properties": {}}
        alertas = gerar_alertas_completos(entidade, [], {}, [])
        assert len(alertas) == 1
        assert alertas[0]["tipo"] == "ok"
        assert "irregularidade" in alertas[0]["texto"].lower()

    def test_patrimonio_alto_gera_alerta(self):
        entidade = {
            "properties": {
                "patrimonio_declarado": 15_000_000,
                "role": "deputado federal",
            },
        }
        alertas = gerar_alertas_completos(entidade, [], {}, [])
        assert any(a["tipo"] == "atencao" for a in alertas)
        # Nao deve ter o "ok" fallback
        assert not any(a["tipo"] == "ok" for a in alertas)

    def test_emendas_alto_valor_gera_alerta(self):
        entidade = {"properties": {}}
        emendas = [
            {"value_paid": 6_000_000, "municipality": "SP", "type": "Individual"},
            {"value_paid": 5_000_000, "municipality": "SP", "type": "Individual"},
        ]
        alertas = gerar_alertas_completos(entidade, [], {}, emendas)
        assert any("emendas" in a["texto"].lower() for a in alertas)

    def test_emendas_relator_gera_alerta_grave(self):
        entidade = {"properties": {}}
        emendas = [
            {"value_paid": 1_000_000, "municipality": "", "type": "Relator"},
        ]
        alertas = gerar_alertas_completos(entidade, [], {}, emendas)
        assert any(a["tipo"] == "grave" for a in alertas)


# === Modelos Pydantic ===


class TestModelos:
    def test_perfil_politico_campos_opcionais(self):
        """Verifica que campos novos tem defaults."""
        perfil = PerfilPolitico(
            politico=PoliticoResumo(id="1", nome="Teste"),
            resumo="Resumo",
            emendas=[],
            total_emendas_valor=0,
            total_emendas_valor_fmt="R$ 0,00",
            empresas=[],
            contratos=[],
            alertas=[],
            conexoes_total=0,
        )
        assert perfil.despesas_gabinete == []
        assert perfil.total_despesas_gabinete == 0
        assert perfil.fonte_emendas is None

    def test_despesa_gabinete_model(self):
        d = DespesaGabinete(tipo="Combustivel", total=5000, total_fmt="R$ 5.0 mil")
        assert d.tipo == "Combustivel"
        assert d.total == 5000

    def test_emenda_model(self):
        e = Emenda(
            id="1", tipo="Individual", funcao="Saude",
            valor_empenhado=100, valor_empenhado_fmt="R$ 100,00",
            valor_pago=80, valor_pago_fmt="R$ 80,00",
        )
        assert e.municipio is None
        assert e.valor_pago == 80


class TestNovosModelos:
    def test_servidor_resumo(self):
        s = ServidorResumo(id="1", nome="Teste", cargo="Analista")
        assert s.is_comissionado is False
        assert s.orgao is None

    def test_municipio_resumo(self):
        m = MunicipioResumo(id="5208707", nome="Goiania")
        assert m.populacao is None
        assert m.receita_total is None

    def test_licitacao_go(self):
        lic = LicitacaoGO(id="1", orgao="SEAD", objeto="Material de escritorio")
        assert lic.valor_estimado is None
        assert lic.municipio is None

    def test_nomeacao_go(self):
        n = NomeacaoGO(id="1", nome_pessoa="Fulano", tipo="nomeacao")
        assert n.cargo is None

    def test_vereador_resumo(self):
        v = VereadorResumo(id="1", nome="Teste")
        assert v.municipio == "Goiania"
        assert v.proposicoes == 0

    def test_status_response_novos_campos(self):
        s = StatusResponse(
            status="online", bracc_conectado=True,
            total_nos=100, total_relacionamentos=200,
            deputados_federais=17, deputados_estaduais=41, senadores=3,
        )
        assert s.servidores_estaduais == 0
        assert s.vereadores_goiania == 0

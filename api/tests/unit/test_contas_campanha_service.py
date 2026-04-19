"""Tests para bracc.services.contas_campanha_service.

Fase 1 do roadmap cross-check perspectivas TSE (Issue:
``todo-list-prompts/high_priority/debitos/cross-check-perspectivas-tse.md``).
"""

from __future__ import annotations

from bracc.services.contas_campanha_service import gerar_comparacao_contas


class TestGerarComparacaoContas:
    def test_retorna_none_sem_receitas(self) -> None:
        """Sem total_tse_2022 → card omitido."""
        props = {"total_despesas_tse_2022": 100_000}
        assert gerar_comparacao_contas(props, 2022) is None

    def test_retorna_none_sem_despesas(self) -> None:
        """Sem total_despesas_tse_2022 → card omitido."""
        props = {"total_tse_2022": 100_000}
        assert gerar_comparacao_contas(props, 2022) is None

    def test_retorna_none_receitas_zero(self) -> None:
        props = {"total_tse_2022": 0, "total_despesas_tse_2022": 100_000}
        assert gerar_comparacao_contas(props, 2022) is None

    def test_retorna_none_despesas_zero(self) -> None:
        props = {"total_tse_2022": 100_000, "total_despesas_tse_2022": 0}
        assert gerar_comparacao_contas(props, 2022) is None

    def test_status_ok_diferenca_baixa(self) -> None:
        """Divergência < 5% → status ok."""
        props = {"total_tse_2022": 100, "total_despesas_tse_2022": 102}
        resultado = gerar_comparacao_contas(props, 2022)
        assert resultado is not None
        assert resultado.status == "ok"
        assert resultado.divergencia_pct < 5

    def test_status_atencao_diferenca_moderada(self) -> None:
        """5% <= divergência < 20% → status atencao."""
        props = {"total_tse_2022": 100, "total_despesas_tse_2022": 110}
        resultado = gerar_comparacao_contas(props, 2022)
        assert resultado is not None
        assert resultado.status == "atencao"
        # 10/110 ~ 9.1%
        assert 5 <= resultado.divergencia_pct < 20

    def test_status_divergente_diferenca_alta(self) -> None:
        """Divergência >= 20% + despesas > receitas → divergente + despesas_excedem."""
        props = {"total_tse_2022": 100, "total_despesas_tse_2022": 200}
        resultado = gerar_comparacao_contas(props, 2022)
        assert resultado is not None
        assert resultado.status == "divergente"
        assert resultado.direcao == "despesas_excedem"
        # 100/200 = 50%
        assert resultado.divergencia_pct >= 20

    def test_direcao_receitas_excedem(self) -> None:
        """Receitas > despesas → direcao receitas_excedem."""
        props = {"total_tse_2022": 200, "total_despesas_tse_2022": 50}
        resultado = gerar_comparacao_contas(props, 2022)
        assert resultado is not None
        assert resultado.direcao == "receitas_excedem"
        assert resultado.status == "divergente"

    def test_divergencia_preserva_sinal(self) -> None:
        """divergencia_valor = receitas - despesas; sinal preservado."""
        props_sobra = {"total_tse_2022": 200, "total_despesas_tse_2022": 50}
        resultado_sobra = gerar_comparacao_contas(props_sobra, 2022)
        assert resultado_sobra is not None
        assert resultado_sobra.divergencia_valor == 150  # 200 - 50

        props_estouro = {"total_tse_2022": 100, "total_despesas_tse_2022": 200}
        resultado_estouro = gerar_comparacao_contas(props_estouro, 2022)
        assert resultado_estouro is not None
        assert resultado_estouro.divergencia_valor == -100  # 100 - 200
        # divergencia_valor_fmt sempre mostra absoluto
        assert "100" in resultado_estouro.divergencia_valor_fmt

    def test_fmt_fields_preenchidos(self) -> None:
        props = {"total_tse_2022": 1_500_000, "total_despesas_tse_2022": 1_450_000}
        resultado = gerar_comparacao_contas(props, 2022)
        assert resultado is not None
        assert resultado.total_receitas_fmt.startswith("R$")
        assert resultado.total_despesas_fmt.startswith("R$")
        assert resultado.divergencia_valor_fmt.startswith("R$")

    def test_ano_eleicao_ecoa_parametro(self) -> None:
        """Permite chamar com outro ano (ex.: 2026 futuro)."""
        props = {"total_tse_2026": 500_000, "total_despesas_tse_2026": 480_000}
        resultado = gerar_comparacao_contas(props, 2026)
        assert resultado is not None
        assert resultado.ano_eleicao == 2026

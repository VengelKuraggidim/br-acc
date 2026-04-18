"""Tests para bracc.services.validacao_tse_service."""

from __future__ import annotations

from bracc.services.validacao_tse_service import gerar_validacao_tse


class TestGerarValidacaoTse:
    def test_sem_total_tse_retorna_none(self) -> None:
        assert gerar_validacao_tse({}, 0) is None

    def test_total_tse_zero_retorna_none(self) -> None:
        """Campo ausente/falsy — sem dados TSE ingeridos."""
        assert gerar_validacao_tse({"total_tse_2022": 0}, 0) is None

    def test_status_ok_divergencia_baixa(self) -> None:
        props = {"total_tse_2022": 1_000_000}
        resultado = gerar_validacao_tse(props, total_doacoes=980_000)
        assert resultado is not None
        assert resultado.status == "ok"
        assert resultado.divergencia_pct < 5

    def test_status_atencao_divergencia_media(self) -> None:
        """Divergência 10% (entre 5 e 20)."""
        props = {"total_tse_2022": 1_000_000}
        resultado = gerar_validacao_tse(props, total_doacoes=900_000)
        assert resultado is not None
        assert resultado.status == "atencao"

    def test_status_divergente_divergencia_alta(self) -> None:
        """Divergência 50%."""
        props = {"total_tse_2022": 1_000_000}
        resultado = gerar_validacao_tse(props, total_doacoes=500_000)
        assert resultado is not None
        assert resultado.status == "divergente"

    def test_ano_eleicao_2022(self) -> None:
        props = {"total_tse_2022": 100_000}
        resultado = gerar_validacao_tse(props, 100_000)
        assert resultado is not None
        assert resultado.ano_eleicao == 2022

    def test_breakdown_preenchido(self) -> None:
        props = {
            "total_tse_2022": 1_000_000,
            "tse_2022_partido": 600_000,
            "tse_2022_pessoa_fisica": 200_000,
            "tse_2022_proprios": 200_000,
        }
        resultado = gerar_validacao_tse(props, total_doacoes=1_000_000)
        assert resultado is not None
        assert len(resultado.breakdown_tse) == 3
        origens = {item["origem"] for item in resultado.breakdown_tse}
        assert any("Partido" in o for o in origens)
        assert any("Pessoas físicas" in o for o in origens)

    def test_breakdown_ignora_zeros(self) -> None:
        props = {
            "total_tse_2022": 500_000,
            "tse_2022_partido": 500_000,
            "tse_2022_pessoa_fisica": 0,
            "tse_2022_proprios": None,
        }
        resultado = gerar_validacao_tse(props, total_doacoes=500_000)
        assert resultado is not None
        assert len(resultado.breakdown_tse) == 1

    def test_divergencia_valor_positivo_ou_negativo(self) -> None:
        """Ingerido pode ser maior que declarado (rara mas possível)."""
        props = {"total_tse_2022": 1_000_000}
        resultado = gerar_validacao_tse(props, total_doacoes=1_200_000)
        assert resultado is not None
        assert resultado.divergencia_valor == -200_000
        # divergencia_valor_fmt usa abs() — sempre positivo no texto
        assert "200" in resultado.divergencia_valor_fmt

    def test_fmt_fields_preenchidos(self) -> None:
        props = {"total_tse_2022": 2_500_000}
        resultado = gerar_validacao_tse(props, total_doacoes=2_500_000)
        assert resultado is not None
        assert resultado.total_declarado_tse_fmt.startswith("R$")
        assert resultado.total_ingerido_fmt.startswith("R$")
        assert resultado.divergencia_valor_fmt.startswith("R$")

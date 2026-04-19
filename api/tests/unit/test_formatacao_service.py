"""Tests para bracc.services.formatacao_service.

mascarar_cpf é o teste mais crítico (LGPD — CPF pleno nunca pode vazar).
"""

from __future__ import annotations

import pytest

from bracc.services.formatacao_service import (
    fmt_brl,
    fmt_data_br,
    mascarar_cpf,
    nomear_mes,
)


class TestFmtBrl:
    def test_bilhoes(self) -> None:
        assert fmt_brl(2_500_000_000) == "R$ 2.50 bi"

    def test_milhoes(self) -> None:
        assert fmt_brl(1_234_567) == "R$ 1.23 mi"

    def test_mil(self) -> None:
        assert fmt_brl(25_000) == "R$ 25.0 mil"

    def test_mil_com_decimal(self) -> None:
        assert fmt_brl(1500) == "R$ 1.5 mil"

    def test_centavos(self) -> None:
        assert fmt_brl(123.45) == "R$ 123,45"

    def test_centenas(self) -> None:
        assert fmt_brl(999.99) == "R$ 999,99"

    def test_zero(self) -> None:
        assert fmt_brl(0) == "R$ 0,00"

    def test_none_fallback(self) -> None:
        assert fmt_brl(None) == "R$ 0,00"

    def test_pequeno_decimal(self) -> None:
        assert fmt_brl(0.5) == "R$ 0,50"

    def test_separador_milhar(self) -> None:
        # 500.00 deve ficar "500,00" (vírgula decimal, sem ponto nos milhares)
        assert fmt_brl(500) == "R$ 500,00"

    def test_milhar_no_limiar(self) -> None:
        # Entra na faixa "mil"
        assert fmt_brl(1000) == "R$ 1.0 mil"

    def test_milhao_no_limiar(self) -> None:
        assert fmt_brl(1_000_000) == "R$ 1.00 mi"

    def test_bilhao_no_limiar(self) -> None:
        assert fmt_brl(1_000_000_000) == "R$ 1.00 bi"


class TestMascararCpf:
    def test_cpf_pleno_mascarado(self) -> None:
        """CPF pleno (11 dígitos) vira ***.***.***-YY."""
        assert mascarar_cpf("11122233344") == "***.***.***-44"

    def test_cpf_pleno_nunca_aparece_no_resultado(self) -> None:
        """LGPD: o CPF pleno NUNCA pode aparecer na saída."""
        cpf = "11122233344"
        resultado = mascarar_cpf(cpf)
        assert resultado is not None
        assert cpf not in resultado
        # Apenas os 2 últimos dígitos podem aparecer
        assert "1112223334" not in resultado  # primeiros 10 dígitos
        assert "112223334" not in resultado
        assert "2223334" not in resultado

    def test_cpf_com_pontuacao(self) -> None:
        """CPF formatado (111.222.333-44) é normalizado antes."""
        assert mascarar_cpf("111.222.333-44") == "***.***.***-44"

    def test_cpf_com_espacos_e_pontuacao(self) -> None:
        assert mascarar_cpf("  111.222.333-44  ") == "***.***.***-44"

    def test_cpf_parcial_retorna_none(self) -> None:
        """CPF com menos de 11 dígitos é considerado inválido — retorna None."""
        assert mascarar_cpf("1112223") is None

    def test_cpf_muito_longo_retorna_none(self) -> None:
        """Mais de 11 dígitos (ex: CNPJ de 14) retorna None."""
        assert mascarar_cpf("12345678000190") is None

    def test_cpf_vazio(self) -> None:
        assert mascarar_cpf("") is None

    def test_cpf_none(self) -> None:
        assert mascarar_cpf(None) is None

    def test_cpf_somente_letras(self) -> None:
        assert mascarar_cpf("abcdefghijk") is None

    def test_cpf_zeros(self) -> None:
        """Edge case — CPF todo zeros. Ainda tem 11 dígitos, mascara."""
        assert mascarar_cpf("00000000000") == "***.***.***-00"


class TestNomearMes:
    @pytest.mark.parametrize(
        ("mes", "esperado"),
        [
            (1, "Jan"),
            (2, "Fev"),
            (3, "Mar"),
            (4, "Abr"),
            (5, "Mai"),
            (6, "Jun"),
            (7, "Jul"),
            (8, "Ago"),
            (9, "Set"),
            (10, "Out"),
            (11, "Nov"),
            (12, "Dez"),
        ],
    )
    def test_meses_validos(self, mes: int, esperado: str) -> None:
        assert nomear_mes(mes) == esperado

    def test_mes_invalido_retorna_string(self) -> None:
        assert nomear_mes(13) == "13"

    def test_mes_zero(self) -> None:
        assert nomear_mes(0) == "0"

    def test_mes_none(self) -> None:
        assert nomear_mes(None) == ""


class TestFmtDataBr:
    def test_iso_data_pura(self) -> None:
        assert fmt_data_br("2022-09-15") == "15/09/2022"

    def test_iso_com_hora(self) -> None:
        assert fmt_data_br("2022-09-15T13:45:00Z") == "15/09/2022"

    def test_none(self) -> None:
        assert fmt_data_br(None) is None

    def test_vazio(self) -> None:
        assert fmt_data_br("") is None

    def test_formato_invalido_retorna_none(self) -> None:
        assert fmt_data_br("15/09/2022") is None
        assert fmt_data_br("2022") is None
        assert fmt_data_br("abcdefghij") is None

    def test_digitos_invalidos(self) -> None:
        assert fmt_data_br("20XX-09-15") is None

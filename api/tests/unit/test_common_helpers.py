"""Unit tests pros helpers primitivos compartilhados (:mod:`bracc.services.common_helpers`).

Estes testes não existem pra cobrir linhas — cobrem o **contrato** que
outros services (``conexoes_service``, futuros) dependem. Qualquer
mudança de comportamento (ex.: ``as_float`` passar a levantar em vez
de retornar ``0.0``) quebra aqui antes de quebrar na cadeia.
"""

from __future__ import annotations

from bracc.services.common_helpers import as_float, as_str, norm_type


class TestAsStr:
    def test_string_nao_vazia_retorna_valor(self) -> None:
        assert as_str({"name": "Fulano"}, "name") == "Fulano"

    def test_string_vazia_retorna_none(self) -> None:
        assert as_str({"name": ""}, "name") is None

    def test_chave_ausente_retorna_none(self) -> None:
        assert as_str({}, "name") is None

    def test_valor_nao_string_retorna_none(self) -> None:
        assert as_str({"name": 42}, "name") is None
        assert as_str({"name": None}, "name") is None
        assert as_str({"name": ["a"]}, "name") is None


class TestAsFloat:
    def test_none_retorna_zero(self) -> None:
        assert as_float(None) == 0.0

    def test_float_retorna_valor(self) -> None:
        assert as_float(1.5) == 1.5

    def test_int_retorna_float(self) -> None:
        assert as_float(42) == 42.0

    def test_string_parseavel(self) -> None:
        assert as_float("12.34") == 12.34

    def test_string_invalida_retorna_zero(self) -> None:
        assert as_float("abc") == 0.0

    def test_objeto_arbitrario_retorna_zero(self) -> None:
        """Contrato: nunca levanta — valor inválido vira 0.0."""
        assert as_float(object()) == 0.0


class TestNormType:
    def test_lowercase(self) -> None:
        assert norm_type("Company") == "company"
        assert norm_type("Amendment") == "amendment"

    def test_ja_lowercase_passthrough(self) -> None:
        assert norm_type("person") == "person"

    def test_nao_string_retorna_vazio(self) -> None:
        assert norm_type(None) == ""
        assert norm_type(42) == ""
        assert norm_type([]) == ""

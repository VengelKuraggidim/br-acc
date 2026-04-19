"""Unit tests pros helpers primitivos compartilhados (:mod:`bracc.services.common_helpers`).

Estes testes não existem pra cobrir linhas — cobrem o **contrato** que
outros services (``conexoes_service``, futuros) dependem. Qualquer
mudança de comportamento (ex.: ``as_float`` passar a levantar em vez
de retornar ``0.0``) quebra aqui antes de quebrar na cadeia.
"""

from __future__ import annotations

from bracc.services.common_helpers import archival_url, as_float, as_str, norm_type


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


class TestArchivalUrl:
    """Contrato de :func:`archival_url` — reescrita pra ``/archival/<uri>``.

    URIs de snapshot vêm gravadas relativas à raiz ``BRACC_ARCHIVAL_ROOT``
    (ex.: ``tse_prestacao_contas/2026-04/abc.bin``). Sem prefixar, o
    browser resolve contra o origin do PWA e cai no fallback do nginx.
    """

    def test_none_retorna_none(self) -> None:
        assert archival_url(None) is None

    def test_string_vazia_retorna_none(self) -> None:
        assert archival_url("") is None

    def test_uri_relativa_recebe_prefixo(self) -> None:
        assert (
            archival_url("tse_prestacao_contas/2026-04/abc.bin")
            == "/archival/tse_prestacao_contas/2026-04/abc.bin"
        )

    def test_uri_absoluta_http_passa_igual(self) -> None:
        url = "http://internet-archive.example/snapshot/xyz"
        assert archival_url(url) == url

    def test_uri_absoluta_https_passa_igual(self) -> None:
        url = "https://archive.org/web/2026/snapshot"
        assert archival_url(url) == url

    def test_uri_ja_prefixada_passa_igual(self) -> None:
        url = "/archival/tse_prestacao_contas/2026-04/abc.bin"
        assert archival_url(url) == url

    def test_uri_com_barra_inicial_nao_duplica(self) -> None:
        """URI começando com ``/`` (mas sem ser ``/archival/``) tem a barra
        absorvida pelo ``lstrip`` — evita devolver ``/archival//foo``.
        """
        assert (
            archival_url("/tse_prestacao_contas/2026-04/abc.bin")
            == "/archival/tse_prestacao_contas/2026-04/abc.bin"
        )

"""Tests for ``bracc_etl.transforms.brazilian_numbers``."""

from __future__ import annotations

import math

import pytest

from bracc_etl.transforms.brazilian_numbers import (
    parse_brl_amount,
    parse_brl_flexible,
    parse_number_smart,
    parse_numeric_comma,
)


class TestParseBrlAmount:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("1.234.567,89", 1_234_567.89),
            ("1.234,56", 1234.56),
            ("1234,56", 1234.56),
            ("0,01", 0.01),
            ("0", 0.0),
            ("1000", 1000.0),
            (" 1.234,56 ", 1234.56),
        ],
    )
    def test_happy_paths(self, raw: str, expected: float) -> None:
        assert parse_brl_amount(raw) == pytest.approx(expected)

    @pytest.mark.parametrize("raw", ["", "   ", None])
    def test_empty_returns_default(self, raw: str | None) -> None:
        assert parse_brl_amount(raw) == 0.0

    def test_invalid_returns_default(self) -> None:
        assert parse_brl_amount("abc") == 0.0

    def test_custom_default_none(self) -> None:
        assert parse_brl_amount("", default=None) is None
        assert parse_brl_amount("abc", default=None) is None

    def test_custom_default_sentinel(self) -> None:
        assert parse_brl_amount("abc", default=-1.0) == -1.0

    def test_accepts_non_string_input(self) -> None:
        # Strict BR treats dots as thousand-separators, even for float input:
        # ``3.14`` stringifies to ``"3.14"`` and becomes ``314.0``. Callers that
        # already have a float should pass it through unchanged instead.
        assert parse_brl_amount(42) == 42.0
        assert parse_brl_amount(3.14) == 314.0


class TestParseNumericComma:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("1234,56", 1234.56),
            ("1234.56", 1234.56),
            ("0,5", 0.5),
            ("42", 42.0),
        ],
    )
    def test_happy_paths(self, raw: str, expected: float) -> None:
        assert parse_numeric_comma(raw) == pytest.approx(expected)

    def test_does_not_strip_dots(self) -> None:
        # "1.234,56" becomes "1.234.56" after comma swap -> invalid
        assert parse_numeric_comma("1.234,56") == 0.0

    @pytest.mark.parametrize("raw", ["", "   ", None])
    def test_empty_returns_default(self, raw: str | None) -> None:
        assert parse_numeric_comma(raw) == 0.0

    def test_custom_default_none(self) -> None:
        assert parse_numeric_comma("abc", default=None) is None


class TestParseBrlFlexible:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("R$ 1.234,56", 1234.56),
            ("R$1.234,56", 1234.56),
            ("1.234,56", 1234.56),
            ("1234,56", 1234.56),
            ("1234.56", 1234.56),
            ("1000", 1000.0),
        ],
    )
    def test_happy_paths(self, raw: str, expected: float) -> None:
        assert parse_brl_flexible(raw) == pytest.approx(expected)

    @pytest.mark.parametrize("raw", ["", "   ", None, "R$"])
    def test_empty_returns_default(self, raw: str | None) -> None:
        assert parse_brl_flexible(raw) == 0.0

    def test_invalid_returns_default(self) -> None:
        assert parse_brl_flexible("abc") == 0.0

    def test_custom_default_none(self) -> None:
        assert parse_brl_flexible("", default=None) is None


class TestParseNumberSmart:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("1.234,56", 1234.56),  # BR format
            ("1234,56", 1234.56),  # BR decimal only
            ("1234.56", 1234.56),  # en decimal only
            ("1.234.567,89", 1_234_567.89),
            ("R$ 1.234,56", 1234.56),  # non-numeric stripped
        ],
    )
    def test_happy_paths(self, raw: str, expected: float) -> None:
        assert parse_number_smart(raw) == pytest.approx(expected)

    def test_en_thousand_comma_is_not_supported(self) -> None:
        # "1,234.56" keeps the comma (rightmost is dot) → float() fails → default.
        # Documented divergence from parse_brl_flexible, which handles this case.
        assert parse_number_smart("1,234.56") == 0.0

    @pytest.mark.parametrize("raw", ["", "   ", None, "R$", "abc"])
    def test_empty_or_invalid_returns_default(self, raw: str | None) -> None:
        assert parse_number_smart(raw) == 0.0

    def test_custom_default_none(self) -> None:
        assert parse_number_smart("", default=None) is None
        assert parse_number_smart("xyz", default=None) is None

    def test_negative_value(self) -> None:
        assert parse_number_smart("-1.234,56") == pytest.approx(-1234.56)


class TestNaNNotReturned:
    """Guard: parsers must never return NaN — downstream sum/avg would corrupt."""

    @pytest.mark.parametrize(
        "parser",
        [parse_brl_amount, parse_numeric_comma, parse_brl_flexible, parse_number_smart],
    )
    def test_invalid_never_returns_nan(self, parser) -> None:  # type: ignore[no-untyped-def]
        result = parser("garbage", default=0.0)
        assert not math.isnan(result)

"""Tests for transforms modules not covered by the pre-existing test_transforms.py.

test_transforms.py focuses on name_normalization, document_formatting
(format_cpf/cnpj) and deduplication. This file fills in the rest:
date_formatting, value_sanitization, and the newly-extracted
document_extraction module.
"""

from __future__ import annotations

import pytest

from bracc_etl.transforms import (
    MAX_CONTRACT_VALUE,
    cap_contract_value,
    extract_cnpjs,
    extract_cnpjs_with_spans,
    extract_cpfs,
    mask_cpf,
    parse_date,
    validate_cnpj,
    validate_cpf,
)


class TestParseDate:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("2024-01-15", "2024-01-15"),
            ("15/01/2024", "2024-01-15"),
            ("15/01/2024 10:30:45", "2024-01-15"),
            ("20240115", "2024-01-15"),
        ],
    )
    def test_parses_known_formats(self, raw: str, expected: str) -> None:
        assert parse_date(raw) == expected

    def test_strips_whitespace(self) -> None:
        assert parse_date("  2024-01-15  ") == "2024-01-15"

    def test_empty_returns_empty(self) -> None:
        assert parse_date("") == ""
        assert parse_date("   ") == ""

    def test_unparseable_returns_empty(self) -> None:
        assert parse_date("not a date") == ""
        assert parse_date("2024-13-45") == ""  # invalid month/day

    def test_american_format_not_accepted(self) -> None:
        # Safety: %m/%d/%Y isn't in the format list, so "01/15/2024" must
        # parse as DD/MM (invalid month 15) → empty.
        assert parse_date("01/15/2024") == ""


class TestCapContractValue:
    def test_threshold_is_10b(self) -> None:
        assert MAX_CONTRACT_VALUE == 10_000_000_000.0

    def test_none_passes_through(self) -> None:
        assert cap_contract_value(None) is None

    def test_zero_passes_through(self) -> None:
        assert cap_contract_value(0.0) == 0.0

    def test_small_value_passes_through(self) -> None:
        assert cap_contract_value(12345.67) == 12345.67

    def test_at_threshold_is_kept(self) -> None:
        assert cap_contract_value(MAX_CONTRACT_VALUE) == MAX_CONTRACT_VALUE

    def test_above_threshold_nulled(self) -> None:
        assert cap_contract_value(MAX_CONTRACT_VALUE + 1) is None

    def test_absurd_value_nulled(self) -> None:
        assert cap_contract_value(1e18) is None

    def test_negative_passes_through(self) -> None:
        # Negative values are out of the contract-outlier concern; leave
        # downstream to handle (e.g. signed cash flows).
        assert cap_contract_value(-42.0) == -42.0


class TestExtractCpfs:
    def test_finds_formatted_cpf(self) -> None:
        cpfs = extract_cpfs("Fulano CPF 529.982.247-25 participa do ato")
        assert cpfs == ["529.982.247-25"]

    def test_multiple_cpfs(self) -> None:
        text = "Fulano 111.222.333-44 e Beltrano 555.666.777-88"
        assert extract_cpfs(text) == ["111.222.333-44", "555.666.777-88"]

    def test_dedupes_by_digits(self) -> None:
        text = "111.222.333-44 citado e de novo 111.222.333-44"
        assert extract_cpfs(text) == ["111.222.333-44"]

    def test_ignores_unformatted_digits(self) -> None:
        # extract_cpfs only matches the formatted pattern.
        assert extract_cpfs("11122233344") == []

    def test_no_matches(self) -> None:
        assert extract_cpfs("texto sem cpf") == []


class TestExtractCnpjs:
    def test_finds_formatted_cnpj(self) -> None:
        cnpjs = extract_cnpjs("Empresa 12.345.678/0001-95 contratada")
        assert cnpjs == ["12.345.678/0001-95"]

    def test_finds_raw_14_digit_cnpj(self) -> None:
        cnpjs = extract_cnpjs("cnpj 12345678000195 na licitação")
        assert cnpjs == ["12.345.678/0001-95"]

    def test_dedupes_formatted_and_raw_pointing_to_same_entity(self) -> None:
        # Same digits in both forms must dedupe to one result.
        text = "12.345.678/0001-95 aparece, e de novo como 12345678000195"
        result = extract_cnpjs(text)
        assert result == ["12.345.678/0001-95"]

    def test_no_matches(self) -> None:
        assert extract_cnpjs("nenhum cnpj aqui") == []


class TestExtractCnpjsWithSpans:
    def test_returns_span_for_formatted(self) -> None:
        text = "prefix 12.345.678/0001-95 suffix"
        result = extract_cnpjs_with_spans(text)
        assert len(result) == 1
        cnpj, span = result[0]
        assert cnpj == "12.345.678/0001-95"
        start, end = (int(s) for s in span.split(":"))
        assert text[start:end] == "12.345.678/0001-95"

    def test_returns_span_for_raw(self) -> None:
        text = "prefix 12345678000195 suffix"
        result = extract_cnpjs_with_spans(text)
        assert len(result) == 1
        cnpj, span = result[0]
        assert cnpj == "12.345.678/0001-95"
        start, end = (int(s) for s in span.split(":"))
        assert text[start:end] == "12345678000195"

    def test_dedupes_by_digits(self) -> None:
        text = "12.345.678/0001-95 depois 12345678000195"
        result = extract_cnpjs_with_spans(text)
        # Only the first occurrence is kept.
        assert len(result) == 1


class TestDocumentFormattingReExports:
    """Smoke tests to confirm the transforms package still re-exports
    mask_cpf / validate_cpf / validate_cnpj (these have deeper tests in
    test_transforms.py / test_folha_go_pipeline.py)."""

    def test_mask_cpf_masks_last_four(self) -> None:
        assert mask_cpf("12345678901") == "***.***.*89-01"

    def test_validate_cpf_requires_check_digits(self) -> None:
        # All-zero CPF is a classic invalid sentinel.
        assert validate_cpf("00000000000") is False

    def test_validate_cnpj_requires_check_digits(self) -> None:
        assert validate_cnpj("00000000000000") is False

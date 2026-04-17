"""Unit tests for pure helpers in bracc.services.intelligence_provider.

test_intelligence_provider.py covers provider selection and end-to-end
pattern execution via HTTP. The small pure helpers (_format_cnpj,
_build_pattern_meta, _community_pattern_params, _sanitize_public_pattern_data)
are better exercised in isolation.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from bracc.config import settings
from bracc.services.intelligence_provider import (
    _build_pattern_meta,
    _community_pattern_params,
    _format_cnpj,
    _sanitize_public_pattern_data,
)


class TestFormatCnpj:
    def test_canonical_14_digits(self) -> None:
        assert _format_cnpj("12345678000195") == "12.345.678/0001-95"

    def test_pads_segments_correctly(self) -> None:
        # Each segment boundary is fixed: 2, 5, 8, 12.
        result = _format_cnpj("00011122000133")
        assert result == "00.011.122/0001-33"


class TestBuildPatternMeta:
    def test_empty_tuple(self) -> None:
        assert _build_pattern_meta(()) == []

    def test_known_pattern_has_localized_fields(self) -> None:
        result = _build_pattern_meta(("sanctioned_still_receiving",))
        assert len(result) == 1
        entry = result[0]
        assert entry["id"] == "sanctioned_still_receiving"
        # Catalog entries populate these — don't hardcode the text, just
        # ensure they're present and non-empty.
        assert entry["name_pt"]
        assert entry["name_en"]
        assert entry["description_pt"]
        assert entry["description_en"]

    def test_unknown_pattern_falls_back_to_id(self) -> None:
        result = _build_pattern_meta(("definitely_not_a_real_pattern",))
        entry = result[0]
        assert entry["id"] == "definitely_not_a_real_pattern"
        assert entry["name_pt"] == "definitely_not_a_real_pattern"
        assert entry["name_en"] == "definitely_not_a_real_pattern"
        # Descriptions default to empty string (not pid).
        assert entry["description_pt"] == ""
        assert entry["description_en"] == ""

    def test_preserves_input_order(self) -> None:
        ids = ("sanctioned_still_receiving", "debtor_contracts", "embargoed_receiving")
        result = _build_pattern_meta(ids)
        assert [r["id"] for r in result] == list(ids)


class TestCommunityPatternParams:
    def test_includes_identity_and_settings(self) -> None:
        params = _community_pattern_params("elem-1", "12345678000195", "12.345.678/0001-95")
        # Identity fields
        assert params["company_id"] == "elem-1"
        assert params["company_identifier"] == "12345678000195"
        assert params["company_identifier_formatted"] == "12.345.678/0001-95"
        # Settings fields — match the live values so tests track config changes.
        assert params["pattern_split_threshold_value"] == settings.pattern_split_threshold_value
        assert params["pattern_split_min_count"] == settings.pattern_split_min_count
        assert params["pattern_share_threshold"] == settings.pattern_share_threshold
        assert params["pattern_srp_min_orgs"] == settings.pattern_srp_min_orgs
        assert params["pattern_inexig_min_recurrence"] == settings.pattern_inexig_min_recurrence
        assert params["pattern_max_evidence_refs"] == settings.pattern_max_evidence_refs


class TestSanitizePublicPatternData:
    def _record(self, data: dict[str, object]) -> MagicMock:
        record = MagicMock()
        record.__iter__ = lambda self: iter(data.keys())
        record.__getitem__ = lambda self, key: data[key]
        return record

    def test_strips_noise_fields(self) -> None:
        record = self._record({
            "pattern_id": "sanctioned_still_receiving",
            "summary_pt": "pt text",
            "summary_en": "en text",
            "contract_count": 5,
        })
        result = _sanitize_public_pattern_data(record)
        assert "pattern_id" not in result
        assert "summary_pt" not in result
        assert "summary_en" not in result
        assert result["contract_count"] == 5

    def test_strips_sensitive_tokens(self) -> None:
        # Keys that contain any _PUBLIC_PATTERN_BLOCKLIST token must be
        # dropped — this is the LGPD safety net for public-tier output.
        record = self._record({
            "contract_count": 5,
            "person_cpf": "12345678901",
            "doc_partial": "***.***.***-01",
            "politician_name": "FULANO",
            "partner_name": "BELTRANO",
            "deputy_id": "dep1",
            "family_count": 3,
            "legislator_name": "SENADOR",
            "neutral_field": "kept",
        })
        result = _sanitize_public_pattern_data(record)
        for blocked in (
            "person_cpf",
            "doc_partial",
            "politician_name",
            "partner_name",
            "deputy_id",
            "family_count",
            "legislator_name",
        ):
            assert blocked not in result
        assert result["neutral_field"] == "kept"
        assert result["contract_count"] == 5

    def test_lists_are_stringified_per_item(self) -> None:
        record = self._record({
            "ids": [1, "two", None, "  ", 3.14],
        })
        result = _sanitize_public_pattern_data(record)
        # None filtered, whitespace-only stripped out, rest stringified.
        assert result["ids"] == ["1", "two", "3.14"]

    def test_non_list_scalars_pass_through(self) -> None:
        record = self._record({"count": 42, "amount": 99.5, "flag": True, "tag": "x"})
        result = _sanitize_public_pattern_data(record)
        assert result == {"count": 42, "amount": 99.5, "flag": True, "tag": "x"}

    def test_case_insensitive_blocklist(self) -> None:
        # Blocklist tokens are lowercased when matched.
        record = self._record({"Person_CPF": "x", "PARTNER_id": "y", "safe": "z"})
        result = _sanitize_public_pattern_data(record)
        assert "Person_CPF" not in result
        assert "PARTNER_id" not in result
        assert result["safe"] == "z"

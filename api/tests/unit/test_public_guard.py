from __future__ import annotations

import pytest
from fastapi import HTTPException

from bracc.config import settings
from bracc.services.public_guard import (
    enforce_entity_lookup_enabled,
    enforce_entity_lookup_policy,
    enforce_person_access_policy,
    ensure_investigations_enabled,
    has_person_labels,
    infer_exposure_tier,
    is_public_mode,
    sanitize_public_properties,
    should_hide_person_entities,
)


@pytest.fixture
def public_mode_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "public_mode", False)
    monkeypatch.setattr(settings, "public_allow_person", False)
    monkeypatch.setattr(settings, "public_allow_entity_lookup", False)
    monkeypatch.setattr(settings, "public_allow_investigations", False)


@pytest.fixture
def public_mode_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    # Public mode ON with all allowances OFF — the LGPD-default stance.
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_person", False)
    monkeypatch.setattr(settings, "public_allow_entity_lookup", False)
    monkeypatch.setattr(settings, "public_allow_investigations", False)


class TestIsPublicMode:
    def test_reflects_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        assert is_public_mode() is True
        monkeypatch.setattr(settings, "public_mode", False)
        assert is_public_mode() is False


class TestShouldHidePersonEntities:
    def test_false_when_private_mode(self, public_mode_off: None) -> None:
        assert should_hide_person_entities() is False

    def test_true_when_public_without_person_allowance(
        self, public_mode_strict: None
    ) -> None:
        assert should_hide_person_entities() is True

    def test_false_when_public_with_person_allowance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(settings, "public_allow_person", True)
        assert should_hide_person_entities() is False


class TestHasPersonLabels:
    def test_detects_person(self) -> None:
        assert has_person_labels(["Person"]) is True

    def test_detects_partner(self) -> None:
        assert has_person_labels(["Company", "Partner"]) is True

    def test_ignores_company_only(self) -> None:
        assert has_person_labels(["Company", "Contract"]) is False

    def test_empty(self) -> None:
        assert has_person_labels([]) is False


class TestInferExposureTier:
    def test_internal_takes_precedence(self) -> None:
        # A Person+Investigation node is still internal_only.
        assert infer_exposure_tier(["Person", "Investigation"]) == "internal_only"

    def test_person_labels_are_restricted(self) -> None:
        assert infer_exposure_tier(["Person"]) == "restricted"
        assert infer_exposure_tier(["Partner", "Company"]) == "restricted"

    def test_user_is_internal(self) -> None:
        assert infer_exposure_tier(["User"]) == "internal_only"

    def test_investigation_is_internal(self) -> None:
        assert infer_exposure_tier(["Investigation"]) == "internal_only"

    def test_plain_company_is_public_safe(self) -> None:
        assert infer_exposure_tier(["Company"]) == "public_safe"

    def test_empty_is_public_safe(self) -> None:
        assert infer_exposure_tier([]) == "public_safe"


class TestSanitizePublicProperties:
    def test_private_mode_is_passthrough(self, public_mode_off: None) -> None:
        props: dict[str, str | float | int | bool | None] = {
            "cpf": "12345678901",
            "razao_social": "Acme",
        }
        assert sanitize_public_properties(props) == props

    def test_public_mode_strips_sensitive_keys(
        self, public_mode_strict: None
    ) -> None:
        props: dict[str, str | float | int | bool | None] = {
            "cpf": "12345678901",
            "doc_partial": "***.***.***-01",
            "doc_raw": "12345678901",
            "masked_doc": "***.***.***-01",
            "razao_social": "Acme",
            "cnpj": "12345678000195",
        }
        out = sanitize_public_properties(props)
        assert "cpf" not in out
        assert "doc_partial" not in out
        assert "doc_raw" not in out
        assert "masked_doc" not in out
        # Non-sensitive keys remain.
        assert out["razao_social"] == "Acme"
        assert out["cnpj"] == "12345678000195"

    def test_public_mode_strips_any_cpf_substring_key(
        self, public_mode_strict: None
    ) -> None:
        props: dict[str, str | float | int | bool | None] = {
            "person_cpf_raw": "12345678901",
            "name": "Fulano",
        }
        out = sanitize_public_properties(props)
        assert "person_cpf_raw" not in out
        assert out["name"] == "Fulano"


class TestEnforceEntityLookupEnabled:
    def test_allowed_when_private_mode(self, public_mode_off: None) -> None:
        enforce_entity_lookup_enabled()  # does not raise

    def test_forbidden_when_public_mode_lookup_disabled(
        self, public_mode_strict: None
    ) -> None:
        with pytest.raises(HTTPException) as exc:
            enforce_entity_lookup_enabled()
        assert exc.value.status_code == 403

    def test_allowed_when_public_mode_lookup_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(settings, "public_allow_entity_lookup", True)
        enforce_entity_lookup_enabled()  # does not raise


class TestEnforceEntityLookupPolicy:
    def test_private_mode_allows_anything(self, public_mode_off: None) -> None:
        enforce_entity_lookup_policy("12.345.678/0001-95")
        enforce_entity_lookup_policy("123.456.789-01")
        enforce_entity_lookup_policy("not-a-document")  # no-op in private mode

    def test_rejects_cpf_in_strict_public(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(settings, "public_allow_entity_lookup", True)
        monkeypatch.setattr(settings, "public_allow_person", False)
        with pytest.raises(HTTPException) as exc:
            enforce_entity_lookup_policy("123.456.789-01")
        assert exc.value.status_code == 403
        assert "Person lookup" in str(exc.value.detail)

    def test_accepts_cnpj_in_public_with_lookup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(settings, "public_allow_entity_lookup", True)
        monkeypatch.setattr(settings, "public_allow_person", False)
        # Valid CNPJ shape — should not raise.
        enforce_entity_lookup_policy("12.345.678/0001-95")

    def test_rejects_invalid_format(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(settings, "public_allow_entity_lookup", True)
        monkeypatch.setattr(settings, "public_allow_person", True)
        with pytest.raises(HTTPException) as exc:
            enforce_entity_lookup_policy("abc")
        assert exc.value.status_code == 400

    def test_cpf_allowed_when_person_flag_on(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(settings, "public_allow_entity_lookup", True)
        monkeypatch.setattr(settings, "public_allow_person", True)
        enforce_entity_lookup_policy("123.456.789-01")  # does not raise

    def test_rejects_lookup_when_feature_disabled(
        self, public_mode_strict: None
    ) -> None:
        # Even well-formed CNPJ must be rejected when lookup feature itself is off.
        with pytest.raises(HTTPException) as exc:
            enforce_entity_lookup_policy("12.345.678/0001-95")
        assert exc.value.status_code == 403


class TestEnforcePersonAccessPolicy:
    def test_private_mode_always_allows(self, public_mode_off: None) -> None:
        enforce_person_access_policy(["Person"])
        enforce_person_access_policy(["Company"])

    def test_public_with_person_label_forbids(
        self, public_mode_strict: None
    ) -> None:
        with pytest.raises(HTTPException) as exc:
            enforce_person_access_policy(["Person"])
        assert exc.value.status_code == 403

    def test_public_with_non_person_label_allows(
        self, public_mode_strict: None
    ) -> None:
        enforce_person_access_policy(["Company"])

    def test_public_with_person_allowance_allows(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(settings, "public_allow_person", True)
        enforce_person_access_policy(["Person"])


class TestEnsureInvestigationsEnabled:
    def test_private_mode_allows(self, public_mode_off: None) -> None:
        ensure_investigations_enabled()

    def test_public_mode_default_forbids(self, public_mode_strict: None) -> None:
        with pytest.raises(HTTPException) as exc:
            ensure_investigations_enabled()
        assert exc.value.status_code == 403

    def test_public_with_investigations_flag_allows(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "public_mode", True)
        monkeypatch.setattr(
            settings, "public_allow_investigations", True
        )
        ensure_investigations_enabled()

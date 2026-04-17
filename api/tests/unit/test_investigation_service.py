"""Direct unit tests for `bracc.services.investigation_service`.

Complements the HTTP-level coverage in ``test_investigation.py`` by pinning the
record-converter helpers and the branches in async functions that depend on
``None`` records or typed-result coercions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

if TYPE_CHECKING:
    from collections.abc import Mapping

from bracc.services import investigation_service
from bracc.services.investigation_service import (
    _record_to_annotation,
    _record_to_investigation,
    _record_to_tag,
    _str,
    add_entity_to_investigation,
    generate_share_token,
    get_by_share_token,
    remove_entity_from_investigation,
    revoke_share_token,
)


def _record(data: Mapping[str, object]) -> MagicMock:
    """Build a MagicMock that quacks like neo4j.Record for __getitem__ access."""
    rec = MagicMock()
    rec.__getitem__.side_effect = lambda key: data[key]
    return rec


# --- _str ---


class TestStr:
    def test_none_returns_empty_string(self) -> None:
        assert _str(None) == ""

    def test_value_is_coerced_via_str(self) -> None:
        assert _str(42) == "42"
        assert _str(datetime(2026, 1, 1, tzinfo=UTC)) == "2026-01-01 00:00:00+00:00"


# --- _record_to_investigation ---


class TestRecordToInvestigation:
    def _base(self) -> dict[str, Any]:
        return {
            "id": "inv-1",
            "title": "T",
            "description": "D",
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 1, 2, tzinfo=UTC),
            "entity_ids": ["e1"],
            "share_token": None,
            "share_expires_at": None,
        }

    def test_maps_all_fields(self) -> None:
        overrides: dict[str, Any] = {
            "share_token": "tok",
            "share_expires_at": datetime(2026, 1, 3, tzinfo=UTC),
        }
        record = _record(self._base() | overrides)
        inv = _record_to_investigation(record)
        assert inv.id == "inv-1"
        assert inv.share_token == "tok"
        assert inv.share_expires_at == "2026-01-03 00:00:00+00:00"
        assert inv.created_at == "2026-01-01 00:00:00+00:00"

    def test_missing_share_expires_at_key_is_tolerated(self) -> None:
        data = self._base()
        data.pop("share_expires_at")
        rec = MagicMock()
        rec.__getitem__.side_effect = lambda key: data[key]
        # Force KeyError path for "share_expires_at"
        original = rec.__getitem__.side_effect

        def raise_on_missing(key: str) -> object:
            if key == "share_expires_at":
                raise KeyError(key)
            return original(key)

        rec.__getitem__.side_effect = raise_on_missing
        inv = _record_to_investigation(rec)
        assert inv.share_expires_at is None

    def test_none_share_expires_at_maps_to_none(self) -> None:
        inv = _record_to_investigation(_record(self._base()))
        assert inv.share_expires_at is None
        assert inv.share_token is None


# --- _record_to_annotation ---


def test_record_to_annotation_maps_all_fields() -> None:
    record = _record(
        {
            "id": "ann-1",
            "entity_id": "ent-1",
            "investigation_id": "inv-1",
            "text": "hello",
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
    )
    ann = _record_to_annotation(record)
    assert ann.id == "ann-1"
    assert ann.entity_id == "ent-1"
    assert ann.investigation_id == "inv-1"
    assert ann.text == "hello"
    assert ann.created_at == "2026-01-01 00:00:00+00:00"


# --- _record_to_tag ---


def test_record_to_tag_maps_all_fields() -> None:
    record = _record(
        {
            "id": "tag-1",
            "investigation_id": "inv-1",
            "name": "reviewed",
            "color": "#abcdef",
        }
    )
    tag = _record_to_tag(record)
    assert tag.id == "tag-1"
    assert tag.investigation_id == "inv-1"
    assert tag.name == "reviewed"
    assert tag.color == "#abcdef"


# --- Async service functions with non-trivial branches ---


@pytest.mark.anyio
async def test_add_entity_returns_true_when_record_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=_record({"investigation_id": "i", "entity_id": "e"})),
    )
    assert await add_entity_to_investigation(AsyncMock(), "i", "e", "u") is True


@pytest.mark.anyio
async def test_add_entity_returns_false_when_no_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=None),
    )
    assert await add_entity_to_investigation(AsyncMock(), "i", "e", "u") is False


@pytest.mark.anyio
async def test_remove_entity_returns_false_when_no_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=None),
    )
    assert await remove_entity_from_investigation(AsyncMock(), "i", "e", "u") is False


@pytest.mark.anyio
async def test_remove_entity_returns_true_when_deleted_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=_record({"deleted": 1})),
    )
    assert await remove_entity_from_investigation(AsyncMock(), "i", "e", "u") is True


@pytest.mark.anyio
async def test_remove_entity_returns_false_when_deleted_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=_record({"deleted": 0})),
    )
    assert await remove_entity_from_investigation(AsyncMock(), "i", "e", "u") is False


@pytest.mark.anyio
async def test_generate_share_token_returns_none_when_record_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=None),
    )
    assert await generate_share_token(AsyncMock(), "i", "u") is None


@pytest.mark.anyio
async def test_generate_share_token_returns_token_and_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expires = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=_record({"share_token": "tok", "share_expires_at": expires})),
    )
    result = await generate_share_token(AsyncMock(), "i", "u")
    assert result == ("tok", "2026-01-01 00:00:00+00:00")


@pytest.mark.anyio
async def test_generate_share_token_tolerates_missing_expires_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rec = MagicMock()

    def getitem(key: str) -> object:
        if key == "share_token":
            return "tok"
        raise KeyError(key)

    rec.__getitem__.side_effect = getitem
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=rec),
    )
    assert await generate_share_token(AsyncMock(), "i", "u") == ("tok", None)


@pytest.mark.anyio
async def test_revoke_share_token_returns_false_when_no_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=None),
    )
    assert await revoke_share_token(AsyncMock(), "i", "u") is False


@pytest.mark.anyio
async def test_revoke_share_token_respects_updated_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=_record({"updated": 1})),
    )
    assert await revoke_share_token(AsyncMock(), "i", "u") is True

    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=_record({"updated": 0})),
    )
    assert await revoke_share_token(AsyncMock(), "i", "u") is False


@pytest.mark.anyio
async def test_get_by_share_token_returns_none_when_record_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=None),
    )
    assert await get_by_share_token(AsyncMock(), "t") is None


@pytest.mark.anyio
async def test_get_by_share_token_converts_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = _record(
        {
            "id": "inv-1",
            "title": "T",
            "description": "D",
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 1, 2, tzinfo=UTC),
            "entity_ids": [],
            "share_token": "tok",
            "share_expires_at": datetime(2026, 1, 3, tzinfo=UTC),
        }
    )
    monkeypatch.setattr(
        investigation_service,
        "execute_query_single",
        AsyncMock(return_value=record),
    )
    inv = await get_by_share_token(AsyncMock(), "tok")
    assert inv is not None
    assert inv.id == "inv-1"
    assert inv.share_token == "tok"

import sys
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pytest

from bracc.models.investigation import Annotation, InvestigationResponse, Tag

FAKE_PDF = b"%PDF-1.4 fake pdf content for testing"


def _make_investigation(**overrides: object) -> InvestigationResponse:
    defaults: dict[str, Any] = {
        "id": "inv-1",
        "title": "Test Investigation",
        "description": "Test description",
        "created_at": "2026-01-15T10:00:00Z",
        "updated_at": "2026-01-15T10:00:00Z",
        "entity_ids": [],
        "share_token": None,
    }
    defaults.update(overrides)
    return InvestigationResponse(**defaults)


def _make_annotation(**overrides: object) -> Annotation:
    defaults: dict[str, Any] = {
        "id": "ann-1",
        "entity_id": "ent-1",
        "investigation_id": "inv-1",
        "text": "Annotation text",
        "created_at": "2026-01-15T12:00:00Z",
    }
    defaults.update(overrides)
    return Annotation(**defaults)


def _make_tag(**overrides: object) -> Tag:
    defaults: dict[str, Any] = {
        "id": "tag-1",
        "investigation_id": "inv-1",
        "name": "reviewed",
        "color": "#3498db",
    }
    defaults.update(overrides)
    return Tag(**defaults)


@pytest.fixture(autouse=True)
def _mock_weasyprint() -> object:
    """Install a fake weasyprint module so tests run without system libraries."""
    mock_html_cls = MagicMock()
    mock_html_cls.return_value.write_pdf.return_value = FAKE_PDF

    fake_module = ModuleType("weasyprint")
    fake_module.HTML = mock_html_cls  # type: ignore[attr-defined]

    sys.modules["weasyprint"] = fake_module
    yield
    sys.modules.pop("weasyprint", None)


@pytest.mark.anyio
async def test_render_pdf_produces_valid_pdf() -> None:
    from bracc.services.pdf_service import render_investigation_pdf

    investigation = _make_investigation()
    annotations = [_make_annotation()]
    tags = [_make_tag()]
    entities = [{"name": "Test Entity", "type": "Person", "document": "***.***.***-34"}]

    result = await render_investigation_pdf(
        investigation, annotations, tags, entities, lang="pt"
    )

    assert isinstance(result, bytes)
    assert result[:5] == b"%PDF-"


@pytest.mark.anyio
async def test_render_pdf_handles_empty_data() -> None:
    from bracc.services.pdf_service import render_investigation_pdf

    investigation = _make_investigation(description=None)

    result = await render_investigation_pdf(investigation, [], [], [], lang="pt")

    assert isinstance(result, bytes)
    assert result[:5] == b"%PDF-"


@pytest.mark.anyio
async def test_render_pdf_lang_pt() -> None:
    from bracc.services.pdf_service import render_investigation_pdf

    investigation = _make_investigation()

    result = await render_investigation_pdf(investigation, [], [], [], lang="pt")

    assert isinstance(result, bytes)
    assert result[:5] == b"%PDF-"


@pytest.mark.anyio
async def test_render_pdf_lang_en() -> None:
    from bracc.services.pdf_service import render_investigation_pdf

    investigation = _make_investigation()

    result = await render_investigation_pdf(investigation, [], [], [], lang="en")

    assert isinstance(result, bytes)
    assert result[:5] == b"%PDF-"


def test_get_labels_returns_pt_by_default() -> None:
    from bracc.services.pdf_service import _get_labels

    labels = _get_labels("pt")
    assert labels["label_created"] == "Criado em"
    assert "Dados compilados" in labels["disclaimer"]


def test_get_labels_returns_en_when_requested() -> None:
    from bracc.services.pdf_service import _get_labels

    labels = _get_labels("en")
    assert labels["label_created"] == "Created at"
    assert "Data compiled" in labels["disclaimer"]


def test_get_labels_unknown_lang_falls_back_to_pt() -> None:
    """Unrecognised language codes must fall back to pt (not crash or return en)."""
    from bracc.services.pdf_service import _get_labels

    labels = _get_labels("es")
    assert labels["label_created"] == "Criado em"  # pt value


@pytest.mark.anyio
async def test_render_pdf_passes_structured_data_to_template() -> None:
    """Regression guard: template.render receives annotations/tags/entities shaped
    as dicts derived from the domain objects (not raw models)."""
    from bracc.services import pdf_service

    captured: dict[str, object] = {}

    def _capture(**ctx: object) -> str:
        captured.update(ctx)
        return "<html></html>"

    mock_template = MagicMock()
    mock_template.render.side_effect = _capture
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(pdf_service._env, "get_template", lambda *_: mock_template)
        await pdf_service.render_investigation_pdf(
            _make_investigation(description="desc"),
            [_make_annotation(text="note")],
            [_make_tag(name="reviewed", color="#123456")],
            [{"name": "Entity X", "type": "Company", "document": "11.222.333/0001-81"}],
            lang="pt",
        )

    assert captured["title"] == "Test Investigation"
    assert captured["description"] == "desc"
    assert captured["tags"] == [{"name": "reviewed", "color": "#123456"}]
    # Annotations are converted to {created_at, text} dicts (no id, no entity_id).
    annotations_passed = captured["annotations"]
    assert isinstance(annotations_passed, list)
    assert annotations_passed == [
        {"created_at": "2026-01-15T12:00:00Z", "text": "note"}
    ]
    # pt labels merged into context
    assert captured["label_created"] == "Criado em"


@pytest.mark.anyio
async def test_render_pdf_coerces_none_description_to_empty() -> None:
    from bracc.services import pdf_service

    captured: dict[str, object] = {}

    def _capture(**ctx: object) -> str:
        captured.update(ctx)
        return "<html></html>"

    mock_template = MagicMock()
    mock_template.render.side_effect = _capture
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(pdf_service._env, "get_template", lambda *_: mock_template)
        await pdf_service.render_investigation_pdf(
            _make_investigation(description=None), [], [], [], lang="pt",
        )

    assert captured["description"] == ""

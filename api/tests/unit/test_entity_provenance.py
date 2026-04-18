"""Unit tests for ProvenanceBlock extraction in entity router."""

from __future__ import annotations

from typing import Any

from bracc.models.entity import ProvenanceBlock
from bracc.routers.entity import _extract_provenance, _node_to_entity


def _stamped_props(**extra: Any) -> dict[str, Any]:
    return {
        "cnpj": "12345678000195",
        "razao_social": "ACME LTDA",
        "source_id": "pncp_go",
        "source_record_id": "12345678000195|2024|42",
        "source_url": "https://pncp.gov.br/record/42",
        "ingested_at": "2026-04-17T12:34:56+00:00",
        "run_id": "pncp_go_20260417123456",
        **extra,
    }


class TestExtractProvenance:
    def test_returns_block_when_all_fields_present(self) -> None:
        props = _stamped_props()
        block = _extract_provenance(props)
        assert isinstance(block, ProvenanceBlock)
        assert block.source_id == "pncp_go"
        assert block.source_record_id == "12345678000195|2024|42"
        assert block.source_url.startswith("https://")
        assert block.ingested_at.startswith("20")
        assert block.run_id.startswith("pncp_go_")
        # props were popped
        for field in ("source_id", "source_url", "ingested_at", "run_id"):
            assert field not in props

    def test_returns_none_when_required_field_missing(self) -> None:
        props = _stamped_props()
        del props["source_url"]
        assert _extract_provenance(props) is None

    def test_returns_none_on_empty_required_field(self) -> None:
        props = _stamped_props(source_id="")
        assert _extract_provenance(props) is None

    def test_allows_empty_source_record_id(self) -> None:
        props = _stamped_props(source_record_id="")
        block = _extract_provenance(props)
        assert block is not None
        assert block.source_record_id is None

    def test_legacy_props_without_provenance_yield_none(self) -> None:
        props: dict[str, Any] = {"cnpj": "123", "razao_social": "Old LTDA"}
        assert _extract_provenance(props) is None
        # props untouched (legacy data still flows)
        assert "cnpj" in props
        assert "razao_social" in props

    def test_block_without_snapshot_has_null_snapshot_url(self) -> None:
        # Pipelines legados não gravam source_snapshot_uri — API deve
        # serializar snapshot_url=None mas seguir retornando o bloco.
        props = _stamped_props()
        assert "source_snapshot_uri" not in props
        block = _extract_provenance(props)
        assert block is not None
        assert block.snapshot_url is None
        # O modelo serializa o campo (explicitamente) como None.
        assert block.model_dump()["snapshot_url"] is None

    def test_block_with_snapshot_uri_surfaces_as_snapshot_url(self) -> None:
        props = _stamped_props(source_snapshot_uri="pncp_go/2026-04/abcdef123456.json")
        block = _extract_provenance(props)
        assert block is not None
        assert block.snapshot_url == "pncp_go/2026-04/abcdef123456.json"
        # source_snapshot_uri popped off properties dict
        assert "source_snapshot_uri" not in props
        assert block.model_dump()["snapshot_url"] == "pncp_go/2026-04/abcdef123456.json"

    def test_empty_source_snapshot_uri_treated_as_none(self) -> None:
        # Se alguém gravou string vazia por engano, tratamos como ausência.
        props = _stamped_props(source_snapshot_uri="")
        block = _extract_provenance(props)
        assert block is not None
        assert block.snapshot_url is None


class TestNodeToEntityProvenance:
    def test_entity_carries_provenance(self) -> None:
        node = _stamped_props()
        entity = _node_to_entity(node, ["Company"], "12345678000195")
        assert entity.provenance is not None
        assert entity.provenance.source_id == "pncp_go"
        # Provenance fields stripped from properties
        assert "source_id" not in entity.properties
        assert "source_url" not in entity.properties
        assert "ingested_at" not in entity.properties
        assert "run_id" not in entity.properties

    def test_legacy_entity_has_null_provenance(self) -> None:
        node = {"cnpj": "12345678000195", "razao_social": "Old LTDA"}
        entity = _node_to_entity(node, ["Company"], "12345678000195")
        assert entity.provenance is None

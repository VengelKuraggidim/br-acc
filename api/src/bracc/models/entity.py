from typing import Any

from pydantic import BaseModel


class SourceAttribution(BaseModel):
    database: str
    record_id: str | None = None
    extracted_at: str | None = None


class ProvenanceBlock(BaseModel):
    """Per-record traceability fields. See ``docs/provenance.md``.

    Present on every node/relationship persisted under the current
    contract. May be absent for legacy data loaded before the contract
    was in force, in which case the field is ``None``.

    ``snapshot_url`` is the opt-in archival snapshot URI (content-addressed
    raw copy of the source payload at ingestion time). Present when the
    pipeline carimbou ``source_snapshot_uri`` via ``archive_fetch``. Ver
    ``docs/archival.md``. ``None`` para dados legados sem archival.
    """

    source_id: str
    source_record_id: str | None = None
    source_url: str
    ingested_at: str
    run_id: str
    snapshot_url: str | None = None


class EntityResponse(BaseModel):
    id: str
    type: str
    entity_label: str | None = None
    identity_quality: str | None = None
    properties: dict[str, str | float | int | bool | None]
    sources: list[SourceAttribution]
    provenance: ProvenanceBlock | None = None
    is_pep: bool = False
    exposure_tier: str = "public_safe"


class ConnectionResponse(BaseModel):
    source_id: str
    target_id: str
    relationship_type: str
    properties: dict[str, str | float | int | bool | None]
    confidence: float = 1.0
    sources: list[SourceAttribution]
    provenance: ProvenanceBlock | None = None
    exposure_tier: str = "public_safe"


class EntityWithConnections(BaseModel):
    entity: EntityResponse
    connections: list[ConnectionResponse]
    connected_entities: list[EntityResponse]


class ExposureFactor(BaseModel):
    name: str
    value: float
    percentile: float
    weight: float
    sources: list[str]


class ExposureResponse(BaseModel):
    entity_id: str
    exposure_index: float
    factors: list[ExposureFactor]
    peer_group: str
    peer_count: int
    sources: list[SourceAttribution]
    intelligence_tier: str = "community"


class TimelineEvent(BaseModel):
    id: str
    date: str
    label: str
    entity_type: str
    properties: dict[str, Any]
    sources: list[SourceAttribution]
    provenance: ProvenanceBlock | None = None


class TimelineResponse(BaseModel):
    entity_id: str
    events: list[TimelineEvent]
    total: int
    next_cursor: str | None

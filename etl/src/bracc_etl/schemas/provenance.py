"""Provenance columns common to all Fiscal Cidadão ETL outputs.

Every node and relationship persisted to Neo4j must carry five fields that
let a user trace the fact back to its origin:

- ``source_id``         (str)  Registry key (``docs/source_registry_br_v1.csv``).
- ``source_record_id``  (str)  Stable natural id on the source (CPF, resource_id, ...).
                               May be empty when the dump has no natural key.
- ``source_url``        (str)  Deep-link when the source exposes one, otherwise a
                               hierarchical fallback. Never empty.
- ``ingested_at``       (str)  ISO 8601 UTC timestamp of the ingestion run.
- ``run_id``            (str)  Correlates with ``IngestionRun`` nodes.

The :func:`with_provenance` helper merges these columns into any existing
pandera :class:`pa.DataFrameSchema`, so business-level schemas can declare
their own columns and still enforce the provenance contract.
"""

from __future__ import annotations

from typing import Literal, cast

import pandera.pandas as pa

PROVENANCE_COLUMNS: dict[str, pa.Column] = {
    "source_id": pa.Column(
        str,
        nullable=False,
        coerce=True,
        checks=[pa.Check.str_length(min_value=1, error="source_id must not be empty")],
    ),
    "source_record_id": pa.Column(
        str,
        nullable=True,
        coerce=True,
    ),
    "source_url": pa.Column(
        str,
        nullable=False,
        coerce=True,
        checks=[
            pa.Check.str_startswith(
                "http",
                error="source_url must be an http(s) URL",
            ),
        ],
    ),
    "ingested_at": pa.Column(
        str,
        nullable=False,
        coerce=True,
        checks=[
            pa.Check.str_matches(
                r"^\d{4}-\d{2}-\d{2}T",
                error="ingested_at must be ISO 8601 (YYYY-MM-DDT...)",
            ),
        ],
    ),
    "run_id": pa.Column(
        str,
        nullable=False,
        coerce=True,
        checks=[pa.Check.str_length(min_value=1, error="run_id must not be empty")],
    ),
}

PROVENANCE_FIELDS: tuple[str, ...] = tuple(PROVENANCE_COLUMNS.keys())


def with_provenance(schema: pa.DataFrameSchema) -> pa.DataFrameSchema:
    """Return a new schema with the provenance columns merged in.

    Preserves the original schema's ``coerce`` and ``strict`` settings.
    If a business-level column collides with a provenance name, the
    business definition wins — but pipelines should not redeclare
    provenance columns.
    """
    merged = {**PROVENANCE_COLUMNS, **schema.columns}
    return pa.DataFrameSchema(
        columns=merged,
        coerce=schema.coerce,
        strict=cast("bool | Literal['filter']", schema.strict),
    )

# Provenance contract

Every node and relationship persisted to Neo4j by a Fiscal Cidadão pipeline
must carry five fields so an end-user can trace any fact back to its
origin in the source system.

This is a product requirement: Fiscal Cidadão surfaces connections between
public entities and must not show data without a verifiable path back to
the official dump or portal that produced it.

## The five fields

| Field | Type | Nullable | Purpose |
| --- | --- | --- | --- |
| `source_id` | `str` | no | Registry key (see `docs/source_registry_br_v1.csv`). Must not be empty. |
| `source_record_id` | `str` | yes (`""`) | Stable natural id on the source. Empty when the dump has no natural key. |
| `source_url` | `str` | no | Deep-link to the record, or a hierarchical fallback. Must start with `http`. |
| `ingested_at` | `str` | no | ISO 8601 UTC timestamp of the ingestion run (e.g. `2026-04-17T12:34:56+00:00`). |
| `run_id` | `str` | no | Correlates with `IngestionRun` nodes. Typically `f"{source_id}_{YYYYMMDDHHMMSS}"`. |

## Rules for `source_record_id`

1. **Natural id exists** — use the source's id as a string (CKAN
   `resource_id`, CPF for `folha_go`, `numero_ato` for DOU, …).
2. **Composite id** (e.g. PNCP `cnpj:year:sequential`) — use the raw
   composite, not a hash. The raw form is verifiable on the portal; the
   hash only exists in our graph.
3. **No natural id** — fall back to `f"{filename}#row={N}"`, computed
   **after** deterministic sorting of the dataframe so the same record
   always gets the same row index across re-runs.
4. **Never `None` / `NaN`** — use an empty string `""`. Neo4j has no
   property-level `NULL`, so an empty string is the only uniform way to
   mean "the source did not expose an id for this row".

## Rules for `source_url`

Hierarchical fallback — always produce a non-empty `http(s)` URL:

1. **Record-specific deep-link**, if the source exposes one
   (e.g. `https://www.in.gov.br/web/dou/-/{url_title}` for DOU).
2. **Registry primary_url + query fragment** when the source has no
   deep-link but exposes a resource identifier
   (e.g. `{primary_url}?resource_id={resource_id}`).
3. **`primary_url` from the source registry**. The user lands on the
   source's main page and confirms the dataset manually — worse than a
   deep-link, but still traceable.

`source_url` is **never** empty. If all three fallbacks fail, the
pipeline must raise rather than emit a record.

## Storage

Provenance fields live as **flat properties** on each node and
relationship. No nested objects, no separate `:Source` node joined by
a `:FROM_SOURCE` relationship.

Rationale: the source registry already holds per-source metadata
(name, tier, cadence, primary_url) — `source_id` on the node is enough
to join back to that catalog in the API layer. Flat properties cost
zero extra hops in Cypher queries and ~5 extra props per node is
negligible at 10M-scale.

## Validation

- `bracc_etl.schemas.provenance.with_provenance(schema)` merges the
  five columns into any existing pandera `DataFrameSchema`. Apply it
  to business-level schemas so validation happens in a single pass.
- `bracc_etl.base.Pipeline.attach_provenance(row, record_id, url)`
  (coming in commit 2) is the canonical way to stamp rows inside
  a pipeline's `transform()`.
- `Neo4jBatchLoader` (commit 2) validates presence of the five fields
  before writing. Controlled by `BRACC_PROVENANCE_MODE=warn|strict`
  (default `warn` during rollout, `strict` post-migration).
- A governance CI job (`provenance-contract-audit`) will later refuse
  PRs that introduce pipelines missing the contract.

## API surface

- `api/src/bracc/models/entity.py` exposes `ProvenanceBlock` with the
  same five fields on every `EntityResponse` and `ConnectionResponse`.
- The legacy `SourceAttribution` (`database` / `record_id` /
  `extracted_at`) stays for backwards compatibility; new clients
  should read the richer `provenance` block.

## Migration

Pipelines already loaded into Neo4j do not carry the five fields.
Strategy:

1. Re-run each pipeline (MERGE is idempotent) ordered by business
   value: `folha_go`, `pncp_go`, `dou` first.
2. Loader runs in `BRACC_PROVENANCE_MODE=warn` during rollout so
   legacy data keeps loading while new data is validated.
3. Once `MATCH (n) WHERE n.source_id IS NULL` hits zero, flip to
   `strict`.

Provenance is never back-filled with placeholder values — a missing
`source_url` or `source_record_id` would be worse than no claim at
all, because it would falsely imply traceability.

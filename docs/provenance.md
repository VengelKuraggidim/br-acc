# Provenance contract

Every node and relationship persisted to Neo4j by a Fiscal Cidadão pipeline
must carry five required fields (plus one optional sixth) so an end-user
can trace any fact back to its origin in the source system — and, when
available, to an immutable raw-payload snapshot.

This is a product requirement: Fiscal Cidadão surfaces connections between
public entities and must not show data without a verifiable path back to
the official dump or portal that produced it.

## The five required fields

| Field | Type | Nullable | Purpose |
| --- | --- | --- | --- |
| `source_id` | `str` | no | Registry key (see `docs/source_registry_br_v1.csv`). Must not be empty. |
| `source_record_id` | `str` | yes (`""`) | Stable natural id on the source. Empty when the dump has no natural key. |
| `source_url` | `str` | no | Deep-link to the record, or a hierarchical fallback. Must start with `http`. |
| `ingested_at` | `str` | no | ISO 8601 UTC timestamp of the ingestion run (e.g. `2026-04-17T12:34:56+00:00`). |
| `run_id` | `str` | no | Correlates with `IngestionRun` nodes. Typically `f"{source_id}_{YYYYMMDDHHMMSS}"`. |

## The optional sixth field — `source_snapshot_uri`

| Field | Type | Nullable | Purpose |
| --- | --- | --- | --- |
| `source_snapshot_uri` | `str \| None` | yes | URI relativa de um snapshot content-addressed do payload bruto que produziu o row. Preenchido via `bracc_etl.archival.archive_fetch`. Ver [`archival.md`](archival.md). |

Opt-in: pipelines novos **devem** popular via `attach_provenance(snapshot_uri=…)`;
os 10 pipelines GO legados foram retrofitados em 2026-04-18 e também o populam
(ver tabela em `archival.md`). Não entra em `_REQUIRED_PROVENANCE_FIELDS`,
então pipelines pré-archival que não conseguem snapshot (ex.: dumps multi-GB
servidos via `script_download` direto pra disco) continuam válidos sem o campo.

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

- `api/src/bracc/models/entity.py` exposes `ProvenanceBlock` com os
  cinco campos requeridos + `snapshot_url` (nullable) em todo
  `EntityResponse` e `ConnectionResponse`.
- O legacy `SourceAttribution` (`database` / `record_id` /
  `extracted_at`) permanece por compatibilidade; novos clientes devem
  ler o bloco `provenance` mais rico.

## Migration — status 2026-04-18

Os 10 pipelines GO legados (`folha_go`, `pncp_go`, `alego`, `ssp_go`,
`tcmgo_sancoes`, `state_portal_go`, `querido_diario_go`, `camara_goiania`,
`tce_go`, `tcm_go`) já foram migrados em duas ondas:

1. **Provenance básica** (commit `d4a0a56` + restos do contrato) — os
   cinco campos requeridos.
2. **Archival snapshot** (2026-04-18) — `source_snapshot_uri` adicionado
   via `archive_fetch`. Ver tabela completa em
   [`archival.md`](archival.md#retrofit-nos-10-pipelines-go-legados--concluído-2026-04-18).

`Neo4jBatchLoader` continua suportando `BRACC_PROVENANCE_MODE=warn|strict`
(default `warn`) para futuras migrações de pipelines federais (DOU etc.).

Provenance é **nunca** back-filled com placeholders — um `source_url` ou
`source_record_id` ausente vira string vazia (ou `None` no opt-in), nunca
um valor sintético. Falsa rastreabilidade é pior que ausência de claim.

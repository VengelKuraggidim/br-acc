# Bootstrap GO

`bootstrap-go` is a filtered variant of [`bootstrap-all`](bootstrap_all.md) that
only ingests Goias-scoped sources. It shares the same orchestrator
(`scripts/run_bootstrap_all.py`) but uses a different contract
(`config/bootstrap_go_contract.yml`, declared as `contract_mode: subset`) and
writes reports to `audit-results/bootstrap-go/` instead of
`audit-results/bootstrap-all/`.

## Commands

```bash
# interactive reset prompt (default)
make bootstrap-go

# noninteractive reset for automation
make bootstrap-go-noninteractive

# print latest summary report
make bootstrap-go-report
```

## What It Does

1. Starts Docker services for Neo4j, API, and frontend.
2. Waits for Neo4j and API health.
3. Prompts whether to reset the local graph (`yes/no`) unless noninteractive flags are set.
4. Loads source contract from `config/bootstrap_go_contract.yml`.
5. Validates that every source declared in the contract is `implemented`
   in `docs/source_registry_br_v1.csv` (subset parity, not full parity).
6. Attempts the declared GO pipelines in contract order.
7. Continues on errors and classifies outcomes per source.
8. Writes machine/human summaries under
   `audit-results/bootstrap-go/<UTC_STAMP>/` and copies latest to
   `audit-results/bootstrap-go/latest/`.

## Contract Scope

The contract currently includes the five GO-scoped sources flagged as
`loaded` + `implemented` in the registry:

| Pipeline          | Scope                                           | Core |
|-------------------|-------------------------------------------------|------|
| `camara_goiania`  | Goiania city council (vereadores, expenses)     | yes  |
| `folha_go`        | Goias state payroll and commissioned positions  | yes  |
| `pncp_go`         | GO state/municipal procurement via PNCP         | yes  |
| `querido_diario_go` | GO municipal gazette acts                     | yes  |
| `tcm_go`          | GO municipal finance via SICONFI (246 munis)    | yes  |

`tce_go` and `state_portal_go` are listed under `excluded_from_contract`
because they are `not_built` / `not_implemented` in the registry and have
no corresponding pipeline module in `etl/src/bracc_etl/pipelines/`. Including
them as non-core entries would break the orchestrator's subset parity check
(the contract requires every pipeline_id to exist as an implemented pipeline).
When these sources graduate to `implemented`, add them to the contract.

## Prerequisites

- Docker + Docker Compose available locally.
- `.env` present (start from `.env.example`).
- Adequate machine resources for long-running ingestion.
- The GO contract does not currently require any extra credentials, but
  future sources may add `credential_env` entries.

## Status Model

Per-source terminal status is one of:

- `loaded`
- `blocked_external`
- `blocked_credentials`
- `failed_download`
- `failed_pipeline`
- `skipped`

## Exit Policy

Run exits with non-zero code only when one or more **core** sources fail.
All sources in `bootstrap_go_contract.yml` are currently core, so any GO
pipeline failure will fail the run.

## Report Interpretation

`audit-results/bootstrap-go/<stamp>/summary.json` includes:

- `run_id`, `output_label` (`bootstrap-go`), `contract_mode` (`subset`)
- `started_at_utc`, `ended_at_utc`
- `full_historical`, `db_reset_used`
- per-source statuses, durations, and remediation hints
- aggregate counts and core failure list

`summary.md` is a compact human-readable table. Run `make bootstrap-go-report`
to print the most recent one.

## Relationship to bootstrap-all

`bootstrap-go` is a strict subset of the sources orchestrated by
`bootstrap-all`. Running both leaves independent audit directories:

- `audit-results/bootstrap-all/` — full historical ingestion (52 sources)
- `audit-results/bootstrap-go/`  — GO-only slice (5 sources)

The subset parity check guarantees that any pipeline in the GO contract is
also a valid `bootstrap-all` pipeline, so `bootstrap-go` can be thought of
as a fast smoke-path for Goias data without running the full stack.

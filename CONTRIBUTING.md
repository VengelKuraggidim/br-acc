# Contributing to Fiscal Cidadão

Language: **English** | [Português (Brasil)](docs/pt-BR/CONTRIBUTING.md)

Thanks for helping improve Fiscal Cidadão — a fork of [`brunoclz/br-acc`](https://github.com/brunoclz/br-acc) (AGPL v3) re-scoped to Goiás.

> **Naming note.** User-facing contexts use the name "Fiscal Cidadão". Internal Python packages (`bracc`, `bracc_etl`), the `bracc-etl` CLI entry point, and upstream import paths remain unchanged. When adding code, keep using the `bracc` / `bracc_etl` identifiers; only adjust user-visible strings (UI copy, docs titles, page headers) to the new brand.

## Ground Rules

- Keep changes aligned with public-interest transparency goals.
- Do not add secrets, credentials, or private infrastructure details.
- Respect public-safe defaults and privacy/legal constraints.

## Development Setup

```bash
cd api && uv sync --dev
cd ../etl && uv sync --dev
cd ../frontend && npm install
```

## Security and environment

- **Frontend env:** Only `VITE_*` variables are exposed in the client bundle. Do not put secrets in `VITE_*`; use them only for public config (e.g. `VITE_API_URL`, `VITE_PUBLIC_MODE`).
- **Auth:** Keep tokens in memory or HttpOnly cookies only; do not persist JWT in `localStorage` or `sessionStorage`.
- **Releases:** Before releases, run `npm audit` in `frontend/` and address high/critical findings.

## Quality Checks

Run this before opening a pull request:

```bash
make pre-commit
```

`pre-commit` bundles everything CI enforces on every PR — lint,
type-check, unit tests, neutrality audit, and registry/docs
governance — so you don't get a green-local / red-CI surprise.

Individual targets are also available: `make check` (lint + type +
tests only), `make neutrality`, `make check-public-claims`,
`make check-pipeline-contracts`, `make check-pipeline-inputs`,
`make check-provenance-contract`.

## Provenance contract (ETL pipelines)

Every node and relationship a pipeline persists to Neo4j must carry five
fields (`source_id`, `source_record_id`, `source_url`, `ingested_at`,
`run_id`) so end-users can trace any fact back to its origin.

New or modified pipelines **must** route every dict destined for
`Neo4jBatchLoader` through `self.attach_provenance(...)` in
`bracc_etl.base.Pipeline`. See `docs/provenance.md` for the full
contract and `etl/src/bracc_etl/pipelines/folha_go.py` for the reference
retrofit.

Runtime enforcement lives in `Neo4jBatchLoader` — set
`BRACC_PROVENANCE_MODE=strict` locally to reproduce the production
posture that rejects unstamped rows. CI runs `make
check-provenance-contract` on every PR.

## Pull Request Expectations

- Keep PR scope focused and explain the user impact.
- Include tests for behavior changes.
- Update docs when interfaces or workflows change.
- Ensure all required CI and security checks are green.

## AI-Assisted Contributions

AI-assisted contributions are allowed.  
Human contributors remain responsible for:

- technical correctness,
- security/privacy compliance,
- and final review/sign-off before merge.

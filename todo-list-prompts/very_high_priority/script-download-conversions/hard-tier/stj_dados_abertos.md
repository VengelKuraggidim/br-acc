# Hard — `stj_dados_abertos` (Superior Tribunal de Justiça)

**Registry status**: `partial → not_loaded` — pipeline built but no production load.

**Why hard**:
- STJ dados abertos portal at `https://www.stj.jus.br/dadosabertos/` — CKAN-style but schemas shift between releases.
- Datasets split: decisões, sessões, juristas, partes processuais — which does the pipeline consume?
- PDF-only releases for some jurisprudência; CSV/JSON for statistics-only.

**Investigation first**: read the pipeline module carefully. The "not_loaded" status suggests it may have been scaffolded without a clear upstream target. Don't invent one.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: EVALUATE whether `stj_dados_abertos` can be converted from file_manifest to script_download. This is a HARD-tier pipeline — not_loaded per the registry, scaffolding may be incomplete.

## Phase 1 — Investigate

1. Read etl/src/bracc_etl/pipelines/stj_dados_abertos.py in full. Check if extract() is fully implemented or stub.
2. Check etl/tests/fixtures/stj_dados_abertos/ for fixture schemas.
3. Browse https://www.stj.jus.br/dadosabertos/ for current CKAN resources. Test which datasets match the pipeline's schema.
4. Assess:
   - (A) Pipeline extract() implemented + matching upstream exists → proceed.
   - (B) Pipeline extract() exists but no matching upstream → flag as "upstream gap, pipeline needs redesign".
   - (C) Pipeline is stub → flag as "not production-ready, skip script_download until scope defined".

## Phase 2 (only if A)

Standard fetch_to_disk + scripts/download_stj_dados_abertos.py + smoke + contract snippet.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- **DO NOT invent an upstream** — if the pipeline's expected schema doesn't match any current STJ dataset, the conversion blocks on a design question that a human must answer.
- File scope: scripts/download_stj_dados_abertos.py + etl/src/bracc_etl/pipelines/stj_dados_abertos.py.

## Deliverable

Phase 1 memo (always), Phase 2 snippet if A.
```

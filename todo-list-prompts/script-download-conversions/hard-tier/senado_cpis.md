# Hard — `senado_cpis`

**Registry status**: `partial — Needs richer sessions and requirements`.

**Why hard**: similar to `camara_inquiries` — CPIs live inside `senado` pipeline's generic "comissões" coverage. Individual session transcripts are PDF-only.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: EVALUATE and (if feasible) convert `senado_cpis` from file_manifest to script_download.

The existing `senado` pipeline (already script_download) may already cover the needed upstream via /dadosabertos/senador/ and related endpoints. Check overlap first.

## Phase 1 — Investigate

1. Read etl/src/bracc_etl/pipelines/senado_cpis.py fully.
2. Cross-check with etl/src/bracc_etl/pipelines/senado.py — overlap?
3. Senado API reference:
   - https://legis.senado.leg.br/dadosabertos/comissoes/lista/tipo/CPI
   - https://legis.senado.leg.br/dadosabertos/comissao/{sigla}/membros
   - https://legis.senado.leg.br/dadosabertos/comissao/{sigla}/reunioes
4. Assess:
   - (A) Overlap with senado → extend senado fetch_to_disk.
   - (B) New calls needed → dedicated fetch_to_disk.
   - (C) Session detail behind PDFs → partial scope only.

## Phase 2

Standard conversion if (A) or (B).

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- File scope: scripts/download_senado_cpis.py + etl/src/bracc_etl/pipelines/senado_cpis.py. If (A) also senado.py minimally.

## Deliverable

Phase 1 assessment + Phase 2 snippet if feasible. Explicit recommendation on "richer sessions" gap.
```

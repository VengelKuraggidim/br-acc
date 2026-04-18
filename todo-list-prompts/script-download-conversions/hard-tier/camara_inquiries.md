# Hard — `camara_inquiries` (CPIs + sub-relatórios Câmara dos Deputados)

**Registry status**: `partial — Sessions still low`.

**Why hard**:
- CPIs (Comissões Parlamentares de Inquérito) live inside the `/orgaos/{id}/` endpoints of the Câmara API (already partially covered by the `camara` pipeline's orgaos_*.json).
- Individual session transcripts / depoimentos are behind the Câmara's transcript portal, often PDF-only for oral sessions.
- "Sessions still low" suggests the pipeline's extract() produces fewer records than expected — may reflect an upstream gap, not a transport issue.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: EVALUATE and (if feasible) convert `camara_inquiries` from file_manifest to script_download.

The existing `camara` pipeline (already script_download) writes `orgaos_<UF>.json` via /api/v2/orgaos. If camara_inquiries consumes that same data differently, there may be no NEW download needed — just a pointer.

## Phase 1 — Investigate

1. Read etl/src/bracc_etl/pipelines/camara_inquiries.py fully.
2. Cross-check with etl/src/bracc_etl/pipelines/camara.py — does camara_inquiries read files that camara.fetch_to_disk already produces? (`orgaos_*.json`, `proposicoes_*.json`?)
3. If NOT overlap: identify what NEW files are needed. Sources to consider:
   - /api/v2/orgaos/{id}/membros
   - /api/v2/orgaos/{id}/eventos
   - /api/v2/eventos?itens=100&ordem=DESC&ordenarPor=dataHoraInicio
   - Session transcripts (PDF-only, usually out of scope for structured ingest).
4. Assess:
   - (A) Overlap with camara → modify camara's fetch_to_disk to also write the files camara_inquiries needs; camara_inquiries's own fetch_to_disk becomes a no-op that logs "handled by camara pipeline".
   - (B) Needs new API calls → add targeted fetch_to_disk.
   - (C) Session transcripts required but no API → flag as blocked by upstream PDF-only format.

## Phase 2 (only A or B)

Implement fetch_to_disk accordingly. If (A), keep the camara file edits minimal and well-scoped.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- File scope: scripts/download_camara_inquiries.py + etl/src/bracc_etl/pipelines/camara_inquiries.py. If (A), also etl/src/bracc_etl/pipelines/camara.py — touch minimally.

## Deliverable

Phase 1 assessment + Phase 2 snippet if feasible. Clear recommendation on the "Sessions still low" issue (is it transport or domain?).
```

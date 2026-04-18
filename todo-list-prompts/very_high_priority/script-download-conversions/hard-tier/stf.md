# Hard — `stf` (Supremo Tribunal Federal — jurisprudência / decisões) — ✅ CONCLUÍDO (2026-04-18)

> Script criado em commit `cbd8cde`.

**Why hard**:
- No stable bulk dump. Data lives behind search forms at `https://portal.stf.jus.br/jurisprudencia/`.
- Some Mecanismos: STF's acervo API at `https://ferramentadeconsulta.stf.jus.br/api/` (requires CSRF tokens scraped from landing page).
- Decisões monocráticas vs. colegiadas vs. súmulas have different endpoints and schemas.

**Investigation first**:
Before writing any code, determine:
1. What exactly does `STFPipeline.extract()` consume? Decisões por CPF/CNPJ? Súmulas? Jurisprudência por tema?
2. Is there a public API endpoint (DataJud integration?) the pipeline already targets?
3. Are fixtures under `etl/tests/fixtures/stf/` representative of real upstream format?

**DataJud**: STF publishes to the CNJ's DataJud (elasticsearch). DataJud is marked `blocked_external` in the contract — that may cover what's needed here and make STF a wrapper over DataJud. Check.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: EVALUATE whether `stf` can be converted from file_manifest to script_download.

This is a HARD-tier pipeline. Your FIRST deliverable is a go/no-go decision + scoping document; only if feasible do you proceed with code.

## Phase 1 — Investigate (this may be the whole task)

1. Read etl/src/bracc_etl/pipelines/stf.py in full. Understand: what files does extract() read? What columns? What CPF/CNPJ extraction strategy?
2. Check etl/tests/fixtures/stf/ for real-world upstream format.
3. Research programmatic access:
   - Is there a REST API? (Try `https://portal.stf.jus.br/.../api/`, `https://ferramentadeconsulta.stf.jus.br/api/`)
   - Is it covered by DataJud (CNJ)? Check pipelines/datajud.py for overlap.
   - Landing-page scraping required? If so, what's the CSRF/session pattern?
4. Assess feasibility:
   - (A) Clean public API or bulk dump → proceed to Phase 2.
   - (B) Only DataJud → mark the pipeline as better served by enhancing datajud.py; flag to human.
   - (C) Scraping required with captcha/JS rendering → mark as needing Playwright or similar (NOT in current deps); skip + document why.

**Deliverable from Phase 1** (whether or not you proceed to Phase 2):
- Scope assessment (A/B/C).
- Upstream analysis (URLs tested, failures).
- Recommendation: proceed, defer, or skip.

## Phase 2 (only if Phase 1 → A)

Standard conversion: fetch_to_disk + scripts/download_stf.py + smoke + contract snippet.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory.
- **DO NOT add browser automation deps** (Playwright, Selenium) without explicit approval.
- File scope: scripts/download_stf.py + etl/src/bracc_etl/pipelines/stf.py.
- If Phase 1 concludes (B) or (C), STOP and report — don't force a fragile scraper.

## Deliverable

Phase 1 memo (always). Phase 2 snippet only if feasibility is clear.
```

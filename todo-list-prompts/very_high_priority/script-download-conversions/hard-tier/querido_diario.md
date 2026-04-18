# Hard — `querido_diario` (versão nacional, NÃO a GO) — 🚫 BLOQUEADO (2026-04-18)

> Pipeline federal `querido_diario` foi DEPRECATED (commit `7208381`) — substituído
> por `querido_diario_go` (escopo Goiás-only). Sem trabalho a fazer.

**Sibling**: `querido_diario_go` (already script_download — only Goiânia + Aparecida de Goiânia have data per the prior-session memo).

**Why hard**:
- The national `querido_diario` covers ~5500 municipalities × N years of gazettes. Full backfill is multi-GB.
- API: `https://api.queridodiario.ok.org.br/` (already the corrected host; UI at queridodiario.ok.org.br is a SPA).
- Most municipalities have NO coverage: the api endpoints `/cities?state_code=XX` + `availability_date` filter show actual published cities. Most rows return 0.
- **Extract path for gazettes**: the API returns metadata + a direct URL to the PDF. Pipeline likely fetches those PDFs and runs regex/CPF extraction — that's the heavy part.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: EVALUATE and (if feasible) convert `querido_diario` (national) from file_manifest to script_download.

Related: querido_diario_go already script_download. Same API, but national scope = ~5500 municipalities, very sparse coverage.

## Phase 1 — Investigate

1. Read etl/src/bracc_etl/pipelines/querido_diario.py AND querido_diario_go.py in full.
2. Identify:
   - Which API endpoints querido_diario uses (likely /gazettes with broader filters).
   - Whether it downloads PDFs or just metadata + ocr-extracted text.
   - Expected on-disk files: raw gazette JSONs? extracted .txt per gazette? per-municipality shards?
3. Scope constraints:
   - Total population of usefully-covered cities is probably <100 nationally.
   - Full backfill = thousands of PDFs, each 1-50 MB.
4. Assess:
   - (A) Simple: reuse querido_diario_go's fetch_to_disk pattern with a wider UF scope. Offer --uf filter defaulting to ALL.
   - (B) Complex: PDF OCR extraction required in-script → much harder; might need pdfminer.six (check deps).
   - (C) Behind form/captcha → blocked.

## Phase 2

Fetch_to_disk with (A) simple metadata grab or (B) with PDF handling. Always require --uf or --limit for smoke.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory.
- **ASK before adding pdf libs** (pdfminer.six, pypdf) unless already in deps.
- **ASK before running national full-backfill** (multi-hour, multi-GB).
- File scope: scripts/download_querido_diario.py + etl/src/bracc_etl/pipelines/querido_diario.py.

## Deliverable

Phase 1 memo (A/B/C + reasoning), Phase 2 snippet if feasible.
```

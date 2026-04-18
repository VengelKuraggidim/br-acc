# Script-download conversions — pending work

Convert pipelines from `acquisition_mode: file_manifest` (manual file placement) to
`script_download` (automated CLI wrapper). Pattern already landed for **26 pipelines**
(5 GO + 8 federais first batch + 3 easy + 6 sanctions/regulatory + 3 CGU transparency).

## How to use this folder

Each `<tier>/<pipeline>.md` is a **self-contained Agent prompt**. Copy its contents and
paste into the `Agent` tool (general-purpose subagent) as the `prompt` parameter. The
Agent will:

1. Read the pipeline's `extract()` to learn the on-disk schema it expects.
2. Add `fetch_to_disk(output_dir, ...)` at module level.
3. Create `scripts/download_<name>.py` (thin CLI).
4. Smoke-test end-to-end (download + extract + transform).
5. Return a contract snippet for `config/bootstrap_all_contract.yml`.

**Do NOT let agents edit the contract** — they return snippets; the orchestrator merges
them atomically to avoid races when running multiple agents in parallel.

## Tiers

| Folder | Pipelines | Est. effort | Parallelizable? |
|---|---|---|---|
| `easy-recovery/` | 6 (inep, cepim, ceaf, cpgf, sanctions, transferegov) | ~10 min ea. | 2 agents × 3 pipelines |
| `medium-tier/` | 8 (pncp, siconfi, mides, holdings, bndes, caged, rais, icij) | ~30 min ea. | 3 agents × ~2-3 pipelines |
| `hard-tier/` | 6 (stf, stj_dados_abertos, camara_inquiries, senado_cpis, querido_diario, tce_go) | ~1-2h ea. | 1 agent per pipeline, investigate first |

## Canonical pattern

See `PATTERN.md` for the full Agent prompt template. All `<tier>/<pipeline>.md` files
inherit it — they only list the **deltas** (URL, schema notes, known gotchas).

## Git reset guardrail

The work in `easy-recovery/` was **already executed successfully by agents earlier in a
session**, but a concurrent `git reset --hard HEAD` (run by a parallel instance doing
provenance work) wiped the unstaged files before commit. Re-running these prompts is
safe — URLs and schemas are captured in each file.

If you're running multiple parallel agents across the repo, **serialize `git reset`
operations** or stash unstaged work first.

## Reference implementations

Study these before editing new pipelines:

- `etl/src/bracc_etl/pipelines/tcu.py` (fetch_to_disk at L206) + `scripts/download_tcu.py` — minimal, clean, APEX HTML scrape
- `etl/src/bracc_etl/pipelines/tesouro_emendas.py` + `scripts/download_tesouro_emendas.py` — Portal da Transparência CSV pattern
- `etl/src/bracc_etl/pipelines/siop.py` fetch_to_disk — Portal ZIP-consolidated-CSV-split-by-year pattern (watch for `/emendas-parlamentares/<year>` redirects to a single file)
- `etl/src/bracc_etl/pipelines/camara.py` `_download_ceap_csv` — ZIP-inside-HTTP workaround for upstream CSV endpoints with null-byte padding bugs

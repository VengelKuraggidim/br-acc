# Medium — `mides` (Ministério do Desenvolvimento Social / programas sociais)

**Complexity**: CKAN dataset registry is stable but schema varies across programs (Bolsa Família, BPC, SUAS, etc.). Pipeline probably consumes monthly CSV dumps.

**URL hints**:
- CKAN catalog: `https://aplicacoes.mds.gov.br/sagi/vis/` or `https://dados.gov.br/dados/conjuntos-dados?organization=ministerio-cidadania`
- Bolsa Família beneficiários: `https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/<YYYYMM>`
- PDET-Transparência may also host it.

**Gotchas**:
- Check pipeline docstring for exact URL/endpoint — don't guess.
- CPFs almost certainly masked (MDS-level PII policy).

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `mides` (MDS social programs) from file_manifest to script_download.

## Task

1. **INVESTIGATE FIRST** — read etl/src/bracc_etl/pipelines/mides.py carefully. The docstring + extract() should tell you:
   - Which specific MDS program(s) this pipeline ingests (Bolsa Família? BPC? SUAS? all?)
   - Expected file layout (monthly? annual? per-município?)
   - Column schema
   Do NOT guess the URL — derive it from the pipeline's own assumptions. Look for URL hints in the docstring or existing test fixtures under etl/tests/fixtures/mides/.

2. Once the scope is clear, add fetch_to_disk(output_dir, ...) matching that layout.
3. Create scripts/download_mides.py with appropriate flags (--year, --month, --program, --output-dir, --limit).
4. Smoke-test against a small window; confirm extract+transform produces expected counts.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- File scope: scripts/download_mides.py + etl/src/bracc_etl/pipelines/mides.py.
- If extract() expects multiple MDS datasets, download ALL of them in one script.
- If the pipeline turns out to aggregate sources that are already covered by other pipelines (e.g. Portal da Transparência bolsa-familia), flag it and suggest deduplication.

## Deliverable

- Exact URL(s) used.
- Output file list + sizes.
- Extract+transform counts.
- Caveats (masked CPFs, schema drift, etc.).
- Contract snippet:
```json
{
  "pipeline_id": "mides",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/mides/*"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_mides.py --output-dir ../data/mides"]
}
```
- Files created/modified.
```

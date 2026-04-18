# Recovery prompt — `cpgf`

**Status**: previously converted end-to-end, wiped. Re-run to restore.

**Prior findings**:
- URL: `https://portaldatransparencia.gov.br/download-de-dados/cpgf/<YYYYMM>` → 302 → `.../YYYYMM_CPGF.zip`
- Output: `data/cpgf/<YYYYMM>_CPGF.csv` (e.g. `202501_CPGF.csv` ~2.83 MB)
- Extract: 8370 expenses, 0 cardholders, 0 GASTOU_CARTAO rels (CPFs masked `***.NNN.NNN-**` — expected, only `GovCardExpense` nodes produced).
- Widget mode is **MES** (monthly).

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `cpgf` (Cartão de Pagamento do Governo Federal, CGU) from file_manifest to script_download. Previously converted but wiped by concurrent git reset.

## Task

1. Read `etl/src/bracc_etl/pipelines/cpgf.py` — CpgfPipeline.extract() globs `data/cpgf/*CPGF.csv` (verify: columns, separator=`;`, encoding=latin-1).
2. Add `fetch_to_disk(output_dir, *, month=None, start=None, end=None, limit=None)`:
   - If `month` is set (YYYYMM): fetch that single month.
   - If `start`/`end` (YYYYMM): iterate inclusive range.
   - Default (nothing set): walk back from current UTC month until the Portal returns a ZIP (lag ~1-2 months; try ~6 months back max).
   - URL: `https://portaldatransparencia.gov.br/download-de-dados/cpgf/<YYYYMM>` → 302 → CGU `dadosabertos-download.cgu.gov.br` → ZIP.
   - Extract in-memory, write `data/cpgf/<YYYYMM>_CPGF.csv`.
3. Create `scripts/download_cpgf.py` with argparse: `--output-dir`, `--month` (repeatable), `--start`, `--end`, `--limit`.
4. Smoke-test with `--month 202501`. Expected: ~8370 expenses, 0 cardholders (CPFs masked).

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add new deps.
- File scope: scripts/download_cpgf.py + etl/src/bracc_etl/pipelines/cpgf.py.

## Deliverable

URL + month(s), output size, extract counts (note masked CPFs as inherent), contract snippet:
```json
{
  "pipeline_id": "cpgf",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/cpgf/*"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_cpgf.py --output-dir ../data/cpgf"]
}
```
Plus caveats + files modified/created.
```

# Medium — `caged` (Novo CAGED, emprego formal)

**Status per registry**: `stale` — "Aggregate-only implementation". Upstream data-quality issue, not pure transport.

**URL hints**:
- PDET/MTE FTP: `ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/<YYYY>/<YYYYMM>/CAGEDMOV<YYYYMM>.7z` (7z format — needs `py7zr` or `libarchive`)
- HTTP mirror: `https://pdet.mte.gov.br/microdados-novo-caged` → login wall. Not usable programmatically.

**Gotchas**:
- **7z archives**: Python stdlib does NOT include 7z. `py7zr` is the pure-Python lib; not in current deps. DO NOT add it unless no alternative — instead check if upstream also ships `.csv.gz` or similar.
- Aggregate-only means the pipeline only computes totals, not per-CPF. That limits downstream graph enrichment — consider whether conversion is worth the scope complexity.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `caged` (Novo CAGED employment stats) from file_manifest to script_download.

Pipeline is registered as "stale, aggregate-only" — the conversion may be simpler than it looks if the extract() just reads a single monthly/annual CSV rather than the microdado dump.

## Task

1. **INVESTIGATE FIRST**:
   - Read etl/src/bracc_etl/pipelines/caged.py carefully. What exactly does extract() read? Aggregate CSV? 7z archive? Parquet?
   - Check etl/tests/fixtures/caged/ for schema hints.
   - Only if extract() expects the raw 7z microdado, the conversion requires py7zr dep (DO NOT add without confirming no alternative).
2. Find the matching upstream:
   - PDET: https://pdet.mte.gov.br/microdados-novo-caged (may require form submission)
   - FTP: ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/
3. Write fetch_to_disk matching the pipeline's schema. If upstream serves 7z and pipeline expects CSV, decide: use py7zr in fetch_to_disk OR punt and mark skipped.
4. Smoke-test 1 month.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory.
- **ASK before adding py7zr** — it's a non-trivial dep.
- File scope: scripts/download_caged.py + etl/src/bracc_etl/pipelines/caged.py.
- If you conclude the conversion is blocked (7z + no dep, or auth wall, or upstream deprecated), DO NOT force it. Report back with a clear blocker.

## Deliverable

URL + format, output sizes, extract+transform counts, contract snippet (OR a skip-reason paragraph if blocked):
```json
{
  "pipeline_id": "caged",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/caged/*"],
  "blocking_reason_if_any": "Aggregate-only implementation; upstream monthly dumps use 7z archives — conversion may require py7zr dep.",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_caged.py --output-dir ../data/caged"]
}
```
Plus files modified.
```

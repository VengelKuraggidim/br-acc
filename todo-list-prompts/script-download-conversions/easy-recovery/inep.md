# Recovery prompt — `inep`

**Status**: previously converted end-to-end, wiped by concurrent `git reset --hard`. Re-run to restore.

**Prior agent's findings (2026-04-17)**:

- URL: `https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2022.zip` (26 MB ZIP → `microdados_ed_basica_2022.csv` ~190 MB)
- Output: `data/inep/microdados_ed_basica_2022.csv`
- Extract counts: 5000 schools, 312 school→company links (at `--limit 5000`)
- Contract: `required_inputs: ["data/inep/microdados_ed_basica_2022.csv"]`

**Known gotchas**:

1. `download.inep.gov.br` TLS cert fails default-bundle verification. CLI should default `--insecure` (content validated by ZIP structure); allow `--no-insecure` override.
2. 2022 microdata omits `QT_FUNCIONARIOS`; pipeline's `_parse_int("")` falls back to 0. Pre-existing issue, not caused by the conversion.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc. Stack: Python 3.14, `uv`, pipelines under `etl/src/bracc_etl/pipelines/*.py`.

Goal: convert `inep` from acquisition_mode=file_manifest to script_download. This pipeline was already converted end-to-end in a prior session but the working tree was reset by a concurrent process before commit. Known-good URL and schema are documented below.

## Task

1. Read `etl/src/bracc_etl/pipelines/inep.py` — InepPipeline.extract() expects `data/inep/microdados_ed_basica_*.csv` (latin-1 or utf-8, check the pipeline).
2. Add `fetch_to_disk(output_dir, *, year=2022, limit=None, insecure=True)` at module level:
   - URL: `https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_<year>.zip`
   - TLS: default `httpx.Client(verify=not insecure)` because the INEP cert chain fails default-bundle verification; content is validated by ZIP structure.
   - Extract the main `microdados_ed_basica_<year>.csv` from the archive (in-memory with `zipfile`), write to `output_dir`.
   - Respect `--limit` by truncating after parse (the file is 190 MB; smoke should default to ~5000 rows).
3. Create `scripts/download_inep.py` with argparse: `--output-dir` (default `data/inep`), `--year`, `--limit`, `--no-insecure`.
4. Smoke-test: `uv run --project etl python scripts/download_inep.py --output-dir /tmp/smoke_inep --limit 5000`. Then exercise the pipeline:
   ```python
   from unittest.mock import MagicMock
   from bracc_etl.pipelines.inep import InepPipeline
   # Arrange data at /tmp/smoke_root/inep/ to match the pipeline's data_dir glob
   p = InepPipeline(driver=MagicMock(), data_dir="/tmp/smoke_root")
   p.extract(); p.transform()
   print(f"schools={len(p.schools)}, links={len(p.school_company_rels)}")
   ```
   Expected counts at limit=5000: ~5000 schools, ~312 links (pre-existing QT_FUNCIONARIOS=0 behavior).

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml — return the snippet in your report.
- DO NOT commit, update memory, or add new dependencies.
- File scope: scripts/download_inep.py + etl/src/bracc_etl/pipelines/inep.py. Nothing else.

## Deliverable (under 300 words)

URL used, output file + size, extract counts, and the exact contract snippet:
```json
{
  "pipeline_id": "inep",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/inep/microdados_ed_basica_2022.csv"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_inep.py --output-dir ../data/inep"]
}
```
Plus caveats and files created/modified.
```

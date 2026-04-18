# Recovery prompt — `cepim` — ✅ CONCLUÍDO (2026-04-18)

> Script criado em commit `e809857`.

**Status**: previously converted, wiped by concurrent `git reset --hard`. Re-run to restore.

**Prior agent's findings**:
- URL: `https://portaldatransparencia.gov.br/download-de-dados/cepim/<YYYYMMDD>` (302 → CGU dadosabertos S3)
- Output: `data/cepim/cepim.csv` (634 KB, 3576 rows)
- Extract: 3576 BarredNGOs, 3576 company→NGO rels
- Widget mode: **DIA** — only the current-published-day snapshot is live (historical dates 403). Scrape landing `arquivos.push({"ano","mes","dia"})` to find the right date.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc. Stack: Python 3.14, `uv`.

Goal: convert `cepim` (Cadastro de Empresas Impedidas de Contratar, CGU) from acquisition_mode=file_manifest to script_download. Previously landed but wiped by concurrent git reset.

## Task

1. Read `etl/src/bracc_etl/pipelines/cepim.py` — CepimPipeline.extract() expects `data/cepim/cepim.csv`. Note separator, encoding, column names.
2. Add `fetch_to_disk(output_dir, *, date=None)` at module level:
   - Landing: `https://portaldatransparencia.gov.br/download-de-dados/cepim` — HTML contains `arquivos.push({"ano":"YYYY","mes":"MM","dia":"DD"})`. Regex it out. (Widget mode is DIA — only today's snapshot works.)
   - Download: `https://portaldatransparencia.gov.br/download-de-dados/cepim/<YYYYMMDD>` → 302 → ZIP on `dadosabertos-download.cgu.gov.br`.
   - Extract in-memory with `zipfile`, write to `data/cepim/cepim.csv`.
3. Create `scripts/download_cepim.py` with argparse: `--output-dir` (default `data/cepim`), `--date YYYYMMDD` (optional; only same-day usually works — log a warning otherwise).
4. Smoke-test: `uv run --project etl python scripts/download_cepim.py --output-dir /tmp/smoke_cepim`. Then:
   ```python
   from unittest.mock import MagicMock
   from bracc_etl.pipelines.cepim import CepimPipeline
   p = CepimPipeline(driver=MagicMock(), data_dir="<root-with-smoke-data>")
   p.extract(); p.transform()
   print(len(p.ngos), len(p.company_ngo_rels))
   ```
   Expected: ~3576 of each.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml — return the snippet.
- DO NOT commit, update memory, or add new dependencies.
- File scope: scripts/download_cepim.py + etl/src/bracc_etl/pipelines/cepim.py.

## Deliverable (under 250 words)

URL + date token used, output file + size, extract counts, contract snippet:
```json
{
  "pipeline_id": "cepim",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/cepim/cepim.csv"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_cepim.py --output-dir ../data/cepim"]
}
```
Plus caveats and files created/modified.
```

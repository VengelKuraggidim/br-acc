# Medium — `bndes` (BNDES operações de crédito) — ✅ CONCLUÍDO (2026-04-18)

> Script criado em commit `44f7ea5`.

**URL hints**:
- Transparência BNDES: `https://www.bndes.gov.br/wps/portal/site/home/transparencia`
- Dados abertos catalog: `https://dadosabertos.bndes.gov.br/dataset/` (CKAN-style)
- Most stable feed: bulk CSV of operações indiretas + diretas at `https://www.bndes.gov.br/SiteBndes/export/sites/default/bndes_pt/Galerias/Convivencia/Transparencia/Operacoes_Indiretas/operacoes_indiretas_nao_automaticas.xlsx` (XLSX — likely needs pandas.read_excel with openpyxl engine)

**Gotchas**:
- Some datasets are XLSX not CSV. `openpyxl` is a dep of pandas; confirm it's in the project.
- "Operações indiretas automáticas" is a separate file — check what the pipeline consumes.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `bndes` (BNDES credit operations) from file_manifest to script_download.

## Task

1. Read etl/src/bracc_etl/pipelines/bndes.py — identify exact files extract() consumes (filenames, columns, .csv vs .xlsx). Check fixtures under etl/tests/fixtures/bndes/ for hints.
2. Research the current upstream source:
   - https://dadosabertos.bndes.gov.br/ (CKAN)
   - https://www.bndes.gov.br/wps/portal/site/home/transparencia
   Pick the endpoint matching the pipeline's expected schema.
3. Add fetch_to_disk(output_dir, *, limit=None). Handle XLSX if needed (pandas.read_excel(..., engine="openpyxl") then write CSV matching pipeline schema, OR save XLSX if pipeline reads XLSX directly).
4. Create scripts/download_bndes.py.
5. Smoke + pipeline extract+transform validation.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps unless openpyxl is missing (confirm first with `uv run --project etl python -c "import openpyxl"`).
- File scope: scripts/download_bndes.py + etl/src/bracc_etl/pipelines/bndes.py.
- If upstream has changed schema vs what pipeline expects, prefer remapping columns in fetch_to_disk over changing extract().

## Deliverable

URL + format (CSV/XLSX), output sizes, extract+transform counts, contract snippet:
```json
{
  "pipeline_id": "bndes",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/bndes/*"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_bndes.py --output-dir ../data/bndes"]
}
```
Plus caveats and files modified.
```

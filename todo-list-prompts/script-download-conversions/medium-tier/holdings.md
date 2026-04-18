# Medium — `holdings` (RFB Quadro Societário — potential scale trap)

**Complexity**: **high — multi-GB bulk dumps, binary-encoded column headers in some versions.**

**URL hints**:
- RFB open-data bulk: `https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj.zip` or per-month `https://arquivos.receitafederal.gov.br/dados/cnpj/<YYYY-MM>/` (each month ships ~30 files, Socios*.zip is the quadro societário specifically)
- SOCIO files are CSV `;`-delimited, latin-1, one row per partner per CNPJ. Full dump = ~50M rows.

**Gotchas**:
- Existing `cnpj` pipeline likely downloads the full RFB base. Check if `holdings` is a thin projection over that (only the SOCIO_*.csv files) or a separate pipeline. **If it's a projection — avoid duplicating downloads**, just symlink or reuse cnpj's raw dir.
- Without `--limit` or month filter, full download = ~6 GB zipped, ~40 GB extracted.
- Schema changed across years; newer RFB format has different column order.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `holdings` (RFB quadro societário / partners) from file_manifest to script_download.

CRITICAL: the existing `cnpj` pipeline almost certainly already downloads the RFB base. Check etl/src/bracc_etl/pipelines/cnpj.py first. If `holdings` is a downstream projection, DO NOT re-download — import/reuse the cnpj download logic.

## Task

1. **INVESTIGATE FIRST**:
   - Read etl/src/bracc_etl/pipelines/holdings.py + etl/src/bracc_etl/pipelines/cnpj.py.
   - Does `holdings` expect its own raw files, or does it read from `data/cnpj/raw/` etc.?
   - What columns/separator/encoding?
2. Decide the right strategy:
   - **Option A (preferred if cnpj already handles download)**: holdings pipeline is a thin view; add a minimal fetch_to_disk that either reuses cnpj's download or is a no-op with a clear log message pointing to cnpj.
   - **Option B (independent source)**: write a full fetch_to_disk that iterates RFB monthly dumps and extracts SOCIO_*.zip files.
3. Create scripts/download_holdings.py with safety: default to last 1 month, require --full for full-historical, always offer --limit for smoke.
4. Smoke-test with ~10MB slice; confirm extract+transform counts.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- File scope: scripts/download_holdings.py + etl/src/bracc_etl/pipelines/holdings.py.
- **Disk safety**: if smoke would write >200 MB, stop and ask for explicit confirmation.

## Deliverable

- Strategy chosen (A or B) and why.
- URL(s) used.
- Output file list + sizes.
- Extract+transform counts.
- Contract snippet:
```json
{
  "pipeline_id": "holdings",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/holdings/*"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_holdings.py --output-dir ../data/holdings"]
}
```
- Files modified/created.
```

# Medium — `pncp` (Portal Nacional de Contratações Públicas, nacional)

**Complexity**: pagination + timeouts. Related pipeline `pncp_go` (already script_download) is a scoped subset; study it first.

**URL hints**:
- `https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?dataInicial=YYYYMMDD&dataFinal=YYYYMMDD&codigoModalidadeContratacao=<1-13>&pagina=<N>&tamanhoPagina=500`
- Timeouts on some modalidades are common; need 4-retry-exponential-backoff pattern (see `scripts/download_comprasnet.py` for the reference).
- National scope = no `uf` param in query; pipeline expects per-year raw JSONs (or yearly+modalidade shards).

**Gotchas**:
- Some modalidades return 204 No Content for certain windows — treat as success.
- Portal lags ~5 days for "dataInicial" near current date.
- Multi-year backfill can yield tens of thousands of pages; respect `--start-year`/`--end-year` CLI flags.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `pncp` (national procurement, all UFs, all modalidades) from file_manifest to script_download.

Reference implementations to STUDY FIRST:
- etl/src/bracc_etl/pipelines/pncp_go.py — GO-scoped sibling (already script_download)
- scripts/download_comprasnet.py — PNCP API retry/timeout pattern

## Task

1. Read etl/src/bracc_etl/pipelines/pncp.py — confirm extract() glob + expected JSON shape. May reuse pncp_go helpers via import.
2. Add fetch_to_disk(output_dir, *, start_year, end_year, modalidades=None, timeout=60, max_retries=4) at module level.
   - URL: https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao
   - Params: dataInicial/dataFinal (YYYYMMDD, 1-year windows), codigoModalidadeContratacao, pagina, tamanhoPagina=500
   - Retry on timeouts (exponential backoff 3s/6s/12s). Skip rest of modalidade on 2+ consecutive timeouts at same page.
   - HTTP 204 → empty, stop this modalidade/window. HTTP 500 → log + skip page.
   - Output: one file per (year, modalidade), e.g. data/pncp/pncp_<modalidade>_<YYYY>.json
3. Create scripts/download_pncp.py: --output-dir, --start-year, --end-year (default: last 2 years), --modalidade (repeatable; default all 1-13), --timeout, --limit (smoke).
4. Smoke: 1 year × 1 modalidade × --limit 100.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml — return snippet.
- DO NOT commit, update memory, add deps.
- File scope: scripts/download_pncp.py + etl/src/bracc_etl/pipelines/pncp.py.
- Do NOT modify pncp_go (already done).

## Deliverable

URL pattern, sample output sizes, extract counts on smoke, contract snippet:
```json
{
  "pipeline_id": "pncp",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/pncp/*"],
  "blocking_reason_if_any": "Freshness SLA pending; upstream has per-modalidade timeout sensitivities that may need longer windows for full-historical runs.",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_pncp.py --output-dir ../data/pncp"]
}
```
Plus caveats + files modified.
```

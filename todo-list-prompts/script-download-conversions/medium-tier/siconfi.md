# Medium — `siconfi` (Sistema de Informações Contábeis e Fiscais, Tesouro Nacional)

**Complexity**: API OData but with distinct resource IDs per dataset (DCA, RREO, RGF). Pipeline currently marks "No CNPJ direct links" in the registry — the gap is fields, not transport.

**URL hints**:
- API base: `https://apidatalake.tesouro.gov.br/ords/siconfi/`
- DCA (Declarações Contas Anuais): `/tt/dca?exercicio=<YYYY>&anexo=<NN>&an_exercicio=<YYYY>` — paginated JSON
- RREO (Relatório Resumido Execução Orçamentária): `/tt/rreo?an_exercicio=<YYYY>&nr_periodo=<1-6>` — bimonthly
- RGF (Relatório Gestão Fiscal): `/tt/rgf?an_exercicio=<YYYY>&nr_periodo=<1-3>` — quadrimestral
- Listas auxiliares: `/tt/entes` (municipal+state registry)

**Gotchas**:
- OData `$top`/`$skip` pagination — page size cap ~1000.
- Must iterate (exercicio, anexo, ente_federado) tuples — full national historical = ~5574 municipalities × many anexos × years.
- Pipeline expects CSV per anexo typically — check the pipeline file for exact file layout.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `siconfi` (Tesouro Nacional DCA/RREO/RGF) from file_manifest to script_download.

Reference: tcm_go pipeline is also SICONFI-family (municipal slice for GO) — check etl/src/bracc_etl/pipelines/tcm_go.py for URL patterns and header handling.

## Task

1. Read etl/src/bracc_etl/pipelines/siconfi.py — document exactly what extract() reads (DCA? RREO? RGF? per-ente? per-anexo?). Match that layout in fetch_to_disk.
2. Add fetch_to_disk(output_dir, *, exercicios, report_type, entes=None, limit=None):
   - URL: https://apidatalake.tesouro.gov.br/ords/siconfi/tt/{report_type} with the params (exercicio, anexo, nr_periodo, an_exercicio, ...) based on report_type.
   - Paginate via $top=1000&$skip=N until items returned < $top.
   - Write one JSON or CSV per (exercicio, ente/anexo) tuple matching pipeline expectations.
3. Create scripts/download_siconfi.py with argparse: --output-dir, --exercicio (repeatable), --report dca|rreo|rgf (repeatable), --ente (optional filter by IBGE code), --limit.
4. Smoke: --report dca --exercicio 2024 --limit 100.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- File scope: scripts/download_siconfi.py + etl/src/bracc_etl/pipelines/siconfi.py.
- Avoid full-national runs during smoke (5574 entes × 8 anexos × 5 years is millions of rows).

## Deliverable

URL(s) used, sample outputs, extract counts, contract snippet:
```json
{
  "pipeline_id": "siconfi",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/siconfi/*"],
  "blocking_reason_if_any": "No CNPJ direct links — SICONFI entities are IBGE codes, not companies. Person/company linkage requires downstream joins.",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_siconfi.py --output-dir ../data/siconfi"]
}
```
Plus caveats + files modified.
```

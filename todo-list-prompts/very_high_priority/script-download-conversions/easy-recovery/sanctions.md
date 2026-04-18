# Recovery prompt — `sanctions` (CEIS + CNEP)

**Status**: previously converted, wiped. Re-run to restore.

**Prior findings** — `sanctions` is NOT a meta-pipeline; it is CGU's **CEIS (Cadastro de Empresas Inidôneas e Suspensas) + CNEP (Cadastro Nacional de Empresas Punidas)** combined, each with its own concrete CSV schema.

- URLs:
  - `https://portaldatransparencia.gov.br/download-de-dados/ceis/<YYYYMMDD>`
  - `https://portaldatransparencia.gov.br/download-de-dados/cnep/<YYYYMMDD>`
- Output: `data/sanctions/ceis.csv` (21.67 MB, 22480 rows) + `data/sanctions/cnep.csv` (0.78 MB, 1620 rows)
- Extract: 24100 sanctions, 24100 sanctioned entities (sample: `CEIS_27673235153_0` → CPF `276.732.351-53`)
- Widget mode DIA; **historical dates 403**. Scrape landing for each of `/ceis` and `/cnep` separately.
- **Contract `core: true`** (different from most — reflects its centrality for sanction graph enrichment).
- Column remap from CGU caps headers to `cpf_cnpj,nome,data_inicio,data_fim,motivo`.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `sanctions` (CEIS + CNEP consolidated, CGU) from file_manifest to script_download. This pipeline aggregates TWO CGU datasets (CEIS: suspended/inidoneous firms; CNEP: punished firms) into a single `SanctionsPipeline`. Previously converted but wiped by concurrent git reset.

## Task

1. Read `etl/src/bracc_etl/pipelines/sanctions.py` — confirm extract() reads `data/sanctions/ceis.csv` AND `data/sanctions/cnep.csv`. Note columns (unified to `cpf_cnpj, nome, data_inicio, data_fim, motivo`).
2. Add `fetch_to_disk(output_dir, *, date=None)`:
   - For BOTH `ceis` and `cnep`, separately:
     - Landing scrape: `https://portaldatransparencia.gov.br/download-de-dados/<ceis|cnep>` → regex `arquivos.push({"ano","mes","dia"})` to discover latest date.
     - Download: `/download-de-dados/<ceis|cnep>/<YYYYMMDD>` → 302 → CGU ZIP.
     - Extract in-memory, remap Portuguese caps headers (`CPF/CNPJ DO SANCIONADO`, `NOME DO SANCIONADO`, `DATA DE INÍCIO DA SANÇÃO`, `DATA DE FIM DA SANÇÃO`, `MOTIVO...` etc.) to the pipeline's snake_case schema.
     - Write `data/sanctions/<ceis|cnep>.csv` (latin-1 or utf-8 per pipeline).
3. Create `scripts/download_sanctions.py` with argparse: `--output-dir`, `--date YYYYMMDD` (optional — default auto-discover).
4. Smoke-test CLI then pipeline: expect ~24100 sanctions, ~24100 sanctioned entities.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml — return the snippet (note `core: true`).
- DO NOT commit, update memory, add new deps.
- File scope: scripts/download_sanctions.py + etl/src/bracc_etl/pipelines/sanctions.py.

## Deliverable

URLs + dates, output sizes (two files), extract/transform counts, contract snippet (note `"core": true`):
```json
{
  "pipeline_id": "sanctions",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/sanctions/*"],
  "blocking_reason_if_any": "-",
  "core": true,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_sanctions.py --output-dir ../data/sanctions"]
}
```
Plus caveats (widget DIA, historical 403) + files modified/created.
```

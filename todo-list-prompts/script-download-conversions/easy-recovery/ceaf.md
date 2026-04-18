# Recovery prompt — `ceaf`

**Status**: previously converted, wiped. Re-run to restore.

**Prior findings**:
- URL: `https://portaldatransparencia.gov.br/download-de-dados/ceaf/<YYYYMMDD>`
- Output: `data/ceaf/ceaf.csv` (3.0 MB, 4079 expulsions)
- Extract: 4079 expulsions, **0 person_rels** (CPFs masked upstream `***.NNN.NNN-**` — intrinsic).
- Widget mode DIA; scrape landing.
- **Column remap required**: Portal ships Portuguese caps headers (`NOME DO SANCIONADO`, `CATEGORIA DA SANÇÃO`, ...) but pipeline reads snake_case with `,` delimiter. `fetch_to_disk` must rename columns and rewrite `,`-delimited latin-1 CSV.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `ceaf` (Cadastro de Expulsos da Administração Federal, CGU) from file_manifest to script_download. Previously converted but wiped by concurrent git reset.

## Task

1. Read `etl/src/bracc_etl/pipelines/ceaf.py` — note expected columns/delim/encoding for `data/ceaf/ceaf.csv`.
2. Add `fetch_to_disk(output_dir, *, date=None)`:
   - Landing scrape: `https://portaldatransparencia.gov.br/download-de-dados/ceaf`, regex `arquivos.push({"ano","mes","dia"})`.
   - Download: `/download-de-dados/ceaf/<YYYYMMDD>` → 302 → CGU ZIP.
   - Extract in-memory. Portal ships `;`-delim latin-1 with Portuguese caps headers; pipeline reads `,`-delim latin-1 with snake_case. **Remap columns and rewrite**:
     - `NOME DO SANCIONADO` → `nome`
     - `CPF DO SANCIONADO (MASCARA)` → `cpf`
     - `CARGO DO SANCIONADO` → `cargo`
     - `ÓRGÃO DO SANCIONADO` → `orgao`
     - `CATEGORIA DA SANÇÃO` → `categoria`
     - `DATA DE PUBLICAÇÃO` → `data_publicacao`
     - `TIPO DE SANÇÃO` → `tipo_sancao`
     - `FUNDAMENTAÇÃO LEGAL` → `fundamentacao_legal`
     - (verify against actual header on download; raise clear error if a column is missing)
3. Create `scripts/download_ceaf.py` with argparse: `--output-dir`, `--date`.
4. Smoke-test: CLI + pipeline extract+transform. Expected 4079 expulsions, 0 person_rels (by design).

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml — return the snippet.
- DO NOT commit, update memory, add new dependencies.
- File scope: scripts/download_ceaf.py + etl/src/bracc_etl/pipelines/ceaf.py.

## Deliverable

URL + date used, output file + size, extract counts (mention masked CPFs), contract snippet:
```json
{
  "pipeline_id": "ceaf",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/ceaf/ceaf.csv"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_ceaf.py --output-dir ../data/ceaf"]
}
```
Plus caveats and files created/modified.
```

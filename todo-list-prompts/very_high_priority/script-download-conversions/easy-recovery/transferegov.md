# Recovery prompt — `transferegov`

**Status**: previously converted, wiped. Re-run to restore.

**Critical finding from prior agent**: `transferegov` pipeline does NOT consume `/download-de-dados/transferencias/` (different schema). The actual upstream is the **same consolidated `/emendas-parlamentares` endpoint** also used by `siop`/`tesouro_emendas` (but parsed differently — transferegov consumes the `_Convenios.csv` and `_PorFavorecido.csv` auxiliary slices that the others skip).

**Prior findings**:
- URL: `https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/<YYYYMMDD>` → 302 → `.../EmendasParlamentares.zip`. The date token is required syntactically but the server ignores it (always returns latest consolidated).
- Output: `data/transferegov/EmendasParlamentares.csv` (42.72 MB) + `EmendasParlamentares_Convenios.csv` (23.51 MB) + `EmendasParlamentares_PorFavorecido.csv` (167.25 MB)
- Extract: 71894 amendments, 1566 authors, 52201 favorecido companies, 0 favorecido persons (PF CPFs masked), 532798 favorecido rels, 68467 convenios

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `transferegov` from file_manifest to script_download. Previously converted but wiped by concurrent git reset.

IMPORTANT: despite the pipeline name, upstream data comes from the `/emendas-parlamentares` endpoint on Portal da Transparência — NOT `/transferencias/`. The Portal-emendas ZIP contains three CSVs and transferegov consumes the Convenios + PorFavorecido auxiliaries that siop/tesouro_emendas skip.

## Task

1. Read `etl/src/bracc_etl/pipelines/transferegov.py` — confirm extract() globs for `EmendasParlamentares*.csv` under `data/transferegov/`. Note columns, separator, encoding.
2. Add `fetch_to_disk(output_dir, *, date=None)`:
   - URL: `https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/<YYYYMMDD>` (date can be arbitrary; server ignores it; use today's).
   - 302 → `dadosabertos-download.cgu.gov.br/.../EmendasParlamentares.zip`.
   - Extract ALL THREE CSVs from the ZIP (main + _Convenios + _PorFavorecido) to `output_dir`. The _PorFavorecido.csv is 167 MB — don't truncate unless `--limit` passed.
3. Create `scripts/download_transferegov.py` with argparse: `--output-dir`, `--limit` (truncates PorFavorecido for smoke).
4. Smoke-test full download (~235 MB). Expected: 71894 amendments, 1566 authors, 52201 companies, 0 PF persons, 532798 favorecido rels, 68467 convenios.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml — return the snippet.
- DO NOT commit, update memory, add new deps.
- File scope: scripts/download_transferegov.py + etl/src/bracc_etl/pipelines/transferegov.py.
- NOTE: if `siop` or `tesouro_emendas` already cache `EmendasParlamentares.zip` under their own raw dirs, consider whether to share the download. Current recommendation: keep outputs separate (transferegov is the only consumer of _Convenios/_PorFavorecido).

## Deliverable

URL used, output sizes (three files), extract/transform counts, contract snippet:
```json
{
  "pipeline_id": "transferegov",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/transferegov/*"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_transferegov.py --output-dir ../data/transferegov"]
}
```
Plus caveats + files modified/created.
```

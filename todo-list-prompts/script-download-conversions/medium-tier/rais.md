# Medium — `rais` (Relação Anual de Informações Sociais)

**Warning**: **multi-GB annual dumps** (RAIS Estabelecimentos ~2-4 GB each; RAIS Vínculos ~50+ GB). Transport is simple but scale is huge.

**URL hints**:
- PDET: `https://pdet.mte.gov.br/rais` (likely requires form submission for microdados)
- Historical FTP mirror: check `ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/`
- Some years republished as Parquet on open-data catalogs; that would be far cheaper — search CKAN.

**Gotchas**:
- CAGED + RAIS share PDET infrastructure; same archive quirks (7z in some years, .zip in others, .txt fixed-width in very old years).
- Unlikely to have per-CPF details exposed publicly — most RAIS public releases are aggregated by estabelecimento/município.
- Full-history ingest will easily consume 100+ GB disk. `--year` filter is essential.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `rais` (annual employment relations) from file_manifest to script_download.

**SCALE WARNING**: raw RAIS is multi-GB per year. Always require --year explicitly; don't default to full-history.

## Task

1. **INVESTIGATE FIRST**:
   - Read etl/src/bracc_etl/pipelines/rais.py: what exactly does it read? RAIS Estabelecimentos? Vínculos? aggregated stats?
   - Fixture files in etl/tests/fixtures/rais/ (if present) show expected schema.
2. Find the correct upstream:
   - PDET portal (likely non-programmatic)
   - CKAN parquet mirrors for some years
   - Archived FTP for older vintages
3. Write fetch_to_disk(output_dir, *, year, kind="estabelecimentos", limit=None). Require `year` explicitly.
4. Create scripts/download_rais.py: --year (required, repeatable), --kind, --output-dir, --limit.
5. Smoke a tiny slice (e.g. 1 município × 1 year if possible; if not, fail gracefully and document).

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory.
- **ASK before adding new deps** (py7zr, pyarrow, etc.).
- **ASK before downloading >500 MB** during smoke.
- File scope: scripts/download_rais.py + etl/src/bracc_etl/pipelines/rais.py.
- If upstream is behind a form/captcha wall, report back with that as the blocker — don't scrape.

## Deliverable

URL + format, output size, extract counts on smoke, contract snippet:
```json
{
  "pipeline_id": "rais",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/rais/*"],
  "blocking_reason_if_any": "Multi-GB annual dumps; full-history requires >100 GB disk.",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_rais.py --output-dir ../data/rais --year 2023"]
}
```
Plus files modified.
```

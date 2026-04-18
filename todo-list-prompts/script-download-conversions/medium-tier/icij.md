# Medium — `icij` (OffShoreLeaks / Pandora Papers / Panama Papers)

**URL hints**:
- Download page: `https://offshoreleaks.icij.org/pages/download`
- Direct CSV bundle (as of 2026): `https://offshoreleaks.icij.org/data/download` or `https://storage.googleapis.com/offshoreleaks-data/offshore_leaks.zip`
- Neo4j import format: ICIJ publishes node/relationship CSVs designed for Neo4j bulk import (schema: `nodes-entities.csv`, `nodes-officers.csv`, `relationships.csv`, etc.)

**Gotchas**:
- Bundle is ~1 GB zipped, ~5 GB extracted.
- URL may require a click-through on the webpage (set a User-Agent header and follow redirects).
- Pipeline likely filters to Brazilian entities — check `_is_brazilian` or similar helper.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: convert `icij` (ICIJ OffShoreLeaks consolidated database) from file_manifest to script_download.

## Task

1. Read etl/src/bracc_etl/pipelines/icij.py — which of the node/relationship CSVs does extract() consume? Usually: nodes-entities.csv, nodes-officers.csv, nodes-addresses.csv, relationships.csv.
2. Find the current bundle URL. Try in order:
   - https://offshoreleaks.icij.org/pages/download (parse HTML for direct link)
   - https://storage.googleapis.com/offshoreleaks-data/offshore_leaks.zip (historical direct)
   - https://offshoreleaks.icij.org/data/download
3. Add fetch_to_disk(output_dir, *, limit=None, user_agent=...). Download the zip (~1 GB), extract in-memory only the files the pipeline needs (avoid writing 5 GB of unused CSVs to disk).
4. Create scripts/download_icij.py with --output-dir, --limit (truncate rows per file), --user-agent.
5. Smoke with --limit 1000.

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- File scope: scripts/download_icij.py + etl/src/bracc_etl/pipelines/icij.py.
- **Disk safety**: only write the CSVs the pipeline actually uses.
- If ICIJ now requires an account/token, report back as blocker.

## Deliverable

URL used, output file list + sizes, extract+transform counts, contract snippet:
```json
{
  "pipeline_id": "icij",
  "acquisition_mode": "script_download",
  "required_inputs": ["data/icij/*"],
  "blocking_reason_if_any": "-",
  "core": false,
  "download_commands": ["cd /workspace/etl && uv run python ../scripts/download_icij.py --output-dir ../data/icij"]
}
```
Plus caveats + files modified.
```

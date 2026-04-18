# Canonical Agent prompt template — script_download conversion

Paste the block below into the `Agent` tool (general-purpose subagent), replacing the
four `{{...}}` placeholders. Per-pipeline files in `easy-recovery/`, `medium-tier/`,
`hard-tier/` provide pre-filled versions — prefer those over manually filling this
template.

---

## Prompt template

```
## Context
Brazilian fiscal-data project `br-acc` (fork of brunoclz). Working dir:
`/home/alladrian/PycharmProjects/br-acc`. Stack: Python 3.14, `uv`, pipelines under
`etl/src/bracc_etl/pipelines/*.py`.

Project goal: convert pipelines from `acquisition_mode: file_manifest` (manual
placement) to `script_download` (automated CLI). 26 pipelines already converted.
Pattern: `fetch_to_disk()` at module level + thin CLI at `scripts/download_<name>.py`
+ contract entry flip in `config/bootstrap_all_contract.yml`.

## Your task — Convert `{{PIPELINES_COMMA_SEPARATED}}`

{{PIPELINE_URL_AND_SCHEMA_HINTS}}

### Workflow per pipeline

1. **Read** `etl/src/bracc_etl/pipelines/<name>.py` carefully — note what files
   `extract()` globs/reads (columns, separator, encoding). Your output files MUST match
   that layout exactly (same encoding, separator, column names — remap if upstream
   renamed anything).
2. **Add `fetch_to_disk(output_dir, ...)` function** at module level. Use `httpx`
   (already a dep). If upstream gives ZIPs, extract in-memory with `zipfile`.
3. **Create `scripts/download_<name>.py`** — argparse CLI, thin call to
   `fetch_to_disk`. Accept `--output-dir`, `--limit` (for smoke), and any
   source-specific flags (`--year`, `--month`, etc.).
4. **Smoke-test**: run the CLI, then exercise pipeline extract+transform:
   ```python
   from unittest.mock import MagicMock
   from bracc_etl.pipelines.<name> import <Name>Pipeline
   # Pipeline looks under data_dir/<name>/; arrange accordingly.
   p = <Name>Pipeline(driver=MagicMock(), data_dir=<root>)
   p.extract(); p.transform()
   print(counts)
   ```

### IMPORTANT CONSTRAINTS

- **DO NOT edit `config/bootstrap_all_contract.yml`** — return contract snippets in
  the report; the orchestrator merges atomically to avoid races.
- **DO NOT commit** anything.
- **DO NOT update memory** or CLAUDE.md.
- **DO NOT add new dependencies** — `httpx`, `pandas`, `lxml` are available.
- **DO NOT touch** any pipeline other than the ones assigned.
- **File scope** for this task: {{FILE_SCOPE}}. Nothing else.
- If one pipeline is stuck (upstream down, unusual schema, auth issue), skip it and
  note why; don't block the others.
- For bulk files >100 MB: `--limit` for smoke; allow unbounded via explicit flag.

### Reference implementations (study before writing)

- `etl/src/bracc_etl/pipelines/tcu.py` + `scripts/download_tcu.py` — minimal pattern
- `etl/src/bracc_etl/pipelines/tesouro_emendas.py` — Portal CSV pattern
- `etl/src/bracc_etl/pipelines/siop.py` — Portal consolidated-ZIP split-by-year
- `etl/src/bracc_etl/pipelines/camara.py` `_download_ceap_csv` — ZIP workaround for
  CSV endpoints with upstream null-byte padding bugs

### Deliverable (under 400 words)

For each pipeline (attempted AND skipped):

- URL used (or "N/A — <reason>" for skipped).
- Output files + sizes.
- Extract + transform counts (real numbers).
- Exact contract snippet (JSON):
  ```json
  {
    "pipeline_id": "<name>",
    "acquisition_mode": "script_download",
    "required_inputs": ["data/<name>/*"],
    "blocking_reason_if_any": "-",
    "core": false,
    "download_commands": [
      "cd /workspace/etl && uv run python ../scripts/download_<name>.py --output-dir ../data/<name>"
    ]
  }
  ```

Plus: caveats, files created/modified list.
```

---

## Running multiple tasks in parallel

Each agent gets 1-3 pipelines. File scopes must be disjoint (different
`<name>.py` + `download_<name>.py`), but **all agents conflict on
`config/bootstrap_all_contract.yml`** — that's why the constraint tells them not to
touch it. The orchestrator merges snippets after all agents return.

Example merge code:

```python
import json
PATH = "config/bootstrap_all_contract.yml"
with open(PATH) as f:
    data = json.loads(f.read())

UPDATES = {  # pipeline_id -> download_command (from agent snippets)
    "pipeline_a": "cd /workspace/etl && uv run python ../scripts/download_pipeline_a.py --output-dir ../data/pipeline_a",
    # ...
}
for s in data["sources"]:
    pid = s.get("pipeline_id")
    if pid in UPDATES and s.get("acquisition_mode") == "file_manifest":
        s["acquisition_mode"] = "script_download"
        s["download_commands"] = [UPDATES[pid]]

with open(PATH, "w") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
    f.write("\n")
```

## Common gotchas (from prior sessions)

- **Portal da Transparência consolidated-file trick**: many `/<dataset>/<year>` URLs
  302-redirect to the same cumulative ZIP. Download once, split by year locally.
- **CGU widget-mode `DIA`**: CEIS/CNEP/CEAF/CEPIM/LENIENCY only serve the
  current-published-day snapshot (historical dates 403). Scrape the landing page's
  `arquivos.push({"ano", "mes", "dia"})` to discover the right date.
- **Portal masked CPFs**: many datasets redact CPF as `***.NNN.NNN-**` —
  Person-linking relationships will be 0 by design. Not a regression.
- **Upstream column rename**: newer CGU/IBAMA CSVs often renamed columns vs. what
  existing pipelines expect. `fetch_to_disk` must remap (don't change the pipeline's
  `extract()`).
- **Runaway downloads**: always provide a `--limit` knob. Default the limit to
  something small for smoke; require explicit override for production scale.

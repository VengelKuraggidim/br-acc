# Hard — `tce_go` (Tribunal de Contas do Estado de Goiás)

**Registry status**: `file_manifest — Pending CSV export schema from TCE-GO dashboards`.

**Why hard**:
- TCE-GO publishes via JavaScript-rendered dashboards (Power BI, Qlik Sense, or similar) at `https://www.tce.go.gov.br/`.
- No bulk CSV/JSON export observed from the public portal.
- Data useful for the graph (contas julgadas, pareceres prévios, sancionados) lives in dashboard iframes that use POST-based JSON APIs with session tokens.

---

## Paste-ready Agent prompt

```
## Context
Brazilian fiscal-data project `br-acc`. Working dir: /home/alladrian/PycharmProjects/br-acc.

Goal: EVALUATE whether `tce_go` can be converted from file_manifest to script_download.

Registry explicitly says upstream has "Pending CSV export schema" — likely a blocker. Confirm and document.

## Phase 1 — Investigate only

1. Read etl/src/bracc_etl/pipelines/tce_go.py to understand what extract() expects (even if never loaded production).
2. Explore https://www.tce.go.gov.br/ and dashboards for ANY public endpoint that returns JSON/CSV:
   - Search for /api/, /export/, /download/ paths.
   - Check "dados abertos" menu (may redirect to a FAQ rather than data).
3. Compare with `tcmgo_sancoes` (already script_download, covers MUNICIPAL TCM-GO — different tribunal). Is there schema reuse?
4. Alternative: is there an LAI / CGE (Controladoria Geral do Estado) mirror with structured data?

## Phase 2 (likely N/A)

Only if Phase 1 finds a clean endpoint. Otherwise: mark skipped + document the blocker clearly for human follow-up (may require direct contact with TCE-GO Ouvidoria).

## Constraints

- DO NOT edit config/bootstrap_all_contract.yml.
- DO NOT commit, update memory, add deps.
- **DO NOT scrape Power BI / Qlik iframes** — fragile, high-risk of breaking on dashboard updates.
- File scope: scripts/download_tce_go.py + etl/src/bracc_etl/pipelines/tce_go.py.

## Deliverable

Phase 1 memo only (likely the full report). State clearly whether the conversion is:
- Feasible (with URL) → include snippet.
- Not feasible with public interfaces → include explicit skip reason so the contract blocking_reason_if_any field can be updated.
```

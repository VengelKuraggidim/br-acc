// Agrega IngestionRun por source_id: quantas runs, última data, total de
// rows carregadas (somando runs) e statuses observados. Alimenta os
// badges live da aba "Fontes" na PWA.
//
// rows_loaded pode ser NULL em runs antigas — coalesce pra 0.
// started_at pode ser NULL se a run nunca começou (edge case); max()
// ignora NULLs.
MATCH (r:IngestionRun)
WHERE r.source_id IS NOT NULL
RETURN r.source_id AS source_id,
       count(r) AS runs,
       max(r.started_at) AS last_run_at,
       sum(coalesce(r.rows_loaded, 0)) AS total_rows,
       collect(DISTINCT r.status) AS statuses

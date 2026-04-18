// Verba indenizatória de um deputado estadual GO (ALEGO) — leitura do grafo,
// zero live-call.
//
// Escopo: label :StateLegislator (pipeline ``alego``). A ingestão atual usa
// uma rel tipada como ``GASTOU_COTA_GO`` — alinhada com o padrão ``INCURRED``
// da Câmara Federal mas específica do escopo estadual GO (mantida por
// compatibilidade com fixtures/ETL já ingerido).
//
// Rel: (:StateLegislator)-[:GASTOU_COTA_GO]->(:LegislativeExpense {source:'alego'})
// Props do nó de despesa (pipeline ``alego``):
//   tipo           STRING   — ``tipo_despesa / subgrupo`` (já concatenado)
//   amount         FLOAT    — valor indenizado em BRL
//   date           STRING   — YYYY-MM-DD (parse_date)
//   uf             STRING   — "GO"
//   source         STRING   — "alego"
//
// Filtro por ano: a prop ``date`` é ISO; pegamos os anos como substring
// pra evitar migração do pipeline (que hoje não grava ``ano`` separado).
MATCH (l:StateLegislator {legislator_id: $legislator_id})
      -[r:GASTOU_COTA_GO]->
      (e:LegislativeExpense {source: 'alego'})
WHERE ($anos IS NULL OR size($anos) = 0)
   OR (e.date IS NOT NULL AND toInteger(substring(e.date, 0, 4)) IN $anos)
RETURN e.tipo AS tipo_raw,
       e.amount AS valor,
       substring(coalesce(e.date, ''), 0, 4) AS ano

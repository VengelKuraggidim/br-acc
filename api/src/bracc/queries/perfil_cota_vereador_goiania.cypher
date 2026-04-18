// Cota/despesas de gabinete de um vereador da Camara Municipal de Goiania —
// leitura do grafo, zero live-call.
//
// Escopo: label :GoVereador (pipeline ``camara_goiania``). A ingestao cria
// a rel ``DESPESA_GABINETE`` entre o vereador e o no ``GoCouncilExpense``
// (equivalente municipal de CEAP/verba ALEGO).
//
// Rel: (:GoVereador)-[:DESPESA_GABINETE]->(:GoCouncilExpense {source:'camara_goiania'})
// Props do no de despesa (pipeline ``camara_goiania``):
//   type           STRING   — categoria bruta do portal
//   description    STRING   — descricao livre
//   amount         FLOAT    — valor em BRL
//   date           STRING   — YYYY-MM-DD (parse_date)
//   year           STRING   — ano da despesa (ja separado pelo pipeline)
//   source         STRING   — "camara_goiania"
//
// Filtro por ano: ``year`` ja vem separado do pipeline; se ausente, deriva
// do prefixo de ``date`` por defesa em profundidade (mesmo padrao da
// query ALEGO).
MATCH (v:GoVereador {vereador_id: $vereador_id})
      -[r:DESPESA_GABINETE]->
      (e:GoCouncilExpense {source: 'camara_goiania'})
WHERE ($anos IS NULL OR size($anos) = 0)
   OR (
        (e.year IS NOT NULL AND toInteger(e.year) IN $anos)
        OR (
             (e.year IS NULL OR e.year = '')
             AND e.date IS NOT NULL
             AND toInteger(substring(e.date, 0, 4)) IN $anos
        )
      )
RETURN e.type AS tipo_raw,
       e.amount AS valor,
       coalesce(e.year, substring(coalesce(e.date, ''), 0, 4)) AS ano

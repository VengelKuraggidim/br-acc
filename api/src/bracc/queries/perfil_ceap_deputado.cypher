// CEAP de um deputado federal GO (leitura do grafo — não live-call).
//
// Escopo: label :FederalLegislator (pipeline camara_politicos_go).
// Rel: (:FederalLegislator)-[:INCURRED {tipo: 'CEAP'}]->(:LegislativeExpense)
// Props do nó de despesa: tipo_despesa, valor_liquido, ano, mes.
//
// Filtro por ano permite reproduzir o default do Flask — últimos 2 anos —
// sem puxar todo o histórico ingerido (desde 2020 pelo pipeline).
MATCH (l:FederalLegislator {id_camara: $id_camara})-[r:INCURRED {tipo: 'CEAP'}]->(e:LegislativeExpense)
WHERE e.ano IN $anos
RETURN e.tipo_despesa AS tipo_raw,
       e.valor_liquido AS valor,
       e.ano AS ano

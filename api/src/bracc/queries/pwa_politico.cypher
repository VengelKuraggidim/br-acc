// PWA /politico/{entity_id} lookup.
//
// Encontra um FederalLegislator por id_camara OU pelo legislator_id
// estável (formato ``camara_{id_camara}``) e devolve o nó + agregação
// CEAP por ano com provenance aninhada do próprio nó do legislador
// (campo-a-campo pra o router reaproveitar o unpacker
// ``_extract_provenance``).
//
// A agregação do CEAP traz total por ano + ano mais recente visto.
MATCH (p:FederalLegislator)
WHERE p.id_camara = $entity_id
   OR p.legislator_id = $entity_id
   OR elementId(p) = $entity_id
WITH p
OPTIONAL MATCH (p)-[r:INCURRED]->(e:LegislativeExpense {tipo: 'CEAP'})
WITH p,
     collect({ano: e.ano, mes: e.mes, valor: e.valor_liquido,
              fornecedor_cnpj: e.fornecedor_cnpj,
              fornecedor_nome: e.fornecedor_nome,
              tipo_despesa: e.tipo_despesa}) AS despesas
RETURN p AS legislator,
       elementId(p) AS element_id,
       despesas
LIMIT 1

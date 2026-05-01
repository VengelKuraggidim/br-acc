// Contas julgadas irregulares pelo TCE-GO ligadas ao politico.
//
// Le :TceGoIrregularAccount via :IMPEDIDO_TCE_GO. O nome do TODO original
// era "TceGoDecision" mas no grafo local quem carrega o conteudo
// (julgamento, motivo, processo) e o :TceGoIrregularAccount linkado por
// :IMPEDIDO_TCE_GO ao :Person stub via CPF — :TceGoDecision ficou como
// nivel de processo agregado, sem rels com Person.
//
// Cluster-walk identico ao perfil_historico_eleitoral.
//
// Parametros:
//   $entity_id STRING — mesmos seis formatos da query principal.
//
// Shape: lista ordenada por ano DESC.
//   contas [list of dict]
//     account_id STRING
//     ano INT (ou null)
//     cargo STRING
//     processo STRING
//     julgamento STRING
//     motivo STRING
//     uf STRING
//     pdf_url STRING
//     fonte_url STRING
CALL {
    MATCH (p)
    WHERE elementId(p) = $entity_id
    RETURN p
  UNION
    MATCH (p:FederalLegislator {id_camara: $entity_id})
    RETURN p
  UNION
    MATCH (p:StateLegislator {legislator_id: $entity_id})
    RETURN p
  UNION
    MATCH (p:Senator {id_senado: $entity_id})
    RETURN p
  UNION
    MATCH (p:Senator {senator_id: $entity_id})
    RETURN p
  UNION
    MATCH (:CanonicalPerson {canonical_id: $entity_id})-[:REPRESENTS]->(p)
    RETURN p
}
WITH collect(DISTINCT p) AS seeds
UNWIND seeds AS seed
OPTIONAL MATCH (seed)<-[:REPRESENTS]-(:CanonicalPerson)-[:REPRESENTS]->(sib)
WITH seeds, collect(DISTINCT sib) AS sibs
WITH apoc.coll.toSet(seeds + sibs) AS cluster
UNWIND cluster AS pessoa
OPTIONAL MATCH (pessoa)-[:IMPEDIDO_TCE_GO]->(c:TceGoIrregularAccount)
WITH c
WHERE c IS NOT NULL
WITH DISTINCT c
ORDER BY coalesce(c.ano, 0) DESC
RETURN collect({
    account_id: c.account_id,
    ano: c.ano,
    cargo: coalesce(c.cargo, ''),
    processo: coalesce(c.processo, ''),
    julgamento: coalesce(c.julgamento, ''),
    motivo: coalesce(c.motivo, ''),
    uf: coalesce(c.uf, ''),
    pdf_url: coalesce(c.pdf_url, ''),
    fonte_url: coalesce(c.source_url, '')
}) AS contas

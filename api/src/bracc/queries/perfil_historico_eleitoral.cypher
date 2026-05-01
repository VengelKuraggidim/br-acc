// Historico eleitoral (TSE) do politico — cluster-aware.
//
// As eleicoes (:Election) ligam-se via :CANDIDATO_EM ao :Person TSE,
// nao ao :FederalLegislator/:Senator/:StateLegislator/:GoVereador.
// Pra surface o historico no perfil de qualquer cargo, fazemos a mesma
// resolucao de cluster canonico que perfil_bens_declarados.cypher faz —
// pegamos todos os nos-irmaos via :REPRESENTS e olhamos CANDIDATO_EM em
// qualquer um deles.
//
// Parametros:
//   $entity_id STRING — aceita os mesmos seis formatos da query principal:
//     elementId(p), id_camara, legislator_id, id_senado, senator_id,
//     ou canonical_id (canon_*).
//
// Shape de retorno: collect ordenado por ano DESC.
//   eleicoes [list of dict] — cada candidatura com:
//     ano INT
//     cargo STRING
//     uf STRING
//     municipio STRING (pode ser null)
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
OPTIONAL MATCH (pessoa)-[:CANDIDATO_EM]->(e:Election)
WITH e
WHERE e IS NOT NULL AND e.year IS NOT NULL
WITH DISTINCT e
ORDER BY e.year DESC
RETURN collect({
    ano: e.year,
    cargo: coalesce(e.cargo, ''),
    uf: coalesce(e.uf, ''),
    municipio: e.municipio
}) AS eleicoes

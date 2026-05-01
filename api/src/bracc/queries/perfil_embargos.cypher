// Embargos ambientais (IBAMA/SEMAD) ligados ao politico — direto via CPF.
//
// :Embargo no grafo local liga-se principalmente a :Person via
// :EMBARGADA (84k rels), mas tambem a :Company (17k rels). O TODO
// original sugeria 2-hop via :SOCIO_DE-:EMBARGADA, mas no local Person
// →SOCIO_DE→Company→EMBARGADA→Embargo retorna 0 — fica so o caminho
// direto. Quando :Company tiver embargos do politico via socio, dah pra
// estender com UNION.
//
// Cluster-walk identico ao perfil_historico_eleitoral.
//
// Parametros:
//   $entity_id STRING — mesmos seis formatos da query principal.
//
// Shape: lista ordenada por date DESC.
//   embargos [list of dict]
//     embargo_id STRING
//     infracao STRING
//     auto_infracao STRING
//     data STRING
//     municipio STRING
//     uf STRING
//     biome STRING
//     area_ha FLOAT (ou null)
//     processo STRING
//     fonte STRING
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
OPTIONAL MATCH (pessoa)-[:EMBARGADA]->(e:Embargo)
WITH e
WHERE e IS NOT NULL
WITH DISTINCT e
ORDER BY coalesce(e.date, '') DESC
RETURN collect({
    embargo_id: e.embargo_id,
    infracao: coalesce(e.infraction, ''),
    auto_infracao: coalesce(e.auto_infracao, ''),
    data: coalesce(e.date, ''),
    municipio: coalesce(e.municipio, ''),
    uf: coalesce(e.uf, ''),
    biome: coalesce(e.biome, ''),
    area_ha: e.area_ha,
    processo: coalesce(e.processo, ''),
    fonte: coalesce(e.source, '')
}) AS embargos

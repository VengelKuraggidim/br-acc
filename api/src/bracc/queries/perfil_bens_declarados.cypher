// Bens declarados (TSE) do politico — cluster-aware.
//
// Os :DeclaredAsset estao SEMPRE no :Person (pipeline tse_bens_go grava no
// CPF, nao no id_camara/legislator_id/id_senado). Pra que o perfil de
// :FederalLegislator/:Senator/:StateLegislator/:GoVereador surface os
// bens, fazemos a mesma resolucao de cluster canonico que
// perfil_politico_connections.cypher faz pra edges — pegamos TODOS os
// nos-irmaos via :REPRESENTS e olhamos DECLAROU_BEM em qualquer um deles.
//
// Parametros:
//   $entity_id STRING — aceita os mesmos seis formatos da query principal:
//     elementId(p), id_camara, legislator_id, id_senado, senator_id,
//     ou canonical_id (canon_*).
//
// Shape de retorno: collect ordenado por (election_year DESC, asset_value DESC).
//   bens [list of dict] — cada bem com:
//     ano INT
//     tipo STRING                       — asset_type (Veiculo, Imovel, ...)
//     descricao STRING                  — asset_description
//     valor FLOAT                       — toFloat(asset_value)
//     source_id STRING
//     source_record_id STRING           — pode ser null
//     source_url STRING
//     ingested_at STRING
//     run_id STRING
//     source_snapshot_uri STRING        — pode ser null
//
// Sem LIMIT — bens por candidato sao em geral < 100 (top observado: 15
// bens). Cap acontece naturalmente.
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
// Caminhada no cluster canonico — pega todos os nos-irmaos, inclusive o seed.
OPTIONAL MATCH (seed)<-[:REPRESENTS]-(:CanonicalPerson)-[:REPRESENTS]->(sib)
WITH seeds, collect(DISTINCT sib) AS sibs
WITH apoc.coll.toSet(seeds + sibs) AS cluster
UNWIND cluster AS pessoa
OPTIONAL MATCH (pessoa)-[:DECLAROU_BEM]->(a:DeclaredAsset)
WITH a
WHERE a IS NOT NULL
WITH DISTINCT a
ORDER BY a.election_year DESC, toFloat(a.asset_value) DESC
RETURN collect({
    ano: a.election_year,
    tipo: coalesce(a.asset_type, ''),
    descricao: coalesce(a.asset_description, ''),
    valor: toFloat(a.asset_value),
    source_id: a.source_id,
    source_record_id: a.source_record_id,
    source_url: a.source_url,
    ingested_at: a.ingested_at,
    run_id: a.run_id,
    source_snapshot_uri: a.source_snapshot_uri
}) AS bens

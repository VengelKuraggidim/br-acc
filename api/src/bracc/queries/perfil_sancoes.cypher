// Sancoes administrativas (CGU CEIS/CNEP/Expulsao) ligadas ao politico.
//
// Cluster-walk via :CanonicalPerson identico ao perfil_historico_eleitoral —
// :SANCIONADA pode estar em qualquer no-irmao do cluster (CPF formatado,
// CPF mascarado, sq_candidato, id_camara). Pega todas e dedup por
// sanction_id.
//
// Parametros:
//   $entity_id STRING — mesmos seis formatos da query principal.
//
// Shape: lista ordenada por date_start DESC.
//   sancoes [list of dict]
//     sanction_id STRING (ou null)
//     tipo STRING (ex.: "INIDONEO", "IMPEDIDO", "MULTA")
//     motivo STRING
//     orgao STRING (vem em ``source`` quando :Sanction nao tem orgao proprio)
//     data_inicio STRING (ISO ou vazio)
//     data_fim STRING (ISO ou vazio)
//     fonte STRING (CEIS, CNEP, etc.)
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
OPTIONAL MATCH (pessoa)-[:SANCIONADA]->(s:Sanction)
WITH s
WHERE s IS NOT NULL
WITH DISTINCT s
ORDER BY coalesce(s.date_start, '') DESC
RETURN collect({
    sanction_id: s.sanction_id,
    tipo: coalesce(s.type, ''),
    motivo: coalesce(s.reason, ''),
    fonte: coalesce(s.source, ''),
    data_inicio: coalesce(s.date_start, ''),
    data_fim: coalesce(s.date_end, '')
}) AS sancoes

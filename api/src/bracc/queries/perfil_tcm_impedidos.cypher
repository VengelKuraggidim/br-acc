// Impedidos TCM-GO (Tribunal de Contas dos Municipios) ligados ao politico.
//
// Diferente de :Sanction e :TceGoIrregularAccount, :TcmGoImpedido nao
// tem rel direta com :Person no grafo local. O ``imp.document`` vem
// MASCARADO (formato ``76***.***-**``) — fonte upstream redige CPF na
// publicacao da lista. Match exato por CPF e impossivel.
//
// Heuristica usada: nome exato (UPPER + trim, sem acento). Risco de
// falso positivo em homonimos comuns; card no PWA expoe ``fonte_url``
// pro usuario verificar. LAI 21021 (2026-05-18) deve liberar a lista
// nominal completa — quando responder, dah pra apertar o match.
//
// Parametros:
//   $entity_id STRING — mesmos seis formatos da query principal.
//
// Shape: lista ordenada por data_inicio DESC.
//   impedidos [list of dict]
//     impedido_id STRING
//     processo STRING
//     motivo STRING
//     data_inicio STRING
//     data_fim STRING
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
WITH [pessoa IN cluster WHERE pessoa.name IS NOT NULL AND pessoa.name <> '' |
      apoc.text.clean(toUpper(pessoa.name))] AS nomes
UNWIND nomes AS nome_match
OPTIONAL MATCH (imp:TcmGoImpedido)
WHERE apoc.text.clean(toUpper(coalesce(imp.name, ''))) = nome_match
WITH imp
WHERE imp IS NOT NULL
WITH DISTINCT imp
ORDER BY coalesce(imp.data_inicio, '') DESC
RETURN collect({
    impedido_id: imp.impedido_id,
    processo: coalesce(imp.processo, ''),
    motivo: coalesce(imp.motivo, ''),
    data_inicio: coalesce(imp.data_inicio, ''),
    data_fim: coalesce(imp.data_fim, ''),
    fonte_url: coalesce(imp.source_url, '')
}) AS impedidos

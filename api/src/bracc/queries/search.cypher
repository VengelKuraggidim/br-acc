CALL db.index.fulltext.queryNodes("entity_search", $query)
YIELD node, score
WITH node, score, labels(node) AS node_labels
WHERE NONE(label IN node_labels WHERE label IN ['User', 'Investigation', 'Annotation', 'Tag'])
  AND (NOT $hide_person_entities OR NONE(label IN node_labels WHERE label IN ['Person', 'Partner']))
  AND ($entity_type IS NULL
       OR ANY(label IN node_labels WHERE toLower(label) = $entity_type))
WITH node, score, node_labels
ORDER BY score DESC
SKIP $skip
LIMIT $limit
// Resolve cluster canônico do nó (se houver) para o caller deduplicar
// resultados que apontam para a mesma pessoa (Person + FederalLegislator
// + Person TSE no mesmo CanonicalPerson). OPTIONAL MATCH aplica-se só
// aos rows pós-paginação para não inflar o custo do fulltext.
OPTIONAL MATCH (node)<-[:REPRESENTS]-(cp:CanonicalPerson)
WITH node, score, node_labels, head(collect(DISTINCT cp.canonical_id)) AS canonical_id
RETURN node, score, node_labels,
       elementId(node) AS node_id,
       coalesce(node.cpf, node.cnpj, node.contract_id, node.sanction_id, node.amendment_id, node.cnes_code, node.finance_id, node.embargo_id, node.school_id, node.convenio_id, node.stats_id, elementId(node)) AS document_id,
       canonical_id
ORDER BY score DESC

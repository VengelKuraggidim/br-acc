CALL db.index.fulltext.queryNodes("entity_search", $query)
YIELD node, score
WITH node, labels(node) AS node_labels
WHERE NONE(label IN node_labels WHERE label IN ['User', 'Investigation', 'Annotation', 'Tag'])
  AND (NOT $hide_person_entities OR NONE(label IN node_labels WHERE label IN ['Person', 'Partner']))
  AND ($entity_type IS NULL
       OR ANY(label IN node_labels WHERE toLower(label) = $entity_type))
// Conta deduplicando por canonical_id quando o nó faz parte de um cluster
// CanonicalPerson (Senator+FederalLegislator+Person TSE da mesma pessoa
// vira 1 só); nós sem cluster usam elementId pra contar individualmente.
OPTIONAL MATCH (node)<-[:REPRESENTS]-(cp:CanonicalPerson)
WITH coalesce(cp.canonical_id, elementId(node)) AS dedup_key
RETURN count(DISTINCT dedup_key) AS total

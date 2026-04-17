MATCH (p:GoProcurement)
WHERE $query = ''
   OR toLower(p.object) CONTAINS toLower($query)
   OR toLower(p.agency_name) CONTAINS toLower($query)
   OR toLower(p.municipality) CONTAINS toLower($query)
RETURN p,
       elementId(p) AS node_id
ORDER BY p.published_at DESC
LIMIT $limit

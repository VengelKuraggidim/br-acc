MATCH (e:StateEmployee)
WHERE $query = ''
   OR toLower(e.name) CONTAINS toLower($query)
RETURN e,
       elementId(e) AS node_id
ORDER BY e.name ASC
LIMIT $limit

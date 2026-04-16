MATCH (p:Person)
WHERE p.uf = $uf
RETURN count(p) AS total

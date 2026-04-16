MATCH (e:Election)
WHERE e.uf = $uf
RETURN e.cargo AS cargo, count(DISTINCT e.candidate_sq) AS total
ORDER BY total DESC
LIMIT 20

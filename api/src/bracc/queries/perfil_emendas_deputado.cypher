MATCH (l:FederalLegislator {id_camara: $id_camara})-[:PROPOS]->(a:Amendment)
RETURN a.amendment_id AS id,
       a.tipo AS tipo,
       a.funcao AS funcao,
       a.municipio AS municipio,
       a.uf AS uf,
       a.valor_empenhado AS valor_empenhado,
       a.valor_pago AS valor_pago,
       a.ano AS ano
ORDER BY coalesce(a.valor_pago, a.valor_empenhado, 0) DESC

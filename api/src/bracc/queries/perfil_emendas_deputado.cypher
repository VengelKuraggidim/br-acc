MATCH (l:FederalLegislator {id_camara: $id_camara})-[:PROPOS]->(a:Amendment)
OPTIONAL MATCH (a)-[:BENEFICIOU]->(c:Company)
// collect(c) filtra nulls automaticamente; [0] devolve null se vazio,
// preservando a ausência de beneficiário quando a emenda ainda não
// teve convênio/contrato emitido.
WITH a, collect(c)[0] AS beneficiario
RETURN a.amendment_id AS id,
       a.tipo AS tipo,
       a.funcao AS funcao,
       a.municipio AS municipio,
       a.uf AS uf,
       a.valor_empenhado AS valor_empenhado,
       a.valor_pago AS valor_pago,
       a.ano AS ano,
       beneficiario.cnpj AS beneficiario_cnpj,
       beneficiario.razao_social AS beneficiario_nome,
       beneficiario.data_abertura AS beneficiario_data_abertura
ORDER BY coalesce(a.valor_pago, a.valor_empenhado, 0) DESC

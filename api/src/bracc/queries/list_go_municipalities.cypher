MATCH (m:GoMunicipality)
CALL (m) {
  MATCH (m)-[:ARRECADOU]->(r:MunicipalRevenue)
    WHERE r.account STARTS WITH 'RECEITAS (EXCETO INTRA'
  RETURN sum(coalesce(r.amount, 0.0)) AS total_revenue
}
CALL (m) {
  MATCH (m)-[:GASTOU]->(e:MunicipalExpenditure)
    WHERE e.account STARTS WITH 'DESPESAS (EXCETO INTRA'
  RETURN sum(coalesce(e.amount, 0.0)) AS total_expenditure
}
RETURN m,
       elementId(m) AS node_id,
       total_revenue,
       total_expenditure
ORDER BY m.name ASC

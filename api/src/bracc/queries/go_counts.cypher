CALL () {
  MATCH (n:StateEmployee) RETURN count(n) AS state_employees
}
CALL () {
  MATCH (n:StateEmployee {is_commissioned: true}) RETURN count(n) AS commissioned
}
CALL () {
  MATCH (n:GoMunicipality) RETURN count(n) AS municipalities
}
CALL () {
  MATCH (n:GoProcurement) RETURN count(n) AS procurements
}
CALL () {
  MATCH (n:MunicipalGazetteAct)
  WHERE toLower(coalesce(n.territory_id, '')) STARTS WITH '52'
     OR toLower(coalesce(n.territory_name, '')) CONTAINS 'goi'
  RETURN count(n) AS appointments
}
RETURN state_employees, commissioned, municipalities, procurements, appointments

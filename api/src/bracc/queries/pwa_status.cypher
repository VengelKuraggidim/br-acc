// Aggregated counters powering the PWA landing-page ``/status``
// endpoint. Mirrors what the legacy Flask aggregator composed from
// ``meta_stats`` + ``person_counts_by_uf`` + ``go_counts``, but runs
// in a single session so the PWA boot cost is one round-trip.
//
// ``vereadores_goiania`` is intentionally scoped to the capital
// (``municipio = 'GOIANIA'``) to match the field name consumers
// expect; otherwise the count would aggregate all 246 GO camaras.
// O filtro ``year = 2024`` mantém o número alinhado ao tooltip da
// home ("último pleito, 2024"). Sem ele, a contagem somava 2020 +
// 2024 (memo: federal/estadual/senador só têm 2022 ingerido, então
// o problema é exclusivo do vereador, que tem dois ciclos no grafo).
CALL () { MATCH (n) RETURN count(n) AS total_nos }
CALL () { MATCH ()-[r]->() RETURN count(r) AS total_relacionamentos }
CALL () {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'DEPUTADO FEDERAL'})
  RETURN count(DISTINCT p) AS deputados_federais
}
CALL () {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'DEPUTADO ESTADUAL'})
  RETURN count(DISTINCT p) AS deputados_estaduais
}
CALL () {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'SENADOR'})
  RETURN count(DISTINCT p) AS senadores
}
CALL () {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'VEREADOR', municipio: 'GOIANIA', year: 2024})
  RETURN count(DISTINCT p) AS vereadores_goiania
}
CALL () { MATCH (n:StateEmployee) RETURN count(n) AS servidores_estaduais }
CALL () {
  MATCH (n:StateEmployee {is_commissioned: true})
  RETURN count(n) AS cargos_comissionados
}
CALL () { MATCH (n:GoMunicipality) RETURN count(n) AS municipios_go }
CALL () { MATCH (n:GoProcurement) RETURN count(n) AS licitacoes_go }
CALL () {
  MATCH (n:MunicipalGazetteAct)
  WHERE toLower(coalesce(n.territory_id, '')) STARTS WITH '52'
     OR toLower(coalesce(n.territory_name, '')) CONTAINS 'goi'
  RETURN count(n) AS nomeacoes_go
}
RETURN total_nos, total_relacionamentos,
       deputados_federais, deputados_estaduais, senadores,
       vereadores_goiania,
       servidores_estaduais, cargos_comissionados,
       municipios_go, licitacoes_go, nomeacoes_go

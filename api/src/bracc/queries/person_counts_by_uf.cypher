CALL {
  MATCH (p:Person {uf: $uf})
  RETURN count(p) AS total
}
CALL {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'DEPUTADO FEDERAL'})
  RETURN count(DISTINCT p) AS deputados_federais
}
CALL {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'DEPUTADO ESTADUAL'})
  RETURN count(DISTINCT p) AS deputados_estaduais
}
CALL {
  // Scope vereadores to the capital (GOIANIA for GO) to keep the count
  // semantically aligned with the `vereadores_goiania` field consumers
  // expect. Without this, the count aggregates candidates across all
  // 246 GO municipalities (>18k), which is misleading.
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'VEREADOR', municipio: 'GOIANIA'})
  RETURN count(DISTINCT p) AS vereadores
}
CALL {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'PREFEITO'})
  RETURN count(DISTINCT p) AS prefeitos
}
CALL {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'SENADOR'})
  RETURN count(DISTINCT p) AS senadores
}
CALL {
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'GOVERNADOR'})
  RETURN count(DISTINCT p) AS governadores
}
RETURN total, deputados_federais, deputados_estaduais, vereadores, prefeitos, senadores, governadores

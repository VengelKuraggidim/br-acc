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
  MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election {uf: $uf, cargo: 'VEREADOR'})
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

// Custo de mandato por cargo eletivo — leitura pura do grafo, zero live-call.
//
// Ingerido pelo pipeline ``custo_mandato_br`` (ver
// ``etl/src/bracc_etl/pipelines/custo_mandato_br.py``). Cargos cobertos no MVP:
// dep_federal, senador, dep_estadual_go, governador_go.
//
// Schema:
//   (:CustoMandato {cargo, esfera, n_titulares, custo_mensal_individual, ...})
//   -[:TEM_COMPONENTE]->(:CustoComponente {componente_id, rotulo, valor_mensal, ...})
//
// Componentes vêm ordenados por ``ordem`` pra exibição estável no PWA
// (subsídio primeiro, depois CEAP/gabinete, depois auxiliares).
MATCH (m:CustoMandato {cargo: $cargo})
OPTIONAL MATCH (m)-[:TEM_COMPONENTE]->(c:CustoComponente)
WITH m, c
ORDER BY coalesce(c.ordem, 999), c.componente_id
RETURN m AS mandato,
       collect(c) AS componentes

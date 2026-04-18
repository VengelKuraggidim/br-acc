// Média CEAP por deputado federal de uma UF.
//
// Amostra top $amostra maiores gastadores (ou todos se ≤ amostra) e
// devolve a média dos totais. Replica a lógica do Flask
// (`backend/apis_externas.py::buscar_media_despesas_estado`) porém lê
// do grafo — sem tocar a API da Câmara. Quando a UF não tem deputados
// ingeridos a query devolve `media = null`.
MATCH (l:FederalLegislator {uf: $uf})-[r:INCURRED {tipo: 'CEAP'}]->(e:LegislativeExpense)
WHERE e.ano IN $anos
WITH l, sum(e.valor_liquido) AS total_deputado
ORDER BY total_deputado DESC
LIMIT $amostra
RETURN avg(total_deputado) AS media

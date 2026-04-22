// Perfil /politico/{entity_id} — traz o nó focal + connections depth=1 pra
// ConexoesService (Fase 04.B) classificar.
//
// Parâmetros:
//   $entity_id STRING — aceita seis formatos (compatibilidade com o PWA):
//     1. elementId(p)             — default dos links internos da API
//     2. p.id_camara              — ID da API da Câmara (:FederalLegislator)
//     3. p.legislator_id          — ID estável "camara_{id_camara}"
//     4. p.id_senado              — ID da API do Senado (:Senator)
//     5. p.senator_id             — ID estável "senado_{id_senado}"
//     6. cp.canonical_id          — ID canônico (:CanonicalPerson) do
//                                   pipeline entity_resolution_politicos_go.
//                                   Resolve pro nó-fonte mais oficial
//                                   (Senator > FederalLegislator >
//                                   StateLegislator > Person).
//
// Resolução de cluster canônico: para QUALQUER formato de entrada acima,
// se o nó match tiver um cluster :CanonicalPerson linkado via :REPRESENTS,
// varre os nós-irmãos e devolve o mais oficial. Isso resolve o sintoma UX
// em que o fulltext ``/buscar-tudo`` devolve o elementId do ``:Person`` TSE
// (sem id_camara → sem emendas, sem CEAP) quando o mesmo cluster tem um
// ``:FederalLegislator`` com emendas carimbadas.
//
// Shape de retorno:
//   politico {dict}    — properties + element_id + labels do político
//   conexoes [list]    — lista de dicts, um por aresta que toca o político,
//                        cada um com:
//     rel_type STRING           — type(r) (SOCIO_DE, DOOU, ...)
//     rel_props {dict}          — properties(r) (valor, amount, ...)
//     source_id STRING          — elementId(startNode(r))
//     target_id STRING          — elementId(endNode(r))
//     target_element_id STRING  — elementId(t) da "outra ponta" da aresta
//                                  (útil quando politico está em source OU target)
//     target_type STRING        — primeira label de t (lowercase em _norm_type
//                                  do service: Company -> company, ...)
//     target_labels [STRING]    — todas as labels do target (pra filtros futuros)
//     target_props {dict}       — properties(t)
//
// Observação: sem LIMIT aqui — o service aplica cap de 50 por categoria
// após classificação, o que é semanticamente mais útil que um LIMIT 1000
// cru (evita tipo X ser zerado por saturação de tipo Y).
// Timeout de 30s deve ser aplicado no driver na hora da chamada.
//
// A resolução do canônico ranqueia o nó-fonte por oficialidade do cargo:
// Senator (0) > FederalLegislator (1) > StateLegislator (2) > Person (3).
// Isso garante que `GET /politico/canon_senado_5895` surface a foto do
// Senator mesmo quando o cluster tem também um :Person TSE histórico —
// e, pela branch B, que clicar num :Person TSE no /buscar-tudo também
// resolva pro :FederalLegislator/:Senator do mesmo cluster.
//
// **Cobertura de edges** — o "nó focal" devolvido é o mais oficial (pros
// props: nome de urna, foto, id_camara), mas as edges vêm da UNIÃO de
// todos os nós-irmãos no cluster canônico. Isso é essencial porque dados
// de pipelines diferentes aterrissam em nós diferentes do cluster:
//   * CEAP da Câmara  → :FederalLegislator
//   * AUTOR_EMENDA    → :Person (pipeline transparencia carimba no CPF
//                       pleno, não no id_camara)
//   * DOOU/CANDIDATO  → :Person TSE
// Sem unir edges de todos os irmãos, o perfil perde emendas/doações
// quando o entity_id bate num :Person mas o cluster tem um cargo mais
// oficial (ou vice-versa). O ``target_element_id`` nas edges aponta pra
// "outra ponta" real da aresta, não pro focal — o ``_adapt_connections``
// no service já trata isso normalmente.
CALL {
    // Branch A: match direto por identificador → ranqueia por label
    // (sem cluster canônico, esse é o único nó retornado).
    MATCH (p)
    WHERE elementId(p) = $entity_id
       OR p.id_camara = $entity_id
       OR p.legislator_id = $entity_id
       OR p.id_senado = $entity_id
       OR p.senator_id = $entity_id
    RETURN p,
           CASE
             WHEN 'Senator' IN labels(p) THEN 0
             WHEN 'FederalLegislator' IN labels(p) THEN 1
             WHEN 'StateLegislator' IN labels(p) THEN 2
             ELSE 3
           END AS source_rank
  UNION
    // Branch B: match direto + caminhada no cluster canônico pra achar
    // nó-irmão mais oficial. :REPRESENTS é direcional CanonicalPerson→source,
    // daí (p_seed)<-[:REPRESENTS]-(cp)-[:REPRESENTS]->(p). Inclui p=p_seed
    // quando o cluster tem só o seed, mas aí o rank é igual ao da Branch A
    // e o tie-breaker no ORDER BY mantém consistência.
    MATCH (p_seed)<-[:REPRESENTS]-(:CanonicalPerson)-[:REPRESENTS]->(p)
    WHERE elementId(p_seed) = $entity_id
       OR p_seed.id_camara = $entity_id
       OR p_seed.legislator_id = $entity_id
       OR p_seed.id_senado = $entity_id
       OR p_seed.senator_id = $entity_id
    RETURN p,
           CASE
             WHEN 'Senator' IN labels(p) THEN 0
             WHEN 'FederalLegislator' IN labels(p) THEN 1
             WHEN 'StateLegislator' IN labels(p) THEN 2
             ELSE 3
           END AS source_rank
  UNION
    // Branch C: match via canonical_id explícito (formato `canon_*`).
    MATCH (:CanonicalPerson {canonical_id: $entity_id})-[:REPRESENTS]->(p)
    RETURN p,
           CASE
             WHEN 'Senator' IN labels(p) THEN 0
             WHEN 'FederalLegislator' IN labels(p) THEN 1
             WHEN 'StateLegislator' IN labels(p) THEN 2
             ELSE 3
           END AS source_rank
}
WITH collect({node: p, rank: source_rank}) AS candidates
// Nó focal = mais oficial (menor rank). Tie-breaker: order original do UNION.
WITH candidates,
     reduce(best = candidates[0], c IN candidates |
         CASE WHEN c.rank < best.rank THEN c ELSE best END
     ) AS focal
WITH focal.node AS p, [c IN candidates | c.node] AS siblings
// União de edges: busca em TODOS os nós-irmãos do cluster, não só no
// focal. Sem isso, perfil do :FederalLegislator perde AUTOR_EMENDA/DOOU
// carimbados no :Person TSE irmão.
UNWIND siblings AS sibling
OPTIONAL MATCH (sibling)-[r]-(t)
WHERE NOT (t:User OR t:Investigation OR t:Annotation OR t:Tag)
  // Exclui edges internas do cluster (REPRESENTS, SAME_AS entre irmãos)
  // pra não poluir a classificação.
  AND NOT type(r) IN ['REPRESENTS']
  AND NOT t IN siblings
// Reescreve source_id/target_id pro focal quando a edge vem de um irmão:
// o ``conexoes_service.classificar`` exige que uma das pontas bata com
// ``politico_entity_id`` (=elementId do focal) — senão dropa a edge.
// Semanticamente correto porque cluster canônico = mesma pessoa real.
// Direção preservada: se sibling era source da rel, focal vira source.
WITH p, siblings, r, t, sibling,
     startNode(r) AS src,
     endNode(r) AS tgt,
     CASE WHEN startNode(r) = sibling THEN elementId(p) ELSE elementId(t) END
         AS source_id,
     CASE WHEN endNode(r) = sibling THEN elementId(p) ELSE elementId(t) END
         AS target_id
WITH p,
     // Deduplica por elementId da rel — evita que a mesma aresta
     // apareça duas vezes quando ambas as pontas caem no UNWIND
     // siblings (filtro `NOT t IN siblings` já previne o caso,
     // DISTINCT é rede de segurança).
     collect(DISTINCT {
         rel_type: type(r),
         rel_props: properties(r),
         source_id: source_id,
         target_id: target_id,
         target_element_id: elementId(t),
         target_type: head(labels(t)),
         target_labels: labels(t),
         target_props: properties(t)
     }) AS conexoes_raw
WITH p, [c IN conexoes_raw WHERE c.rel_type IS NOT NULL | c] AS conexoes
RETURN p {.*, element_id: elementId(p), labels: labels(p)} AS politico,
       conexoes

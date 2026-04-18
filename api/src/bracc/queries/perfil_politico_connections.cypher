// Perfil /politico/{entity_id} — traz o nó focal + connections depth=1 pra
// ConexoesService (Fase 04.B) classificar.
//
// Parâmetros:
//   $entity_id STRING — aceita três formatos (compatibilidade com o PWA):
//     1. elementId(p)             — default dos links internos da API
//     2. p.id_camara              — ID da API da Câmara (:FederalLegislator)
//     3. p.legislator_id          — ID estável "camara_{id_camara}"
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
MATCH (p)
WHERE elementId(p) = $entity_id
   OR p.id_camara = $entity_id
   OR p.legislator_id = $entity_id
WITH p LIMIT 1
OPTIONAL MATCH (p)-[r]-(t)
WHERE NOT (t:User OR t:Investigation OR t:Annotation OR t:Tag)
WITH p, r, t,
     startNode(r) AS src,
     endNode(r) AS tgt
WITH p, collect({
    rel_type: type(r),
    rel_props: properties(r),
    source_id: elementId(src),
    target_id: elementId(tgt),
    target_element_id: elementId(t),
    target_type: head(labels(t)),
    target_labels: labels(t),
    target_props: properties(t)
}) AS conexoes
RETURN p {.*, element_id: elementId(p), labels: labels(p)} AS politico,
       conexoes

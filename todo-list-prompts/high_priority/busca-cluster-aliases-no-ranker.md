# Busca — usar nomes do cluster CanonicalPerson no ranker

> Aberto em 2026-05-02. Caveat documentado em
> `project_er_alego_batch_2026_05_02.md`.

## Sintoma

Buscar `Lincoln Tejota` retorna no topo `LINCOLN MACIEL BARROS`
(Vereador) em vez de `LINCOLN GRAZIANI PEREIRA DA ROCHA` (vice-governador
ex-deputado, conhecido como TEJOTA, agora pareado via ER ao StateLeg
`LINCOLN TEJOTA`).

## Causa

O re-ranker novo (`api/src/bracc/routers/pwa_parity.py::_local_relevance`)
score nome-igual-a-igual:

- `LINCOLN MACIEL BARROS` vs query `LINCOLN TEJOTA` → matches=1 (LINCOLN), classe 4.
- `LINCOLN GRAZIANI PEREIRA DA ROCHA` vs query `LINCOLN TEJOTA` → matches=1 (LINCOLN), classe 4.

Empate em classe; desempate por `-lucene_score`. Lucene dá score maior
ao MACIEL BARROS porque "BARROS"/"MACIEL" não estão na query mas o nome
inteiro está no índice — o nome `LINCOLN GRAZIANI...` perde por menos
hit-density relativa ao Lucene-IDF.

O StateLeg `LINCOLN TEJOTA` (que tem score 7.29 pra "TEJOTA") não chega
no resultado de "Lincoln Tejota" porque o `_run_search` unfiltered pega
20 items e o ranking Lucene pra a query de 2 tokens prioriza nomes com
ambos os tokens lexicais — nenhum tem.

## Direções de fix (escolher uma)

### Opção A — alias indexado no fulltext

Em `etl/.../entity_resolution_politicos_go.py` (ou um pós-processo após
o batch ER): copiar o `name` do StateLeg pra um campo `aliases` (list)
no `:Person` ligado via REPRESENTS. Mexer no schema do índice
`entity_search` pra cobrir `aliases`. Reindex.

- **Prós**: search retorna o Person diretamente com score alto pra qualquer
  alias; cobre apelidos parlamentares.
- **Contras**: precisa migration do índice + repopulação contínua.

### Opção B — cluster expansion no ranker

No `_local_relevance`, se o row tem `canonical_id`, fazer uma 2ª query
pra buscar os nomes de todos os siblings (`(c)-[:REPRESENTS]->(s)` →
`s.name`) e tomar o melhor score entre `name` e cada `sibling.name`.

- **Prós**: zero migration; só código no router.
- **Contras**: 1 round-trip extra por query (pode usar UNWIND com lista
  de canonical_ids únicos pra reduzir); só ajuda quando o sibling caiu
  no `final_results`.

### Opção C — alias-first no `_run_search`

Mudar a query do search pra fazer cluster expansion via REPRESENTS e
usar `apoc.text.fuzzyMatch` ou `db.index.fulltext.queryNodes` em ambos.

## Recomendação

Começar pela **B** — toca só `pwa_parity.py`, é reversível, e o caso
Lincoln Tejota é o único reportado. Subir pra **A** se aparecerem mais
deputados com apelido divergente (DR/PASTOR/DELEGADO etc.).

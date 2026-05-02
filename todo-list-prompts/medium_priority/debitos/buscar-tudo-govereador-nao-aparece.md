# `/buscar-tudo` não retorna `:GoVereador` mesmo após ingest

## Estado (2026-05-02)

Após ingest da Fase 2a do `camara_goiania` (commit `9d9a8d0`), o
Neo4j local tem 41 `:GoVereador` da 20ª Legislatura. O fulltext
`entity_search` **inclui** o label e indexa corretamente:

```
$ docker exec fiscal-neo4j cypher-shell -u neo4j -p changeme \
    "CALL db.index.fulltext.queryNodes('entity_search', 'BESSA') \
     YIELD node, score WHERE 'GoVereador' IN labels(node) \
     RETURN node.name, score LIMIT 3"
node.name, score
"BESSA", 5.023343086242676
```

Score 5.02 é maior que qualquer Person retornada pra `BESSA` (faixa
3.4–4.4). Apesar disso, `/buscar-tudo?q=BESSA` retorna **só** `:Person`
nos top 5 (929 resultados totais; `BESSA` o vereador não aparece em
nenhuma página).

```
$ curl -s "http://localhost:8000/buscar-tudo?q=BESSA"
total: 929
  person   GYSELLE BESSA       score=4.37
  person   ALESSANDRO BESSA    score=4.37
  person   RAPHAEL BESSA GRATAO score=3.87
  ...
```

## Hipóteses

1. **`search.cypher` ranqueia diferente** — a query
   `api/src/bracc/queries/search.cypher` pode somar/multiplicar
   score por algum signal post-fulltext que penaliza GoVereador (ex.:
   priorizar `:Person` com canonical_id resolvido, e GoVereador
   ainda não tem `REPRESENTS` pra `:CanonicalPerson`).
2. **Dedup por canonical_id colapsa GoVereador em Person** —
   `head(collect(DISTINCT cp.canonical_id))` pode deduplicar contra
   um Person homônimo perdendo o GoVereador.
3. **Lucene query expansion** — `_to_lucene_query` no
   `pwa_parity.py` pode adicionar boost por label que privilegia
   Person/Partner (procurar `Person^` ou similar no construtor da
   query Lucene).
4. **Fase 3 do dedup busca PWA** — recém-implementada
   (`project_dedup_busca_pwa.md`) colapsa rows por `name+UF`. Pode
   estar fazendo merge errado entre Person e GoVereador quando o
   nome é homônimo.

## O que verificar (em ordem)

```bash
# 1. Bater na query search.cypher direto pra ver se devolve GoVereador:
docker exec fiscal-neo4j cypher-shell -u neo4j -p changeme \
  -P "query=>'BESSA'" -P "hide_person_entities=>false" \
  -P "entity_type=>null" -P "skip=>0" -P "limit=>20" \
  -f api/src/bracc/queries/search.cypher
```

Se `BESSA :GoVereador` aparece → bug está pós-Cypher (no transform
do `pwa_parity._to_buscar_tudo_item` ou no dedup fase 3).

Se NÃO aparece → bug está no Cypher (provavel: dedup por
`canonical_id` ou ranking score).

## Fix candidato

- Se for ranking: garantir que tipos prioritários (Senator,
  FederalLegislator, StateLegislator, GoVereador — todos cargos
  eletivos) recebem boost positivo no score, não só pelo Lucene
  bruto.
- Se for dedup canonical_id: GoVereador hoje **não tem**
  `REPRESENTS` pra `:CanonicalPerson` (pipeline 2a não cria a
  relação). Quando criar, vai casar com Person via
  `entity_resolution_politicos_go` — ajustar a fase 3 do dedup
  busca pra preferir o cargo eletivo (GoVereador) sobre o Person
  legacy quando ambos casam pelo mesmo canonical.

## Critério de aceite

- `/buscar-tudo?q=BESSA` retorna `BESSA` (vereador, Mobiliza, gab.11)
  no top 5.
- `/buscar-tudo?q=AAVA` retorna `AAVA SANTIAGO` (PSB, gab.19) no
  top 3 (nome único; deveria ser top 1).
- Spot-check com 5 dos 41 vereadores ingeridos confirma cada um
  aparece na busca.

## Relacionados

- `project_dedup_busca_pwa.md` — fase 3 do dedup pode estar
  envolvida.
- `entity_resolution_politicos_go.md` — eventualmente GoVereador
  precisa entrar no ER pra casar com `:Person` TSE quando o
  vereador aparecer como candidato eleito (Maju 2024 etc.).

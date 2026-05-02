# Entity resolution `:GoVereador` ↔ `:Person` TSE — vereadores eleitos 2024 sem REPRESENTS

## Contexto

Fase 2a do `camara_goiania` (commit `9d9a8d0`, 2026-05-02) ingeriu
41 `:GoVereador` da 20ª Legislatura no Neo4j local. **Nenhum** tem
`(:GoVereador)-[:REPRESENTS]->(:CanonicalPerson)` ou ligação direta
ao `:Person` TSE correspondente.

Os 41 vereadores foram eleitos em **2024** pra Goiânia (município
5208707), então **todos** têm registro como `:Person` no TSE local
(ingerido pelo `tse_2024` em 2026-04-22 — `project_vereadores_nao_ingeridos.md`).
Sem o link, o perfil do vereador no PWA fica sem:

- Doações recebidas (`:DOOU` do TSE 2024).
- Bens declarados (`:DeclaredAsset` do `tse_bens`).
- Histórico eleitoral (eleições anteriores, candidaturas).
- Sanctions/conflicts cruzados via Person.

## Como o ER existente funciona hoje

`etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py`
(documentado em `MEMORY.md` como "Branch D + fase 4.5
name_partido_multi") liga:

- `:Senator` ↔ `:Person` (CPF / nome+ano)
- `:FederalLegislator` ↔ `:Person` (idem)
- `:StateLegislator` ↔ `:Person` (idem; cobre o caso "DRA. X" sem
  CPF via REPRESENTS direção c→p — `project_entity_resolution_nome_gabinete.md`)

`:GoVereador` **não** está na lista — pipeline ER ignora.

## Por que adicionar

1. **CPF** — não disponível no portal CMG (mesmo padrão Senado pós-2024,
   `reference_senado_sem_cpf_publico.md`). Tier 1 do ER (CPF-based)
   nunca ativa.
2. **Nome+UF+ano** — Tier 2. Pra um vereador eleito em 2024 em GO,
   `:Person` TSE casa por nome normalizado + `uf=GO` +
   `cargo=VEREADOR` + `eleicao_ano=2024`. Risco: homônimos. Mitigação:
   filtrar `:Person` que tem `:CANDIDATO_A` apontando pra `municipality_code='5208707'`.
3. **Nome+partido** — Tier 3 (fase 4.5 já implementada pros estaduais).
   `:GoVereador` tem `party` rico ("Partido Socialista Brasileiro (PSB)")
   — extrair sigla e bater contra `:Person.partido` quando empatar Tier 2.

## Schema do `:REPRESENTS`

Manter convenção dos outros pipelines:

```cypher
MATCH (v:GoVereador {vereador_id: $vid})
MATCH (p:Person {person_id: $pid})
MERGE (v)-[r:REPRESENTS]->(p)
SET r.tier = $tier,                  // 'name_uf_ano' | 'name_partido' | 'manual'
    r.confidence = $confidence,      // 0.0 - 1.0
    r.matched_at = datetime(),
    r.source_id = 'entity_resolution_politicos_go'
```

Considerar também o caminho inverso `(:Person)-[:CANDIDATO]->(:GoVereador)`
se o ER de outras casas usa essa direção pra `:CanonicalPerson`
(verificar contrato da fase 4.5).

## Trabalho

1. **Rodar audit**: contar quantos dos 41 GoVereador casam Tier 2
   (nome+UF+município+ano). Esperado: ~30-35 (alguns suplentes podem
   ter nome diferente do registrado no TSE; outros podem ter homônimo
   ambíguo).

   ```cypher
   MATCH (v:GoVereador)
   OPTIONAL MATCH (p:Person)
   WHERE p.name = v.name
     AND p.uf = 'GO'
     AND p.eleicao_ano = '2024'
     AND p.cargo CONTAINS 'VEREADOR'
   RETURN v.name, v.party, count(p) AS candidatos_tse
   ```

2. **Estender pipeline ER**: adicionar `_resolve_govereadores()` em
   `entity_resolution_politicos_go.py` seguindo o padrão dos
   `_resolve_state_legislators()` / `_resolve_senators()`.

3. **Tests**: replicar `test_entity_resolution_politicos_go.py` com
   fixtures pra GoVereador (1 caso unique, 1 homônimo resolvido por
   partido, 1 sem match).

4. **Re-rodar ER no Neo4j local** + smoke check no PWA: perfil de
   um vereador eleito (ex.: BESSA) deve mostrar TSE 2024 (doadores,
   bens) + bio CMG simultâneo.

## Critério de aceite

- ≥80% dos 41 GoVereador têm `:REPRESENTS` resolvido após ER.
- 0 falsos positivos em spotcheck manual de 10 amostras (homônimos
  resolvidos corretamente).
- Perfil do vereador no PWA mostra dados TSE (doadores) + dados
  CMG (gabinete, foto, bio) na mesma página.

## Relacionados

- `project_orphan_person_siblings_perfil.md` — caso similar resolvido
  pra StateLegislator/FederalLegislator/Senator.
- `project_entity_resolution_nome_gabinete.md` — variação do ER
  pra cargos sem CPF público.
- `project_dedup_busca_pwa.md` — fase 3 do dedup busca pode usar
  o `canonical_id` deste ER pra colapsar GoVereador + Person no
  /buscar-tudo (ver também `buscar-tudo-govereador-nao-aparece.md`).

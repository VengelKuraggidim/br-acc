# Migração APOC do `source_id` no Aura prod (pré-deploy)

## Contexto

Commit `d23baee` (2026-04-19) fez a migração do `source_id` dos pipelines
`transparencia` e `tse` pra bater 1:1 com `docs/source_registry_br_v1.csv`:

| Antes (legado)                 | Depois (canônico) |
|--------------------------------|-------------------|
| `portal_transparencia`         | `transparencia`   |
| `tribunal_superior_eleitoral`  | `tse`             |

No mesmo commit foi removido o remendo `_GRAPH_TO_REGISTRY_ALIAS` em
`api/src/bracc/services/sources_public_service.py`. A API agora assume
que grafo e registry usam o mesmo slug.

No Neo4j **local** foi rodada a migração retrospectiva via
`apoc.periodic.iterate` (batchSize 5000): ~453k nodes + rels tiveram
`source_id` e `run_id` reescritos. Zero falhas.

O **Aura prod** (Google Drive do Fernando) ainda tem os slugs antigos.
Fazer deploy do backend sem migrar o Aura primeiro **quebra** a aba
Fontes (o badge `com_dados` vai zerar pra `transparencia` e `tse`,
porque queries vão filtrar pelos slugs novos mas IngestionRun antigos
ainda carregam os legados).

## O que rodar no Aura prod

Executar **antes** do primeiro deploy do backend que carrega o
`sources_public_service.py` sem o alias.

### 1. Validar impacto (dry-run)

```cypher
// Quantos IngestionRun vão mudar?
MATCH (r:IngestionRun)
WHERE r.source_id IN ['portal_transparencia', 'tribunal_superior_eleitoral']
RETURN r.source_id, count(r) AS runs;

// Quantos nodes "carimbados" com os slugs legados?
MATCH (n)
WHERE n.source_id IN ['portal_transparencia', 'tribunal_superior_eleitoral']
RETURN n.source_id, count(n) AS nodes;

// Quantos relationships "carimbados"?
MATCH ()-[r]->()
WHERE r.source_id IN ['portal_transparencia', 'tribunal_superior_eleitoral']
RETURN r.source_id, count(r) AS rels;
```

No local a contagem foi ~77k nodes + ~60k rels pra `portal_transparencia`
e ~172k nodes + ~141k rels pra `tribunal_superior_eleitoral`. No prod
pode ser maior — confirmar antes.

### 2. Executar migração em batches

Aura tem `apoc.periodic.iterate` por default (plano standard+).

```cypher
// Migração 1/6: IngestionRun portal_transparencia → transparencia
CALL apoc.periodic.iterate(
  "MATCH (r:IngestionRun) WHERE r.source_id = 'portal_transparencia' RETURN r",
  "SET r.source_id = 'transparencia',
       r.run_id = replace(r.run_id, 'portal_transparencia_', 'transparencia_')",
  {batchSize: 5000, parallel: false}
) YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;

// Migração 2/6: nodes portal_transparencia → transparencia
CALL apoc.periodic.iterate(
  "MATCH (n) WHERE n.source_id = 'portal_transparencia' RETURN n",
  "SET n.source_id = 'transparencia',
       n.run_id = replace(n.run_id, 'portal_transparencia_', 'transparencia_')",
  {batchSize: 5000, parallel: false}
) YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;

// Migração 3/6: rels portal_transparencia → transparencia
CALL apoc.periodic.iterate(
  "MATCH ()-[r]->() WHERE r.source_id = 'portal_transparencia' RETURN r",
  "SET r.source_id = 'transparencia',
       r.run_id = replace(r.run_id, 'portal_transparencia_', 'transparencia_')",
  {batchSize: 5000, parallel: false}
) YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;

// Migração 4/6: IngestionRun tribunal_superior_eleitoral → tse
CALL apoc.periodic.iterate(
  "MATCH (r:IngestionRun) WHERE r.source_id = 'tribunal_superior_eleitoral' RETURN r",
  "SET r.source_id = 'tse',
       r.run_id = replace(r.run_id, 'tribunal_superior_eleitoral_', 'tse_')",
  {batchSize: 5000, parallel: false}
) YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;

// Migração 5/6: nodes tribunal_superior_eleitoral → tse
CALL apoc.periodic.iterate(
  "MATCH (n) WHERE n.source_id = 'tribunal_superior_eleitoral' RETURN n",
  "SET n.source_id = 'tse',
       n.run_id = replace(n.run_id, 'tribunal_superior_eleitoral_', 'tse_')",
  {batchSize: 5000, parallel: false}
) YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;

// Migração 6/6: rels tribunal_superior_eleitoral → tse
CALL apoc.periodic.iterate(
  "MATCH ()-[r]->() WHERE r.source_id = 'tribunal_superior_eleitoral' RETURN r",
  "SET r.source_id = 'tse',
       r.run_id = replace(r.run_id, 'tribunal_superior_eleitoral_', 'tse_')",
  {batchSize: 5000, parallel: false}
) YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;
```

### 3. Validar pós-migração

```cypher
// Zero residual dos slugs legados?
MATCH (r:IngestionRun) WHERE r.source_id IN ['portal_transparencia','tribunal_superior_eleitoral'] RETURN count(r) AS residual_runs;
MATCH (n) WHERE n.source_id IN ['portal_transparencia','tribunal_superior_eleitoral'] RETURN count(n) AS residual_nodes;
MATCH ()-[r]->() WHERE r.source_id IN ['portal_transparencia','tribunal_superior_eleitoral'] RETURN count(r) AS residual_rels;

// Contadores canônicos têm os números esperados?
MATCH (r:IngestionRun) WHERE r.source_id IN ['transparencia','tse'] RETURN r.source_id, count(r);
```

Esperado: residuais = 0; canônicos = soma dos anteriores.

### 4. Deploy do backend

Depois da migração, fazer deploy normal do backend (Cloud Run). A API
passa a resolver o slug canônico contra o registry direto, sem alias.

## Rollback

Se der ruim no Aura (ex: batch trava, disk full): os dados antigos viram
inconsistentes com o registry. Reversão:

```cypher
// Reverter 1/6: transparencia → portal_transparencia
CALL apoc.periodic.iterate(
  "MATCH (r:IngestionRun) WHERE r.source_id = 'transparencia' AND r.run_id STARTS WITH 'transparencia_' RETURN r",
  "SET r.source_id = 'portal_transparencia',
       r.run_id = replace(r.run_id, 'transparencia_', 'portal_transparencia_')",
  {batchSize: 5000}
) YIELD batches, total, errorMessages RETURN batches, total, errorMessages;
// ... análogo pros outros 5 SETs
```

Depois reintroduzir o `_GRAPH_TO_REGISTRY_ALIAS` via hotfix no backend.
Idealmente rodar migração em janela de baixo tráfego pra não precisar disso.

## Critério de "pronto"

- No Aura prod: 0 residuais dos slugs legados
- Deploy do backend sem alias feito
- Aba Fontes da PWA volta a mostrar `com_dados` pra `transparencia` e `tse`
- Este arquivo deletado (débito resolvido)

## Quem executa

Operacional, não autônomo. Fernando roda quando for fazer deploy. Um
Claude Code numa sessão futura pode assistir, mas não tem credencial
Aura — o humano executa o cypher-shell contra a instância prod.

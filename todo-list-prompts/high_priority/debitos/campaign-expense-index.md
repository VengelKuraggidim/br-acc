# Índice faltando em `:CampaignExpense(expense_id)` trava load do TSE — ⏳ PENDENTE (2026-04-21)

> Descoberto em 2026-04-21 ao rodar `tse_prestacao_contas_go` contra Neo4j
> local. MERGE batch de 50k expenses ficou 12+ min numa única transação
> sem progresso e precisou ser morto. Causa raiz: `MATCH/MERGE (n:CampaignExpense
> {expense_id: ...})` faz full-scan sem índice.

## Contexto

Pipeline `tse_prestacao_contas_go` carrega:

* `:Person` (candidatos) — MERGE por `cpf` (índice existe).
* `:CampaignDonation` — MERGE por `donation_id` (verificar se tem índice).
* `:CampaignExpense` — MERGE por `expense_id` (**SEM ÍNDICE**).
* `:Company` — MERGE por `cnpj` (índice `company_cnpj_unique` existe).

Com ~113k expense nodes + ~113k expense_rels pra 2022 GO, cada batch de
50k vira um full-scan quadrático. Na prática o loader nunca finaliza.

**Sintoma real observado**:

```
2026-04-21 21:08:03 INFO [tse_prestacao_contas_go] Starting load...
2026-04-21 21:13:52 INFO   Batch written: 12015 rows (cumulative: 13186)
# ... nada por 20+ min
SHOW TRANSACTIONS mostra: neo4j-transaction-1675 PT12M39.537S
  "UNWIND $rows AS row MERGE (n:CampaignExpense {expense_id: row.expense_id}) SET n.ano = row.ano, ..."
```

Na primeira ingestão (grafo vazio) é menos visível porque MERGE só cria.
Na re-ingestão o match precisa bater no índice pra ser O(1); sem índice
vira O(n²) no tamanho do label.

## Missão

1. Adicionar em `api/src/bracc/queries/schema_init.cypher` (que é onde
   mora o setup dos índices):

   ```cypher
   CREATE INDEX campaign_expense_id IF NOT EXISTS
     FOR (n:CampaignExpense) ON (n.expense_id);
   ```

2. Varrer outras MERGEs de pipeline que podem ter o mesmo problema:
   - `:CampaignDonation(donation_id)` — `tse_prestacao_contas_go` e `tse`
   - `:CampaignDonor(doador_id)` — ver `tse_prestacao_contas_go` load()
   - `:LegislativeProposition` — `alego` usa qual chave?
   - `:StateLegislator(legislator_id)` — verificar
   Adicionar índices `IF NOT EXISTS` pra todos que fazem MERGE por chave
   estável sem índice.

3. Aplicar o schema no Neo4j local (`cypher-shell -f schema_init.cypher`)
   e re-rodar o pipeline `tse_prestacao_contas_go` pra confirmar que o
   load completa em tempo razoável (meta: < 5 min end-to-end pra GO 2022).

4. Docs: documentar o cuidado em `docs/archival.md` ou `docs/schema.md`
   (onde melhor caber) — toda nova label `:X` com MERGE por chave
   precisa de `CREATE INDEX ... IF NOT EXISTS` em `schema_init.cypher`.

## Arquivos relevantes

- `api/src/bracc/queries/schema_init.cypher` — origem dos índices prod.
- `etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py` — load() com
  os MERGEs afetados.
- `etl/src/bracc_etl/loader.py` — `Neo4jBatchLoader.load_nodes` e
  `run_query_with_retry`.

## Critérios de aceite

- [ ] Índice `campaign_expense_id` existe em prod (Aura) e local.
- [ ] `tse_prestacao_contas_go` roda end-to-end contra Neo4j local em
      < 5 min (hoje: timeout indefinido).
- [ ] Outras labels de ETL auditadas; índices faltantes adicionados.
- [ ] `make test-etl` + `make test-api` verdes.

## Prioridade

**Alta.** Sem isso o pipeline TSE não completa — bloqueia a validação
cross-run do todo 07 Fase 1 (committees só foram carregados via script
ad-hoc `/tmp/load_committees_only.py` porque o load do pipeline
completo ficou preso em expense_id).

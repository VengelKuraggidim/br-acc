# Backfill `ano` em todas as rels `:DOOU` — 🔧 PATCH PARCIAL (2026-04-22)

> Commit `7aca93b` ajusta `tse.py` pra `SET r.ano = row.year` nas duas
> queries de DOOU (Person→candidato e Company→candidato). MERGE key
> continua `{year: row.year}` pra preservar idempotencia — re-runs do
> tse bulk vao **atualizar r.ano em rels existentes sem duplicar**.
>
> Backfill em producao (Aura) ainda pendente: depende de rerun do
> pipeline tse OU de uma APOC query ad-hoc tipo
> `MATCH ()-[r:DOOU]->() WHERE r.ano IS NULL AND r.year IS NOT NULL SET r.ano = r.year`
> pra cobrir os 165k rels legacy sem reingerir.

## Original


## Contexto

Commit `f71052f` (2026-04-19) introduziu filtro `ano_doacao=2022` em
`api/src/bracc/services/perfil_service.py` → `classificar` pra evitar
double-count das rels `:DOOU` carimbadas pelo pipeline TSE
(`tse_prestacao_contas_go.py` linha 549) — sem o filtro, candidato com
3 eleições ingeridas (2014/2018/2022) somava `valor_total` × 3.

A versão **original** do filtro descartava também rels com `ano IS NULL`
("evita contaminar o agregado com legado"). Em prod isso zerou os
doadores PJ/PF de Marconi Perillo (e outros): das 702 rels `:DOOU`
inbound, só **9** tinham `ano=2022` carimbado — as outras 693 vinham
de pipelines não-TSE (Company/Person → Person) que não carimbam `ano`.

Hotfix imediato (commit a fazer agora, 2026-04-19): o filtro foi
afrouxado em `conexoes_service.py` pra **manter** rels sem `ano`. Agora
só rels com `ano` carimbado E ≠ 2022 são descartadas. Devolve doadores
legacy mas reintroduz risco subtil: se algum pipeline futuro carimbar
`:DOOU` numa eleição ≠ 2022 sem o `ano`, contamina o agregado.

## Distribuição atual no grafo local (Marconi Perillo, CPF
`035.538.218-09`)

| Source label | `ano` | rels |
|---|---|---|
| `CampaignDonor` | 2018 | 41 |
| `CampaignDonor` | 2022 | 9 |
| `Company` | NULL | 213 |
| `Person` | NULL | 439 |

Total :DOOU no grafo local: 165.762 — só 24.585 (14,8%) com `ano`.

## Objetivo

Carimbar `ano` em **todas** as rels `:DOOU` do grafo, em todos os
pipelines que criam essa rel. Depois disso o filtro pode voltar a ser
estrito (descartar `ano IS NULL`) sem risco de zerar doadores.

## Pipelines a auditar

Buscar quem cria `:DOOU` no ETL:

```bash
grep -rn "DOOU\|donations\|donation_rels" etl/src/bracc_etl/pipelines/
```

Suspeitos imediatos (rels sem `ano` em prod):
- Pipelines que criam `Company-[:DOOU]->Person` (213 rels do Marconi)
- Pipelines que criam `Person-[:DOOU]->Person` (439 rels)

Provavelmente são pipelines TSE *anteriores* a `704a5aa` (1ª versão
do `tse_prestacao_contas_go.py` que já carimbava `ano`), ou pipelines
de cross-check / entity resolution que preservam rels antigas.

## Plano

1. **Audit**: rodar Cypher pra agrupar rels `:DOOU` sem `ano` por
   `source_id` da rel (ou do node origem) → identificar qual pipeline
   gerou cada bloco.
   ```cypher
   MATCH (src)-[r:DOOU]->(tgt)
   WHERE r.ano IS NULL
   RETURN coalesce(r.source_id, 'sem_source') AS pipeline,
          labels(src)[0] AS src_label,
          labels(tgt)[0] AS tgt_label,
          count(*) AS n
   ORDER BY n DESC;
   ```

2. **Patch nos pipelines identificados**: adicionar `ano` na rel
   (mesmo que precise inferir do contexto — ex.: ano da eleição
   declarada no node de origem).

3. **Re-ingestão local**: rodar pipelines patched (cuidado com pipelines
   pesados — ver `rodar-pipelines-pesados.md`).

4. **Re-deploy do graph** pro Aura
   (`bash scripts/deploy/deploy_all.sh --graph`).

5. **Tightening do filtro**: voltar `conexoes_service.py` ao
   comportamento estrito — descartar `ano IS NULL` quando filtro
   ativo. Atualizar teste
   `test_ano_doacao_rel_sem_ano_e_mantida_quando_filtro_ativo` pra
   refletir o novo contrato.

6. **Validação cross-check TSE**: confirmar que `total_doacoes` bate
   com `total_tse_2022` em vários candidatos (não só Marconi).

## Não-objetivos

- Não tocar no card "Contas batem?" (depende da mesma agregação
  filtrada por ano — vai melhorar de graça depois do backfill).
- Não retroagir validação a 2014/2018 — o filtro hoje só compara 2022
  porque é o único ano que `validacao_tse` cobre.

## Referências

- Sintoma original em prod (zero doadores Marconi): conversa Claude
  Code 2026-04-19 + diagnóstico em local DB.
- `api/src/bracc/services/conexoes_service.py` — filtro relaxado.
- `api/src/bracc/services/perfil_service.py:531` — chamada com
  `ano_doacao=2022`.
- `etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py:549` —
  pipeline modelo (carimba `ano`).
- Débito relacionado:
  `todo-list-prompts/high_priority/debitos/investigar-duplicacao-doacoes-tse.md`
  (hipótese 1, já confirmada pelo `f71052f`).

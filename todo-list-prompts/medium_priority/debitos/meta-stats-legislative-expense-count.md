# Observabilidade: `legislative_expense_count` em meta_stats.cypher

## Problema

`api/src/bracc/queries/meta_stats.cypher:56-59` conta `MATCH (e:Expense)`
como `expense_count`, mas o label real usado pelos pipelines da Câmara,
Senado e ALEGO é `:LegislativeExpense`. Resultado: o endpoint
`/api/v1/meta/stats` reporta `expense_count: 0` mesmo com dezenas de
milhares de despesas ingeridas.

Verificado em 2026-04-22 no Aura prod: `expense_count: 0` no stats
endpoint, mas `MATCH (e:LegislativeExpense) RETURN count(e)` devolve
**12.080** (pipeline `camara_politicos_go`, source `camara_deputados_ceap`).

## Origem

Identificado durante a resolução do débito `repopular-ceap-aura.md`
(2026-04-22). A própria doc do débito original alertava que o stats
endpoint seria "ruído, não sinal" pra validação do pipeline de CEAP.

## Fix proposto

Adicionar uma query extra em `meta_stats.cypher` retornando
`legislative_expense_count`:

```cypher
MATCH (le:LegislativeExpense) RETURN count(le) AS legislative_expense_count
```

E expor no schema do endpoint (`MetaStatsResponse` em
`api/src/bracc/models/meta.py`).

Opcionalmente, deprecate ou manter `expense_count` (que conta `:Expense`
genérico) — há algum pipeline que usa esse label puro? Rápida busca:
`grep -rn "':Expense'" api/ etl/src/bracc_etl/` — se não houver, pode
remover.

## Prioridade

Baixa. Não bloqueia UX — é só observabilidade. Qualquer futuro operador
validando o estado do grafo por meio de `/meta/stats` vai ser enganado
até esse fix cair.

## Cuidados

- `bracc-api` é built image (ver memória `project_ceap_federal_ingerido`).
  Mudança no Cypher + modelo exige `docker compose build bracc-api`
  local, e redeploy em prod (`gcloud run deploy`).
- Conferir se há frontend lendo `expense_count` (provavelmente não —
  PWA lê só sources com nome específico).

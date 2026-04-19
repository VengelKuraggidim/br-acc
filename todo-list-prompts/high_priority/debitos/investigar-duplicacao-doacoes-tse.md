# Investigar causa-raiz: total_doacoes ≠ total_tse_2022

## Contexto

Sessão 2026-04-19. Um candidato tinha:

- `total_tse_2022` (prop no Person): R$ 4,66 mi
- `total_doacoes` (soma de `valor_total` por doador retornado ao /politico):
  R$ 14,05 mi
- Divergência: +201,6%

As duas métricas **deveriam bater** porque vêm do mesmo CSV
(`receitas_candidatos_2022.csv`), carregado em
`etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py:443-468`:

- `total_tse_2022` = `sum(VR_RECEITA) agrupado por candidato` (linha 468)
- Rels `:DOOU` individuais = `row-a-row do mesmo CSV` (linhas 496-540)

Quando a API agrega `total_doacoes` em
`api/src/bracc/services/perfil_service.py:616-618`:

```python
total_doacoes = sum(d.valor_total for d in resultado.doadores_empresa) + sum(
    d.valor_total for d in resultado.doadores_pessoa
)
```

... ela soma os `valor_total` que vieram das queries de doadores
(`api/src/bracc/queries/doadores_*.cypher` — confirmar).

## Hipóteses a investigar

Em ordem decrescente de probabilidade:

1. **Query de doadores não filtra por ano** — se `:DOOU` existe para
   2014/2018/2022 e a query agrega todos os anos em `valor_total`,
   `total_doacoes` passa a somar 3 eleições. `total_tse_2022` é só 2022.
   **Checar**: `api/src/bracc/queries/doadores_empresa.cypher` e
   `doadores_pessoa.cypher` têm filtro `WHERE r.ano = 2022`?

2. **MERGE duplicando :DOOU** — `donation_id` em
   `tse_prestacao_contas_go.py:518` é computado como
   `_donation_id(sq, year, doador_id or "anon", f"{valor:.2f}", idx)`.
   Se reruns usam `idx` recomputado (não estável entre runs), cada rerun
   cria rel nova ao invés de fazer MERGE. **Checar**: rodar query
   diagnóstica no grafo:

   ```cypher
   MATCH (p:Person)<-[r:DOOU]-(d)
   WHERE p.total_tse_2022 > 0
   RETURN p.nome, count(r), sum(r.valor), p.total_tse_2022
   LIMIT 10
   ```

   Se `sum(r.valor) > total_tse_2022` por múltiplos de `n_reruns`, é MERGE
   quebrado.

3. **Outro pipeline criando :DOOU** — algum outro pipeline (TSE bens?
   camara_politicos_go? tse de outros anos?) cria rels `:DOOU` no mesmo
   Person sem ano filtrado, adicionando ao sum. **Checar**:
   `grep -r "DOOU" etl/src/bracc_etl/pipelines/`.

4. **CampaignDonation vs :DOOU direto** — pipeline cria node
   `:CampaignDonation` (linha 519) além de ou em vez da rel `:DOOU`.
   Se queries de doadores pegam os dois caminhos, dobra.

## Plano

1. Escrever query diagnóstica Cypher que conta rels `:DOOU` por
   `(candidato, ano)` e compara com `total_tse_{ano}` — identifica qual
   hipótese bate.
2. Rodar contra Aura prod (read-only) via notebook one-off, não
   committar resultados (só o script).
3. Baseado no resultado:
   - Se hipótese 1: adicionar `WHERE r.ano = 2022` nas queries de doadores.
   - Se hipótese 2: refatorar `_donation_id` pra ser estável entre reruns
     (hash estável de `(sq, year, doador_id, valor, origem_receita, dt)`
     sem depender de `idx`).
   - Se hipótese 3: filtrar por source_id ou ano no Cypher do endpoint.
   - Se hipótese 4: escolher uma das duas representações.

## Referências

- `etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py:443-540`
- `api/src/bracc/services/perfil_service.py:616-618`
- `api/src/bracc/queries/doadores_empresa.cypher` (conferir)
- `api/src/bracc/queries/doadores_pessoa.cypher` (conferir)

## Prioridade

**Alta** — é a causa-raiz do débito
`fix-validacao-tse-divergencia-direcao.md`. Sem isso, a mensagem nova
fica dizendo "provavelmente duplicação" sem resolver a duplicação.

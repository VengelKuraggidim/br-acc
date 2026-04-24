# Tightening do filtro `ano_doacao` em `conexoes_service.py` — ✅ RESOLVIDO 2026-04-24

> Filtro restrito em `api/src/bracc/services/conexoes_service.py:447`: agora
> `if rel_ano != ano_doacao: continue` — rels sem `ano` carimbada são
> descartadas quando o filtro está ativo. Teste renomeado para
> `test_ano_doacao_rel_sem_ano_e_descartada_quando_filtro_ativo` reflete o
> novo contrato. Comentário no service atualizado pra apontar o backfill de
> 2026-04-22 + os dois pipelines ativos que carimbam `r.ano`. Sem regressão:
> `cd api && uv run pytest` → 863 passed. Sem rebuild/redeploy feito — fica
> pro próximo push regular da API.

## Original

## Estado atual (2026-04-22)

Backfill das rels `:DOOU` já aplicado no Aura prod: 100% das 46.449
rels têm `r.ano` preenchido (34.164 via `SET r.ano = r.year` dos legados
TSE). Zero rels com `r.ano IS NULL`.

Os dois pipelines ativos que criam `:DOOU` (`tse.py` e
`tse_prestacao_contas_go.py`) já carimbam `r.ano` — nenhum pipeline
atualmente produz rels sem ano.

## Fix opcional

`api/src/bracc/services/conexoes_service.py:439-448` implementa o filtro
de forma permissiva (rels sem `ano` passam pra não zerar doadores
legacy). Agora que a invariante "toda :DOOU tem r.ano" é real, o filtro
pode ser restringido pra descartar também rels com `ano IS NULL` — isso
deixa o contrato explícito e falha fast se algum pipeline futuro criar
rels sem carimbar o ano.

Diff estimado:

```python
if ano_doacao is not None:
    rel_ano_raw = rel_props.get("ano")
    try:
        rel_ano = int(rel_ano_raw) if rel_ano_raw is not None else None
    except (TypeError, ValueError):
        rel_ano = None
-   if rel_ano is not None and rel_ano != ano_doacao:
+   if rel_ano != ano_doacao:
        continue
```

Teste associado:
`api/tests/services/test_conexoes_service.py::test_ano_doacao_rel_sem_ano_e_mantida_quando_filtro_ativo`
precisa ser reescrito pra refletir o novo contrato (rels sem `ano` são
descartadas).

## Por que não foi feito junto com o backfill

- Requer rebuild + redeploy da `bracc-api` (built image, memo
  `project_ceap_federal_ingerido` item 1).
- Sem benefício imediato pro usuário final — o filtro relaxado atual
  está produzindo os mesmos resultados agora que 0 rels têm `ano=NULL`.
- Melhor fazer junto com o próximo deploy regular da API do que abrir
  uma janela dedicada.

## Prioridade

Baixa. Higienização de contrato; não bloqueia UX.

# Investigar e corrigir pipelines TSE (`tse`, `tse_bens`, `tse_filiados`)

## Contexto do projeto

Ver `README.md` desta pasta e `CLAUDE.md` na raiz. Fiscal Cidadão = fork
GO-focused de `brunoclz/br-acc`. Base eleitoral TSE é **coração do
projeto** — é dela que vêm candidatos, doações, filiações partidárias,
bens declarados.

## Evidência do problema

Query:
```
MATCH (r:IngestionRun)
WHERE r.source_id IN ['tse', 'tribunal_superior_eleitoral', 'tse_bens', 'tse_filiados']
RETURN r.source_id, r.status, r.rows_in, r.rows_loaded, r.started_at
ORDER BY r.started_at DESC
```

Resultado 2026-04-19:
- `tribunal_superior_eleitoral`, `loaded`, 0/0, 2026-04-17T23:44:30Z
- `tse_bens`, `loaded`, 0/0, 2026-04-17T23:45:15Z
- `tse_filiados`, `loaded`, 0/0 (3 runs recentes, todas zero)

No disco:
- `data/tse/` — 2.2 GB
- `data/tse_bens/` — 57 MB
- `data/tse_filiados/` — 58 MB

Apesar disso, `/stats` do grafo mostra:
- `election_count: 54` (algo do TSE entrou em algum momento)
- `declared_asset_count: 18225` (tse_bens carregou em algum momento)
- `party_membership_count: 674714` (tse_filiados carregou em algum momento)

Ou seja: já houve carga histórica, mas re-runs recentes falham. Ou o
pipeline criou runs novos sem carregar (re-execução no-op?).

## Hipóteses

1. **Source_id mismatch**: pipeline `tse` escreve `IngestionRun.source_id =
   'tribunal_superior_eleitoral'` em vez de `tse`. Registry espera `tse`.
   Confirmado. Ver `06-fix-source-id-alias.md` — coordenar.
2. **Idempotência mal implementada**: pipeline detecta "já carregado" e
   pula sem carregar nada novo, mas não diferencia "já existe + dado
   igual" de "já existe + dado novo". Rows_in=0 porque nem leu.
3. **Schema do ZIP mudou**: TSE às vezes reorganiza nomes de arquivos
   dentro dos ZIPs (`consulta_cand_2024_BR.csv` vs `consulta_cand_2024.csv`),
   parser não acha.
4. **Filtro UF=GO**: `tse` ingere candidaturas nacionais mas filtra GO.
   Se mapping da coluna UF mudou, pipeline filtra todo mundo fora.

## Missão

1. **Ler os 3 pipelines**:
   - `etl/src/bracc_etl/pipelines/tse.py`
   - `etl/src/bracc_etl/pipelines/tse_bens.py`
   - `etl/src/bracc_etl/pipelines/tse_filiados.py`

2. **Confirmar source_id**: cada pipeline define `self.source_id = ?`.
   Se for `tribunal_superior_eleitoral` em vez de `tse`, é bug.

3. **Testar cada um isoladamente**:
   ```bash
   cd /home/alladrian/PycharmProjects/br-acc/etl
   NEO4J_PASSWORD="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
   uv run python -m bracc_etl.runner run --source tse_bens --data-dir ../data 2>&1 | tee /tmp/tse_bens.log
   ```
   Observar logs: quantas linhas leu? Filtrou quantas? Carregou quantas?

4. **Corrigir o que está quebrado**. Foco em ordem:
   1. `source_id` correto (coordenar com prompt 06)
   2. Extract lê os arquivos certos (ajustar paths/nomes se mudaram)
   3. Filtro UF correto (o dado nacional entra — só candidato de GO vira
      `:Candidate` linkado a políticos GO; resto pode virar `:Person`
      sem label GO mesmo — confirmar com código existente)

5. **Rerodar** cada um. Observar:
   - `IngestionRun.rows_loaded > 0`
   - Counts no grafo aumentaram vs baseline

6. **Tests**: `etl/tests/test_tse*.py` — garantir que passam após mudanças.

7. **Commits atômicos**, um por pipeline. Mensagens:
   - `fix(etl): tse — source_id canonico + extract fix`
   - `fix(etl): tse_bens — parser adaptado a novo layout`
   - `fix(etl): tse_filiados — idempotencia corrigida`

## Critério de "pronto"

Cada um dos 3 pipelines termina com `IngestionRun.rows_loaded > 0` na
run mais recente. Badge na aba Fontes vira `com_dados`.

## Se travar

- Se arquivo esperado pelo pipeline não existe em `data/tse*/` →
  redownload via `scripts/download_tse*.py`
- Se TSE mudou layout fundamental (ex: migrou pra API) → parar, relatar
  como débito de alto valor

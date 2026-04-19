# Investigar e corrigir pipelines CVM (`cvm`, `cvm_funds`)

## Contexto

Ver `README.md` da pasta + `CLAUDE.md`. CVM (Comissão de Valores
Mobiliários) é relevante pro projeto porque mapeia processos de
empresas de capital aberto e fundos de investimento — políticos GO
podem ser cotistas ou sócios em cadeias controladoras listadas aí.

## Evidência do problema

```
MATCH (r:IngestionRun) WHERE r.source_id IN ['cvm', 'cvm_funds']
RETURN r.source_id, r.status, r.rows_in, r.rows_loaded, r.started_at
```

Resultado 2026-04-19:
- `cvm`, `loaded`, 0/0, 2026-04-18T00:04:45Z
- `cvm_funds`, `loaded`, 0/0, 2026-04-18T00:04:57Z

Disco:
- `data/cvm/` — 656 KB (pequeno)
- `data/cvm_funds/` — 18 MB

Mas `/stats` mostra:
- `cvm_proceeding_count: 537`
- `fund_count: 41107`

Ou seja, CVM **já carregou em algum passado**, mas re-runs recentes
rodam com zero linhas. Pode ser idempotência ou path quebrado.

## Hipóteses

1. **CVM publica datasets com data no nome**: `inv_nr_irprofiss_YYYY.csv`,
   `registro_fundo.csv`. Se pipeline espera nome antigo mas download
   trouxe nome novo → extract acha zero arquivos.
2. **API CVM mudou endpoint**: CVM às vezes muda schema de dados abertos.
3. **Idempotency skip**: pipeline detecta nodes já existentes e pula leitura.

## Missão

1. **Ler pipelines**:
   - `etl/src/bracc_etl/pipelines/cvm.py`
   - `etl/src/bracc_etl/pipelines/cvm_funds.py`

2. **Listar o que tem em `data/cvm*/`**:
   ```bash
   find /home/alladrian/PycharmProjects/br-acc/data/cvm* -type f | head -20
   ```

3. **Rodar cada um e observar**:
   ```bash
   cd /home/alladrian/PycharmProjects/br-acc/etl
   NEO4J_PASSWORD="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
   uv run python -m bracc_etl.runner run --source cvm --data-dir ../data 2>&1 | tee /tmp/cvm.log
   ```

4. **Corrigir**:
   - Se path de arquivo mudou, ajustar
   - Se schema mudou, adaptar parser
   - Se idempotency é overzealous, soltar pra relê quando rodar
     manualmente (modo `--force` se existir, ou `UPSERT` vs `CREATE IF NOT EXISTS`)

5. **Rerodar** + verificar `rows_loaded > 0` + counts aumentaram no
   `/stats`.

6. **Tests**: `etl/tests/test_cvm*.py`.

7. **Commits** atômicos:
   - `fix(etl): cvm — parser de processos sancionadores atualizado`
   - `fix(etl): cvm_funds — leitor do cadastro de fundos corrigido`

## Critério de "pronto"

`IngestionRun.rows_loaded > 0` em runs recentes de `cvm` e `cvm_funds`.
Badge vira `com_dados`.

## Se travar

- CVM mudou totalmente o modelo de dados abertos → relatar como débito,
  não forçar.
- Pipelines têm blocker documentado em comentário do código → respeitar
  e seguir.

# Investigar e corrigir `pncp_go` e `camara_goiania`

## Contexto

Ver `README.md` da pasta + `CLAUDE.md`. Dois pipelines GO-específicos
que deveriam ter alto volume (compras públicas estaduais/municipais de
GO + vereadores da capital) mas estão com zero rows.

## Evidência do problema

```
MATCH (r:IngestionRun) WHERE r.source_id IN ['pncp_go', 'camara_goiania']
RETURN r.source_id, r.status, r.rows_in, r.rows_loaded, r.started_at
ORDER BY r.started_at DESC
```

Resultado 2026-04-19:
- `pncp_go`, `loaded`, 0/0, 2026-04-17T23:41:16Z (2 runs, ambas zero)
- `camara_goiania`, `loaded`, 0/0, 2026-04-17T22:08:00Z

Disco:
- `data/pncp_go/` — 5.9 MB
- `camara_goiania` — sem diretório próprio em `data/` (scraping HTML
  direto, não persiste arquivo)

`/stats`:
- `municipal_bid_count: 0`, `municipal_contract_count: 0` (pncp_go deveria alimentar)
- Nenhum count direto de vereadores/gastos de Goiânia no /stats

## Hipóteses

### pncp_go
1. **API PNCP**: usa `https://pncp.gov.br/api/consulta/v1/`. Paginação
   ou parâmetro `uf=GO` pode ter mudado.
2. **Extract OK mas transform zerando**: arquivos em `data/pncp_go/`
   existem (5.9 MB) mas transform não reconhece o formato.
3. **Filtro de data**: pipeline só busca licitações do mês corrente e
   filtra tudo fora.

### camara_goiania
1. **HTML scraping frágil**: `https://www.goiania.go.leg.br/` muda
   layout com frequência. CSS selectors quebram.
2. **Anti-bot / CAPTCHA**: site da Câmara pode ter proteção que derruba
   request automatizada.
3. **Fonte não tem API** — scraping é único caminho.

## Missão

1. **Ler pipelines**:
   - `etl/src/bracc_etl/pipelines/pncp_go.py`
   - `etl/src/bracc_etl/pipelines/camara_goiania.py`

2. **pncp_go — listar arquivos em disco**:
   ```bash
   ls -la /home/alladrian/PycharmProjects/br-acc/data/pncp_go/
   head -2 /home/alladrian/PycharmProjects/br-acc/data/pncp_go/*.json 2>/dev/null
   ```

3. **camara_goiania — probar URL atual**:
   ```bash
   curl -sI https://www.goiania.go.leg.br/ | head -5
   curl -s https://www.goiania.go.leg.br/ | head -40
   ```
   Ver se o site responde e se HTML bate com o que o scraper espera.

4. **Rodar cada um**:
   ```bash
   cd /home/alladrian/PycharmProjects/br-acc/etl
   NEO4J_PASSWORD="$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
   uv run python -m bracc_etl.runner run --source pncp_go --data-dir ../data 2>&1 | tee /tmp/pncp_go.log
   ```

5. **Corrigir**:
   - `pncp_go`: ajustar parâmetros da API (paginação, filtro UF, range
     de data). Confirmar endpoint ativo via curl antes.
   - `camara_goiania`: se site mudou HTML, atualizar seletor. Se
     scraping ficou inviável, parar e relatar como débito de fonte (não
     tentar workarounds fragéis).

6. **Archival**: confirmar que `archive_fetch()` está sendo chamado em
   cada HTTP. Se pipeline estava pulando archival, adicionar (ver
   `docs/archival.md`).

7. **Tests** + **commits** atômicos:
   - `fix(etl): pncp_go — parametros de API atualizados`
   - `fix(etl): camara_goiania — seletor HTML adaptado`

## Critério de "pronto"

- `pncp_go` volta com `rows_loaded > 0`, `/stats.municipal_bid_count` > 0
- `camara_goiania` volta com `rows_loaded > 0` OU fica documentado como
  fonte bloqueada em `docs/pipeline_status.md`

## Se travar

- Se `camara_goiania` realmente não tem como scrappear (layout moderno
  JS-only, bot-detection, etc), parar e:
  1. Atualizar `docs/source_registry_br_v1.csv` com `access_mode=
     blocked_external` ou similar
  2. Criar nota em `todo-list-prompts/high_priority/` pra quando
     aparecer alternativa (API futura, basedosdados.org, etc)
  3. Commit de docs, sem código novo

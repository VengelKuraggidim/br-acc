# Destravar pipeline `tce_go` (TCE Goiás)

## Contexto
O pipeline `etl/src/bracc_etl/pipelines/tce_go.py` é um scaffold: lê CSVs de `data/tce_go/` e os transforma em nós Neo4j. Hoje está `implemented_partial / not_loaded` no registry porque **TCE-GO não publica API JSON nem export CSV em massa** — apenas dashboards HTML em https://portal.tce.go.gov.br/.

Sua tarefa: viabilizar a primeira carga real, idealmente sem depender de humano copiar-e-colar CSV.

## Arquivos relevantes
- `etl/src/bracc_etl/pipelines/tce_go.py` (pipeline scaffold, 282 linhas)
- `etl/tests/test_tce_go_pipeline.py` (testes — já passam com fixtures mínimas)
- `docs/source_registry_br_v1.csv` (linha `tce_go,...`; atualizar quality_tier quando carregar)
- `etl/src/bracc_etl/runner.py` (registro de pipelines)

## Schema esperado (de `extract()` e `transform()`)
Arquivos em `data/tce_go/`:
- `decisoes.csv` → `TceGoDecision` — colunas procuradas: `numero|nr_processo|acordao|decisao`, `tipo|tipo_decisao|modalidade`, `data|dt_publicacao|data_decisao`, `orgao|unidade`, `ementa|resumo|descricao`, `relator|conselheiro`
- `irregulares.csv` → `TceGoIrregularAccount` + rel `IMPEDIDO_TCE_GO` para `Company` — colunas: `cnpj|cpf_cnpj|documento`, `nome|razao_social|responsavel`, `processo|nr_processo`, `julgamento|data_julgamento|data`, `motivo|fundamento|decisao`
- `fiscalizacoes.csv` → `TceGoAudit` — colunas: `numero|nr_processo|processo`, `titulo|objeto|descricao`, `orgao|unidade|jurisdicionado`, `status|situacao|fase`, `data_inicio|dt_inicio|inicio`

## Missão (em ordem de preferência)
1. **Descoberta de endpoints não documentados** (30–60 min de recon):
   - Portal TCE-GO provavelmente usa framework que expõe JSON em URLs internos (Drupal, BI, search REST). Olhar DevTools → Network ao navegar os dashboards; checar `robots.txt`, `sitemap.xml`, `/api/*`, `/ws/*`, `/rest/*`, subdomínios tipo `transparencia.tce.go.gov.br`.
   - Se achar JSON: adicionar método `_extract_from_api()` no pipeline como caminho primário, mantendo CSV como fallback.
2. **Scraping HTML** se não houver JSON:
   - Respeitar `robots.txt` e adicionar `User-Agent` identificando o projeto.
   - Usar `httpx` + `selectolax` ou `beautifulsoup4`; adicionar `tenacity` para retry.
   - Rate limit conservador (≥ 1s entre requests).
3. **Pedido LAI** como plano C: registrar solicitação em https://www.tce.go.gov.br/ ou e-SIC pedindo os 3 datasets em CSV; deixar `data/tce_go/README.md` documentando o pedido.

## Critérios de aceite
- `data/tce_go/` populado com pelo menos 1 dataset real (preferencialmente decisoes.csv — tem volume).
- Pipeline roda com `uv run python -m bracc_etl.runner run --source tce_go --data-dir data` sem erro.
- `rows_loaded > 0` no IngestionRun.
- Todos os testes existentes continuam verdes; novos testes adicionados se vier API/scraping.
- `docs/source_registry_br_v1.csv` atualizado: `quality_tier=healthy` e `implementation_state=loaded`.
- `etl/tests/test_tce_go_pipeline.py` estendido com teste da nova via (API ou scraping), usando mock HTTP.

## Guardrails do repo
- Trabalhar em branch `ai-session-YYYY-MM-DD-tce-go`, **não** em `main`.
- Commits atômicos; `make pre-commit` verde antes de cada commit.
- Não fazer push sem aprovação humana.
- Se recon revelar que portal exige login ou tem cláusula de reuso restritivo: parar e documentar em memória ao invés de contornar.

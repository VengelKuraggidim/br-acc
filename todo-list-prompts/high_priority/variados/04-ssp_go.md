# Destravar pipeline `ssp_go` (Estatísticas SSP Goiás) — ⏳ PENDENTE (2026-04-22)

> Pipeline retrofitado com archival (commit `92dbf95`, 1 PDF por ano).
> Status registry continua `implemented_partial / not_loaded`. Investigar
> se escopo (PDF mensal → CSV anual já implementado) é o que falta carregar.

## Continuidade da linha (sessão 2026-04-22)

Os outros 3 scaffolds da mesma leva (02-alego, 03-tcmgo_sancoes, 01-tce_go) foram
fechados usando este padrão — vale seguir na mesma ordem aqui:

1. **Recon primeiro, código depois**: inspecionar sitemap / `robots.txt` / DevTools
   de portais correlatos (`goias.gov.br/seguranca/`, `dadosabertos.go.gov.br`
   com `q=ssp`, subdomínios `ws.`/`painel.`/`dadosabertos.`). TCE-GO e TCM-GO
   tinham REST escondida não documentada — SSP pode ter feed CKAN separado,
   Power BI embed ou painel Qlik.
2. **Se houver endpoint estruturado**: adicionar `fetch_to_disk()` no
   `etl/src/bracc_etl/pipelines/ssp_go.py` + CLI wrapper em `scripts/download_ssp_go.py`
   (mirror de `download_tce_go.py` ou `download_tcmgo_sancoes.py`).
3. **Se só PDF**: adicionar `pdfplumber` em group opcional `[project.optional-dependencies] ssp = [...]`
   do `etl/pyproject.toml` — não core. Fixture de 1 página de PDF real em
   `etl/tests/fixtures/ssp_go/` pra test offline.
4. **Rodar local**: `uv run python -m bracc_etl.runner run --source ssp_go --neo4j-password changeme --data-dir ../data`.
5. **Atualizar `docs/source_registry_br_v1.csv`** (linha `ssp_go,...`): `status=loaded`,
   `load_state=loaded`, `quality_status=healthy`, `access_mode=api|pdf` conforme o caso.
6. **Deletar este arquivo** (`git rm`) quando carregar; abrir débito em
   `high_priority/debitos/ssp-go-<lacuna>.md` pro que ficar parado.

## Infra gotchas ativos

- **`etl/archival/` owned by root** — rodar com `BRACC_ARCHIVAL_ROOT=/tmp/archival_ssp_go`
  até chown (débito: `high_priority/debitos/archival-chown.md`).
- **Neo4j MERGE sem índice trava** — se `GoSecurityStat` não tem índice em
  `stat_id`, o load trava em full-scan no batch de 50k (aconteceu com
  `CampaignExpense` no todo 07). Checar `SHOW INDEXES` antes; se faltar,
  adicionar em `api/src/bracc/queries/schema_init.cypher` (mesma fix que
  `high_priority/debitos/campaign-expense-index.md` descreve).
- **Container `fiscal-bracc-api` é image baked** — edits em `api/src` não
  hot-reload, só após rebuild. Neo4j local sobe em `docker-compose` com
  password `changeme`.
- **Runner `--start-year`** existe (desde 2026-04-21) pra limitar volume
  em pipelines que aceitam o kwarg.

## Referências de código dos 3 anteriores desta leva

- **`tce_go.py`** (REST JSON backend): padrão de `fetch_to_disk` paginado com
  `httpx.Client` injetável, CSV escrito com `csv.DictWriter` delimitado por `;`,
  `_SAMPLE_FIELD_ORDER` documentado.
- **`tcmgo_sancoes.py`** (REST CSV direto): `_rewrite_contas_csv` faz header
  remap antes de salvar + `_archive_contas_online` opt-in.
- **`alego.py`** (API JSON não documentada via Angular bundle): `fetch_to_disk`
  junta 3 endpoints num formato CSV único pro `transform`.

## Contexto
O pipeline `etl/src/bracc_etl/pipelines/ssp_go.py` é scaffold: ingere estatísticas agregadas de segurança pública (ocorrências por município × tipo de crime × período) publicadas pela SSP-GO em https://goias.gov.br/seguranca/. Hoje `implemented_partial / not_loaded` — **fonte é tipicamente PDF mensal, sem API nem CSV**.

Este é o mais difícil dos 4 scaffolds porque envolve extração de PDF.

## Arquivos relevantes
- `etl/src/bracc_etl/pipelines/ssp_go.py` (scaffold, 143 linhas)
- `etl/tests/test_ssp_go_pipeline.py`
- `docs/source_registry_br_v1.csv` (linha `ssp_go,...`)

## Schema esperado
Arquivo em `data/ssp_go/`:
- `ocorrencias.csv` → `GoSecurityStat` — colunas: `municipio|nome_municipio|cidade`, `cod_ibge|codigo_ibge|ibge`, `natureza|tipo_ocorrencia|crime|classificacao`, `periodo|mes_ano|data|ano`, `quantidade|total|count|ocorrencias`

Pipeline normaliza nomes e converte quantidade pra `int`. Cada linha = 1 stat_id.

## Missão (em ordem)
1. **Recon do portal da SSP-GO** (30 min):
   - https://goias.gov.br/seguranca/estatisticas/ — costuma ter "Boletim Mensal" em PDF e/ou painel dinâmico Power BI.
   - Se houver painel **Power BI embed**: extrair o JSON data model (via `https://app.powerbi.com/...` → query endpoint `querydata`) — frequentemente mais fácil que PDF.
   - Procurar `dadosabertos.go.gov.br` por dataset de segurança (a SSP pode ter feed CKAN separado).
   - Checar `transparencia.go.gov.br` → Segurança Pública.
2. **Extração de PDF** (se só PDF existir):
   - Usar `pdfplumber` (add ao `etl/pyproject.toml` como opcional `[project.optional-dependencies] ssp = ["pdfplumber"]`).
   - Fazer parser da tabela "Ocorrências por Município" — estrutura é razoavelmente estável mês a mês.
   - Começar com 1 boletim (ex.: mês mais recente) pra validar parser antes de escalar.
3. **LAI** como plano C: e-SIC da SSP-GO; pedido de histórico em CSV.

## Critérios de aceite
- `ocorrencias.csv` com pelo menos 1 mês de dados carregado (~300 linhas: 246 municípios × poucos tipos de crime).
- `uv run python -m bracc_etl.runner run --source ssp_go --data-dir data` cria nós `GoSecurityStat` com `uf=GO`.
- Pelo menos 1 município de teste validado: contagens batem com o boletim original (sanity check manual).
- `docs/source_registry_br_v1.csv`: `quality_tier=healthy`, `implementation_state=loaded`.
- Se adicionou `pdfplumber` ou outra dep: `etl/uv.lock` atualizado; dep sob grupo opcional, não core (pra não inflar instalação padrão).

## Guardrails do repo
- Branch dedicada; `make pre-commit` verde; sem push.
- Se extrator de PDF for adicionado: fixture com 1 PDF real (ou página-amostra) em `etl/tests/fixtures/ssp_go/` pra teste offline.
- Tomar cuidado com **taxonomia de crimes**: SSP usa categorias próprias que não batem 1-para-1 com outras unidades federativas. Não forçar mapeamento pra taxonomia unificada agora — registrar categoria crua no campo `crime_type` e documentar no `docs/source_registry_br_v1.csv` (notes).
- Se boletins vierem em PDF digitalizado (scanned): parar e documentar — OCR sai do escopo dessa lane.

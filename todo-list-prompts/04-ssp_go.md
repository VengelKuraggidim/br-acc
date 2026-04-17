# Destravar pipeline `ssp_go` (Estatísticas SSP Goiás)

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

# Destravar pipeline `alego` (Assembleia Legislativa de Goiás)

## Contexto
O pipeline `etl/src/bracc_etl/pipelines/alego.py` é scaffold: lê CSVs de `data/alego/` e gera nós Neo4j. Hoje `implemented_partial / not_loaded` porque **ALEGO não expõe API documentada nem CSV bulk** em https://transparencia.al.go.leg.br/.

**Pista forte**: o docstring do próprio pipeline (linha 14) sugere que ALEGO *talvez* tenha endpoint estilo Câmara Federal em `alegodigital.al.go.leg.br/dadosabertos/deputados`. Validar isso é a primeira coisa a fazer.

## Arquivos relevantes
- `etl/src/bracc_etl/pipelines/alego.py` (scaffold, ~240 linhas)
- `etl/tests/test_alego_pipeline.py`
- `etl/src/bracc_etl/pipelines/camara.py` (referência: Câmara Federal tem endpoint `/dadosabertos/`; se ALEGO copiou o padrão, mesma estrutura deve funcionar)
- `docs/source_registry_br_v1.csv` (linha `alego,...`)

## Schema esperado
Arquivos em `data/alego/`:
- `deputados.csv` → `StateLegislator` — colunas: `nome|deputado|nome_parlamentar`, `cpf|documento`, `partido|sigla_partido`, `legislatura|mandato`
- `cota_parlamentar.csv` → `LegislativeExpense` + rel `GASTOU_COTA_GO` para `StateLegislator` — colunas: `deputado|nome|nome_parlamentar`, `fornecedor|razao_social`, `cnpj_fornecedor|cnpj`, `valor|valor_liquido|valor_total`, `data|data_emissao|dt_documento`, `tipo_despesa|natureza|descricao`
- `proposicoes.csv` → `LegislativeProposition` — colunas: `numero|nr_proposicao|identificacao`, `titulo|ementa|assunto`, `autor|proponente`, `data|data_apresentacao`

## Missão (em ordem)
1. **Confirmar hipótese do endpoint `dadosabertos`** (primeiros 15 min):
   - Testar `curl https://alegodigital.al.go.leg.br/dadosabertos/deputados` e variações (`/api/v1/deputados`, `/v2/`, etc).
   - Checar sitemap, `robots.txt`, documentação de transparência.
   - Se achar JSON/XML: documentar schema e refatorar pipeline pra usar API como via primária.
2. **Scraping HTML** se API não existir:
   - `transparencia.al.go.leg.br` — inspecionar tabelas de cota parlamentar (padrão comum: endpoint `/ajax/` que retorna JSON pra renderizar datatable).
   - Usar `httpx` + `selectolax`; respeitar `robots.txt`; rate limit ≥ 1s.
3. **LAI** como fallback: `ouvidoria.al.go.leg.br` recebe pedido LAI; documentar em `data/alego/README.md`.

## Critérios de aceite
- Pelo menos `deputados.csv` carregado (dataset de menor volume; permite validar schema rapidamente).
- `uv run python -m bracc_etl.runner run --source alego --data-dir data` roda sem erro e cria nós `StateLegislator` com `uf=GO`.
- Dica de privacidade: CPFs são mascarados via `mask_cpf()` — conferir que nenhum CPF cru vaza no grafo.
- `docs/source_registry_br_v1.csv`: `quality_tier=healthy`, `implementation_state=loaded` pós-carga.
- Teste novo em `test_alego_pipeline.py` cobrindo a nova via.

## Guardrails do repo
- Branch dedicada; `make pre-commit` verde; sem push sem aprovação.
- LGPD: CPFs de deputados são públicos mas o pipeline mascara por convenção do projeto — **não remover** `mask_cpf()`.
- Se endpoint exigir token/API key: adicionar em `.env.example` com valor vazio e ler via `os.environ.get(...)`, nunca commitar secret.

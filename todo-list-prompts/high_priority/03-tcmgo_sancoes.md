# Destravar pipeline `tcmgo_sancoes` (Sanções TCM Goiás)

## Contexto
O pipeline `etl/src/bracc_etl/pipelines/tcmgo_sancoes.py` é scaffold: ingere a lista de "impedidos de licitar, contratar ou exercer cargo público" e contas municipais rejeitadas publicadas por TCM-GO (https://www.tcmgo.tc.br/). Hoje `implemented_partial / not_loaded` — **sem API, apenas HTML**.

**Importante**: não confundir com pipeline `tcm_go` (já healthy) que ingere SICONFI fiscal data. Este é separado — trata de **sanções** e **rejeições de contas**.

## Arquivos relevantes
- `etl/src/bracc_etl/pipelines/tcmgo_sancoes.py` (scaffold, ~230 linhas)
- `etl/src/bracc_etl/pipelines/tcm_go.py` (NÃO mexer — pipeline diferente, já carregado)
- `etl/tests/test_tcmgo_sancoes_pipeline.py`
- `etl/src/bracc_etl/pipelines/sanctions.py` (referência para padrões de impedidos de outras fontes)

## Schema esperado
Arquivos em `data/tcmgo_sancoes/`:
- `impedidos.csv` → `TcmGoImpedido` (+ rel `IMPEDIDO_TCMGO` pra `Company` quando CNPJ) — colunas: `cpf_cnpj|documento|cnpj|cpf`, `nome|razao_social|responsavel`, `motivo|fundamento|decisao`, `processo|nr_processo`, `data_inicio|inicio_impedimento|dt_inicio`, `data_fim|fim_impedimento|dt_fim`
- `rejeitados.csv` → `TcmGoRejectedAccount` — colunas: `municipio|ente|nome_ente`, `cod_ibge|codigo_ibge|ibge`, `exercicio|ano|ano_exercicio`, `processo|nr_processo`, `parecer|julgamento|decisao`, `relator|conselheiro`

O pipeline distingue CPF (11 dig) vs CNPJ (14 dig) pelo tamanho e mascara CPF.

## Missão (em ordem)
1. **Recon do portal TCM-GO** (30 min):
   - https://www.tcmgo.tc.br/ → procurar aba "Transparência", "Impedidos", "Julgamentos", "Pareceres de Contas".
   - Muitos tribunais usam sistema **e-Contas** ou **TCE-NET** que expõe JSON em `/rest/` ou `/api/`.
   - Checar DevTools → Network ao filtrar tabelas; frequentemente a busca é JSON.
   - Checar se existe feed RSS ou endpoint `/diario-eletronico/`.
2. **Scraping HTML** se não houver JSON:
   - Tabelas de impedidos costumam ser DataTables jQuery — endpoint `/ajax/...` retorna JSON mesmo sem API pública documentada.
   - Respeitar `robots.txt`; User-Agent identificado; rate limit ≥ 1s.
3. **LAI** como fallback: e-SIC do TCM-GO; pedido por lista completa de impedidos + contas rejeitadas nos últimos 5 anos em CSV.

## Critérios de aceite
- `impedidos.csv` carregado (mais crítico — integra com grafo de sanções via `Company` e conecta ao pipeline `sanctions` existente).
- `rows_loaded > 0`; nós `TcmGoImpedido` + relações `IMPEDIDO_TCMGO` criados.
- `docs/source_registry_br_v1.csv`: `quality_tier=healthy`, `implementation_state=loaded`.
- Validar que CPFs aparecem mascarados no Neo4j (query: `MATCH (t:TcmGoImpedido) WHERE t.document_kind='CPF' RETURN t.document LIMIT 5`).
- Teste estendido em `test_tcmgo_sancoes_pipeline.py` cobrindo a via nova.

## Guardrails do repo
- Branch dedicada; `make pre-commit` verde; sem push.
- LGPD: `mask_cpf()` é obrigatório. Não remover.
- Se scraping for a saída, adicionar fixture HTML real (tipo 1–2 páginas) em `etl/tests/fixtures/tcmgo_sancoes/` pra teste offline.
- **Não mover nem renomear** `tcm_go.py` (pipeline SICONFI diferente); só mexer em `tcmgo_sancoes.py`.

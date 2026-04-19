# Mais dados no grafo — investigação de pipelines parados

## Contexto

Estado observado em 2026-04-19: das **124 fontes catalogadas** em
`docs/source_registry_br_v1.csv`, apenas **9 têm dados efetivos** no
grafo (badge `com_dados` na aba Fontes). As outras caem em 3 categorias:

1. **Rodou com zero linhas** (8 pipelines): `IngestionRun.status=loaded`
   mas `rows_in=0` e `rows_loaded=0`. Pipeline entrou no `run()`, chegou
   ao `load()`, mas nem extraiu linha da fonte. Extract quebrado.

2. **Baixado em disco, nunca rodou `run()`** (6 pipelines): arquivos
   existem em `data/<source>/` (juntos ~10 GB), mas nenhum `IngestionRun`
   no grafo. `bracc-etl run --source X` nunca foi chamado.

3. **Nem baixado nem rodado** (~100 pipelines): catalogadas no registry,
   sem arquivo em `data/`, sem `IngestionRun`. Escopo v1 do fork mas
   nunca materializado.

## Guardrails (valem pra todo prompt desta pasta)

Ver `CLAUDE.md` na raiz. Reforçando o mais relevante aqui:

- **Sem branch novo.** Trabalhar em `main`.
- **Sem auto-push.** Humano pusha depois.
- **Um commit por experimento.** Atômico, reversível, conventional pt-BR.
- **Stop em ambiguidade.** Não inventar solução — sinalizar ao humano.
- **Pipeline pattern**: subclasse `bracc_etl.base.Pipeline`, com
  `extract()`, `transform()`, `load()`, `archive_fetch()` em cada HTTP,
  `attach_provenance()` em cada row.
- **Neutrality check**: `make neutrality` roda regex em `api/src/` e
  `etl/src/`. Sem termos `suspicious|corrupt|criminal|fraudulent|illegal|
  guilty|CRITICAL|HIGH/MEDIUM/LOW severity`. Usar termos neutros.
- **Rodar antes de commitar**: `make pre-commit`.
- **Verificação no grafo**:
  ```bash
  docker exec fiscal-neo4j cypher-shell -u neo4j \
    -p "$(docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2)" \
    "<query>"
  ```

## Prioridade (alto → baixo impacto)

| # | Prompt | Fontes | Tamanho | Por que prioritário |
|---|---|---|---|---|
| 01 | `01-fix-transparencia.md` | `transparencia` | 1.9 GB | Rodou 0 rows; contratos federais em GO = alto valor; alias `portal_transparencia`→`transparencia` |
| 02 | `02-fix-tse.md` | `tse`, `tse_bens`, `tse_filiados` | 2.3 GB | Rodou 0 rows; base eleitoral é coração do projeto; alias `tribunal_superior_eleitoral`→`tse` |
| 03 | `03-fix-cvm.md` | `cvm`, `cvm_funds` | 18 MB | Rodou 0 rows; conexões financeiras com empresários GO |
| 04 | `04-fix-pncp-camara-goiania.md` | `pncp_go`, `camara_goiania` | 5.9 MB | Rodou 0 rows; compras GO + vereadores da capital |
| 05 | `05-run-downloaded-pipelines.md` | `comprasnet`, `pgfn`, `siop`, `senado`, `tesouro_emendas`, `camara` | 6.4 GB | Baixados, nunca carregados — só orquestrar o `run()` |
| 06 | `06-fix-source-id-alias.md` | Fix cross-cutting | — | Alguns pipelines escrevem `source_id` divergente do registry (`portal_transparencia`, `tribunal_superior_eleitoral`); normalizar na origem |
| 07 | `07-tier-unloaded-sources.md` | ~100 fontes zeradas | — | Planejamento: tier P1/P2/P3 por impacto pra nortear próximos sprints |

## Como rodar

Cada prompt é self-contained. Executar via Agent tool (ou subagente),
uma por vez. Prompts 01-04 são independentes e podem rodar em paralelo
com `isolation: "worktree"` se forem ao mesmo tempo, pois cada um toca
pipeline diferente. 05 é orquestração pura, pode rodar em série. 06
toca múltiplos arquivos — rodar isolado, sem paralelismo. 07 é
pesquisa, não toca código.

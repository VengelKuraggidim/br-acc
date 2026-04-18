# refresh_photos — fotos de políticos GO

Script único que roda os 4 pipelines de foto na **ordem correta** e
atualiza o grafo. Substitui ter que decorar `bracc-etl run --source X`
quatro vezes.

## TL;DR

```bash
# Tudo: 4 pipelines em sequência (TSE pode demorar ~70min sem --limit)
make refresh-photos

# Só os rápidos (Senado + Wikidata) pra dev
make refresh-photos ARGS="--only senado_senadores_foto,wikidata_politicos_foto"

# TSE com amostra (recomendado pra dev)
make refresh-photos ARGS="--only tse_candidatos_foto --limit 100"

# Dry-run pra ver o que seria executado
make refresh-photos ARGS="--dry-run"
```

## Ordem canônica e dependências

| # | Pipeline | Cria nodes? | Depende de | Cadência sugerida |
|---|---|---|---|---|
| 1 | `senado_senadores_foto` | ✅ `:Senator` | nada | semanal |
| 2 | `alego_deputados_foto` | enriquece `:StateLegislator` | `alego` (cadastro) ter rodado | mensal |
| 3 | `wikidata_politicos_foto` | só atualiza | `:FederalLegislator`/`:StateLegislator`/`:Person` GO existirem | trimestral |
| 4 | `tse_candidatos_foto` | só atualiza | `:Person` GO com `sq_candidato` | bienal |

A ordem importa: 1 e 2 podem **criar** nodes; 3 e 4 só fazem `MATCH/SET`,
então têm que rodar depois. Se rodar fora de ordem num grafo vazio,
wikidata e tse vão silenciosamente cobrir zero.

## Flags

| Flag | Uso |
|---|---|
| `--only A,B` | Roda só esses pipelines (mantém ordem canônica) |
| `--skip A,B` | Pula esses |
| `--limit N` | Aplica só ao `tse_candidatos_foto` (4k+ candidatos × 1s throttle = ~70min sem limite) |
| `--dry-run` | Imprime comandos sem executar |
| `--continue-on-error` | Não para se um pipeline falha |

## Credenciais Neo4j

O script lê em ordem: `$NEO4J_PASSWORD` → `.env` → `docker exec
fiscal-neo4j env`. Se nada disso achar, o `bracc-etl` cai pro GCP
Secret Manager (`fiscal-cidadao-neo4j-password`).

Em **dev local**: a senha vive em `.env` (gitignored). Se sumir, ver
[`CLAUDE.md` §2](../CLAUDE.md).

Em **prod (Cloud Run + Aura)**: setar `NEO4J_URI`, `NEO4J_USER`,
`NEO4J_DATABASE`, `NEO4J_PASSWORD` como variáveis de ambiente do job.

## Quando rodar

- **Dev**: depois de `make bootstrap-go` (que popula
  `:FederalLegislator`/`:StateLegislator`/`:Person` GO base) — aí
  `refresh-photos` enriquece com fotos.
- **Prod**: agendar via **Cloud Scheduler → Cloud Run job**, cadência
  por pipeline (semanal pra Senado, mensal pra ALEGO, trimestral pra
  Wikidata, bienal pra TSE alinhada com calendário eleitoral). Snippet
  rápido:

  ```bash
  gcloud run jobs create refresh-photos-senado \
    --image gcr.io/$PROJECT/bracc-etl:latest \
    --command python3 \
    --args scripts/refresh_photos.py,--only,senado_senadores_foto \
    --set-secrets NEO4J_PASSWORD=fiscal-cidadao-neo4j-password:latest \
    --set-env-vars NEO4J_URI=neo4j+s://xxx.databases.neo4j.io
  gcloud scheduler jobs create http refresh-photos-senado-weekly \
    --schedule "0 3 * * 1" --uri "https://...run.app/jobs/refresh-photos-senado:run"
  ```

  (Ver `docs/deploy.md` pra setup completo de Aura + service accounts.)

## Restart da API?

**Não precisa.** Os endpoints (`/politico/{id}`, `/buscar-tudo`) leem
`foto_url` do nó em runtime. Assim que o pipeline gravar no Neo4j, o
próximo request retorna a foto. Restart só seria preciso se o **schema**
do model Pydantic mudasse.

## Troubleshooting

- **`Authentication failure`** → senha errada. Recupere com
  `docker exec fiscal-neo4j env | grep NEO4J_AUTH | cut -d/ -f2`.
- **`alego_deputados_foto` carrega 0 fotos** → cadastro ALEGO não rodou.
  Rode `bracc-etl run --source alego` antes.
- **Wikidata 429 / throttle** → reduzir paralelismo (já rodamos serial
  com 1s/req). Se persistir, esperar 1h.
- **ALEGO HTML mudou (selector quebrou)** → pipeline levanta
  `RuntimeError` explícito com URL + snapshot URI. Inspecionar HTML
  arquivado em `archival/alego_deputados_foto/{YYYY-MM}/` pra ajustar
  regex em `etl/src/bracc_etl/pipelines/alego_deputados_foto.py`.
- **TSE retorna placeholder pra todos** → URL pattern mudou. URL
  canônica validada em 2026-04-18:
  `/divulga/rest/arquivo/img/{cd_eleicao}/{sq_candidato}/{uf}` (sem
  `/foto.jpg`). Mapeamento `cd_eleicao` em
  `etl/src/bracc_etl/pipelines/tse_candidatos_foto.py`; adicionar 2026
  pós-pleito.

## Próximos passos

- Adicionar pipeline pra **vereadores Goiânia** (Câmara Municipal) —
  hoje `:Vereador` não tem fonte de foto.
- Considerar **prefeitos GO** via Wikidata (P39 = mayor, P768 =
  electoral district) — extensão natural do `wikidata_politicos_foto`.

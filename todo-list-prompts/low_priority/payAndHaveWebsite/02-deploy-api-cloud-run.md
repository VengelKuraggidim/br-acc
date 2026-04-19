---
# Deploy API no Cloud Run + smoke tests — ⏳ PENDENTE

> Depende de [01-provisionar-neo4j-prod.md](01-provisionar-neo4j-prod.md).
> Com Neo4j prod populado, buildar e deployar a imagem no Cloud Run.

## Contexto

Toda a infraestrutura do GCP já existe (SA, buckets, secrets). O
script `scripts/deploy/deploy_api.sh` lê env vars do ambiente e chama
`gcloud run deploy` — basta exportar os valores corretos do Neo4j
prod e rodar.

A imagem Docker precisa ser buildada antes (`api/Dockerfile` já
está Cloud Run-ready: non-root user, `$PORT`, `--extra gcp`).

## Arquivos relevantes

- `api/Dockerfile` — revisar se ainda está válido
- `scripts/deploy/deploy_api.sh` — script principal
- `api/src/bracc/secrets.py` — precisa `GCP_PROJECT_ID` setado +
  `BRACC_SECRETS_SOURCE` NÃO setado (default `gcp` é obrigatório em prod)
- `CLAUDE.md §3` — guardrails de secrets

## Missão

1. **Build da imagem** (5-10 min):
   ```bash
   DEPLOY_TAG="v$(git rev-parse --short HEAD)"
   gcloud builds submit api/ \
     --tag "gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:${DEPLOY_TAG}" \
     --tag "gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:latest" \
     --project=fiscal-cidadao-493716
   ```
   Confirmar `STATUS: SUCCESS` no final.

2. **Exportar env vars com os valores do Neo4j prod** (do prompt 01):
   ```bash
   export NEO4J_URI=<URI_DO_PROMPT_01>      # neo4j+s://... ou bolt://10.x.x.x:7687
   export NEO4J_USER=<USER_DO_PROMPT_01>    # 'neo4j' na maioria dos casos
   export NEO4J_DATABASE=<DB_DO_PROMPT_01>  # 'neo4j' na maioria dos casos
   ```
   Se Opção A (GCE VM): usar IP interno da VM; Cloud Run acessa via
   Serverless VPC Connector.

3. **Deploy**:
   ```bash
   bash scripts/deploy/deploy_api.sh
   ```
   **Se Opção A (GCE VM)**: adicionar `--vpc-connector=fiscal-connector
   --vpc-egress=private-ranges-only` ao `gcloud run deploy` dentro do
   script (editar antes de rodar).

4. **Smoke tests**:
   ```bash
   URL=$(gcloud run services describe fiscal-cidadao-api \
     --project=fiscal-cidadao-493716 --region=southamerica-east1 \
     --format='value(status.url)')

   curl -fsS "$URL/health"
   # Esperado: {"status":"ok"}

   curl -fsS "$URL/status"
   # Esperado: JSON com contadores reais (Politico, Empresa, Contrato etc. >0)

   # Escolher um deputado federal GO via busca:
   curl -fsS "$URL/buscar-tudo?q=joão"

   # Pegar um ID válido do response acima e testar:
   curl -fsS "$URL/politico/<ID>"
   ```

5. **Verificar logs por erros**:
   ```bash
   gcloud run services logs tail fiscal-cidadao-api \
     --project=fiscal-cidadao-493716 --region=southamerica-east1
   ```
   Procurar traces de conexão Neo4j falha, timeouts, CPU throttling.
   Se tiver, ajustar `--concurrency` ou `--memory` no deploy_api.sh.

## Critérios de aceite

- Imagem buildada e tagueada com git sha + latest.
- Cloud Run service `fiscal-cidadao-api` em revisão ativa, 100%
  tráfego.
- `/health` retorna `{"status":"ok"}` em <500ms.
- `/status` retorna contadores condizentes com o grafo prod (não zero,
  não erro).
- `/politico/<ID>` retorna JSON válido pra pelo menos 1 deputado GO.
- Logs limpos (sem erros de conexão Neo4j recorrentes).

## Guardrails

- **Nunca setar `BRACC_SECRETS_SOURCE`** no Cloud Run — default `gcp`
  é obrigatório (guardrail em `secrets.py`).
- **Nunca** usar `gcr.io/.../...:latest` sem ter tag sha ao lado — se
  precisar rollback, a imagem anterior precisa ser identificável.
- Commits: documentar a nova URI/config em `docs/deploy.md`, commit
  prefix `feat(deploy):`.
- Se CPU/memory insuficiente: ajustar script, commitar, redeploy. Não
  ajustar "no ar" via `gcloud run services update` sem commit
  correspondente.

# Deploy — Cloud Run (GCP)

Sequência pra subir o FastAPI `fiscal-cidadao-api` no Cloud Run em
`southamerica-east1`. PWA + Neo4j Aura são TODOs (ver fim).

> **Nota histórica.** O `DEPLOY.md` na raiz descreve um caminho
> alternativo via Oracle Cloud + docker-compose — útil se quiser
> self-host. Esse doc cobre apenas GCP Cloud Run.

---

## 1. Pré-requisitos

- `gcloud` CLI instalada e autenticada:
  ```bash
  gcloud auth login
  gcloud auth application-default login
  gcloud config set project fiscal-cidadao-493716
  ```
- APIs ativadas no projeto (uma vez):
  ```bash
  gcloud services enable \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    secretmanager.googleapis.com \
    storage.googleapis.com \
    containerregistry.googleapis.com
  ```
- Secrets já criados no Secret Manager (feito):
  - `fiscal-cidadao-neo4j-password`
  - `fiscal-cidadao-jwt-secret`
  - `fiscal-cidadao-transparencia-key`

---

## 2. Setup inicial (rodar uma vez)

### 2.1. Service account + IAM

```bash
bash scripts/deploy/create_service_account.sh
```

Cria `fiscal-cidadao-api@fiscal-cidadao-493716.iam.gserviceaccount.com`
com `roles/secretmanager.secretAccessor` nos 3 secrets acima.
Idempotente — safe rodar de novo.

### 2.2. Bucket de archival

```bash
bash scripts/deploy/create_archival_bucket.sh
```

Cria `gs://fiscal-cidadao-archival` em `southamerica-east1`
(uniform-bucket-level-access, public-access-prevention). SA da API só
lê — escrita é dos pipelines ETL (ver seção 4).

---

## 3. Build + deploy (rodar a cada release)

### 3.1. Build da imagem

```bash
gcloud builds submit api/ \
  --tag gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:latest
```

Contexto de build é `api/` (não a raiz) — Dockerfile e `.dockerignore`
ali. `uv sync --frozen --extra gcp` garante que `google-cloud-secret-manager`
está na imagem (runtime requirement em prod).

Pra pinnar versão, use tag com git sha:

```bash
DEPLOY_TAG="v$(git rev-parse --short HEAD)"
gcloud builds submit api/ \
  --tag "gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:${DEPLOY_TAG}"
```

### 3.2. Deploy

```bash
# tag latest:
bash scripts/deploy/deploy_api.sh

# ou tag específica:
DEPLOY_TAG=v1a2b3c4 bash scripts/deploy/deploy_api.sh
```

Script faz `gcloud run deploy` com `--service-account`, env vars
(`GCP_PROJECT_ID`, `APP_ENV=prod`, flags `PUBLIC_MODE`) e limites
conservadores (max-instances=3, memory=512Mi, cpu=1). Inspecione
`scripts/deploy/deploy_api.sh` antes de rodar.

---

## 4. Archival ETL — credencial separada

Pipelines ETL que escrevem snapshots precisam de SA própria com
`roles/storage.objectCreator`. Não usar a SA da API pra isso — a API
só lê.

Criar sob demanda:

```bash
PROJECT_ID=fiscal-cidadao-493716
ETL_SA=fiscal-cidadao-etl
gcloud iam service-accounts create "$ETL_SA" \
  --project="$PROJECT_ID" \
  --display-name="Fiscal Cidadão ETL (archival writes)"

gcloud storage buckets add-iam-policy-binding gs://fiscal-cidadao-archival \
  --member="serviceAccount:${ETL_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectCreator"
```

---

## 5. Testar após deploy

```bash
URL=$(gcloud run services describe fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1 \
  --format='value(status.url)')

# Healthcheck liveness (não toca Neo4j):
curl -fsS "$URL/health"
# -> {"status":"ok"}

# Status agregado (toca Neo4j — só funciona depois de Aura configurado):
curl -fsS "$URL/status"
```

Logs em tempo real:

```bash
gcloud run services logs tail fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1
```

---

## 6. Rollback

Cloud Run mantém revisões. Pra voltar pra anterior:

```bash
# Lista revisões
gcloud run revisions list \
  --service=fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1

# Roteia 100% do tráfego pra revisão anterior
gcloud run services update-traffic fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1 \
  --to-revisions=REVISION_NAME=100
```

Zero-downtime rollback se a revisão antiga ainda existe.

---

## 7. TODOs (decisões pendentes)

- **Neo4j em prod.** Opção 1: Neo4j Aura (managed) — adicionar
  `NEO4J_URI=neo4j+s://<id>.databases.neo4j.io` em `--set-env-vars` e
  botar o password no secret `fiscal-cidadao-neo4j-password` (já
  existe). Opção 2: GCE VM com Neo4j Community (auto-hosted). Aura é
  default recomendado — sem ops. Depois de decidir, adicionar
  `NEO4J_URI` e `NEO4J_USER` ao `deploy_api.sh`.
- **PWA.** Opções: (a) Cloud Run serve os arquivos estáticos de
  `pwa/` junto com a API (copiar no Dockerfile, montar um rota), (b)
  GCS bucket público + Cloud CDN (bucket separado, `fiscal-cidadao-pwa`).
  (b) é mais barato e isolado; (a) é menos partes móveis pro MVP.
- **Domínio custom.** `gcloud run domain-mappings create` quando o
  domínio for registrado.
- **CI/CD.** Hoje o deploy é manual — migrar pra GitHub Actions com
  OIDC (Workload Identity Federation) assim que o fluxo estabilizar.

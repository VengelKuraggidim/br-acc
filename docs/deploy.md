# Deploy — Cloud Run + Aura Free (GCP)

Sequência pra subir o Fiscal Cidadão em produção:

- **API** (FastAPI) — Cloud Run em `southamerica-east1`
- **PWA** (HTML/JS estático) — GCS bucket público
- **Neo4j** — Aura Free (managed)
- **Archival** — GCS bucket com preservação de snapshots

> **Nota histórica.** O `DEPLOY.md` na raiz descreve um caminho
> alternativo via Oracle Cloud + docker-compose (self-host). Esse doc
> cobre apenas GCP.

---

## 1. Pré-requisitos

- `gcloud` CLI autenticada:
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
- Secrets já criados no Secret Manager:
  - `fiscal-cidadao-neo4j-password`
  - `fiscal-cidadao-jwt-secret`
  - `fiscal-cidadao-transparencia-key`

---

## 2. Neo4j Aura Free (uma vez)

1. Criar conta em [console.neo4j.io](https://console.neo4j.io) — login com
   GitHub/Google.
2. **Create Instance** → **AuraDB Free**. Região mais próxima do
   Cloud Run (SA ainda não é opção — escolher `us-east-1` ou
   `europe-west-1`; `us-east-1` costuma ter latência menor pra SP).
3. Anotar:
   - **URI** (formato `neo4j+s://xxxxxxxx.databases.neo4j.io`).
   - **Username** (`neo4j`).
   - **Generated password** — baixar o arquivo `credentials.txt`
     (Aura só mostra uma vez).
4. Atualizar o secret no GCP com a senha do Aura:
   ```bash
   echo -n "SENHA_GERADA_PELO_AURA" | \
     gcloud secrets versions add fiscal-cidadao-neo4j-password \
     --project=fiscal-cidadao-493716 \
     --data-file=-
   ```
5. Popular o grafo: bootstrap local apontado pro Aura. No seu dev:
   ```bash
   export NEO4J_URI=neo4j+s://xxxxxxxx.databases.neo4j.io
   export NEO4J_PASSWORD=SENHA_AURA
   make bootstrap-go
   ```
   Aura Free tem limite de 200k nodes / 400k relationships — MVP só-GO
   entra tranquilo. Se estourar: Aura Professional (~$65/mês) ou
   migrar pra GCE VM (ver secção 8).

---

## 3. Setup inicial GCP (uma vez)

### 3.1. Service account + IAM da API

```bash
bash scripts/deploy/create_service_account.sh
```

Cria `fiscal-cidadao-api@fiscal-cidadao-493716.iam.gserviceaccount.com`
com `roles/secretmanager.secretAccessor` nos 3 secrets. Idempotente.

### 3.2. Bucket de archival

```bash
bash scripts/deploy/create_archival_bucket.sh
```

Cria `gs://fiscal-cidadao-archival` (private — só SA da API lê;
escrita é dos pipelines ETL, ver secção 6).

### 3.3. Bucket da PWA

```bash
bash scripts/deploy/upload_pwa.sh
```

Cria `gs://fiscal-cidadao-pwa` **público**, uploada `pwa/*.html`,
`sw.js`, `manifest.json` com cache headers apropriados (index/SW
`no-cache` pra updates, manifest 1h). Rerun a cada mudança de PWA.

URL final: `https://storage.googleapis.com/fiscal-cidadao-pwa/index.html`.

---

## 4. Build + deploy da API (a cada release)

### 4.1. Build da imagem

```bash
gcloud builds submit api/ \
  --tag gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:latest
```

Pra pinnar versão:

```bash
DEPLOY_TAG="v$(git rev-parse --short HEAD)"
gcloud builds submit api/ \
  --tag "gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:${DEPLOY_TAG}"
```

### 4.2. Deploy

```bash
export NEO4J_URI=neo4j+s://xxxxxxxx.databases.neo4j.io
bash scripts/deploy/deploy_api.sh
```

O script exige `NEO4J_URI` no ambiente (password vem do Secret
Manager dentro do app). Flags configuradas:

| Flag | Valor | Por quê |
|---|---|---|
| `--memory` | `1Gi` | WeasyPrint (PDF) pode pedir 300-400MB — 512Mi é aperto |
| `--cpu` | `1` | Suficiente pro MVP |
| `--min-instances` | `1` | Evita cold start (~3s em Python) — custa ~$12/mês mas UX fica boa |
| `--max-instances` | `10` | 10×40 concurrency = 400 req simultâneas; cobre pico de viralização |
| `--concurrency` | `40` | Neo4j driver pool default é 100 — 80 concurrent empilha queries; 40 é conservador |
| `--timeout` | `60s` | Endpoints de graph são rápidos; 60s protege contra queries patológicas |
| `--allow-unauthenticated` | on | App público — leigos acessam sem login |

Ajuste conforme observabilidade indicar.

---

## 5. Testar após deploy

```bash
URL=$(gcloud run services describe fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1 \
  --format='value(status.url)')

# Liveness (nao toca Neo4j):
curl -fsS "$URL/health"
# -> {"status":"ok"}

# Tudo integrado (toca Aura):
curl -fsS "$URL/status"
# -> JSON com contadores do grafo
```

Logs em tempo real:

```bash
gcloud run services logs tail fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1
```

Testar a PWA: abrir `https://storage.googleapis.com/fiscal-cidadao-pwa/index.html`
e configurar a URL da API no código ou via query param (ver `pwa/index.html`).

---

## 6. Archival ETL — credencial separada

Pipelines ETL que escrevem snapshots precisam de SA própria com
`roles/storage.objectCreator` (SA da API só lê). Criar sob demanda
quando rodar pipelines em prod — `gcloud iam service-accounts create
fiscal-cidadao-etl` + `buckets add-iam-policy-binding` com
`roles/storage.objectCreator` em `gs://fiscal-cidadao-archival`.

---

## 7. Rollback

Cloud Run mantém revisões. Zero-downtime rollback:

```bash
# Lista revisoes
gcloud run revisions list \
  --service=fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1

# Roteia 100% do trafego pra revisao anterior
gcloud run services update-traffic fiscal-cidadao-api \
  --project=fiscal-cidadao-493716 \
  --region=southamerica-east1 \
  --to-revisions=REVISION_NAME=100
```

Pra re-popular o Aura depois de corrupção de dados: rodar `make
bootstrap-go` localmente apontado pro Aura (ver secção 2.5). Aura
Free não tem snapshot automático — se o dataset crescer, upgrade ou
exportar com `cypher-shell` semanalmente.

---

## 8. Decisões futuras

- **Se Aura Free estourar** (200k nodes / 400k rels): (a) upgrade pra
  Professional (~$65/mês) ou (b) GCE VM `e2-medium` (~$30/mês) na
  mesma região do Cloud Run, com Serverless VPC Connector pra
  conectar. (b) é mais barato e latência <5ms, mas exige ops (backup,
  patches). Decidir quando o dataset justificar.
- **Domínio custom.** Quando registrar: `gcloud run domain-mappings
  create` pra API + Cloud Load Balancer com Cloud CDN pro bucket da
  PWA. Adicionar `CORS_ORIGINS` restrito.
- **CI/CD.** Deploy é manual. Migrar pra GitHub Actions com Workload
  Identity Federation (OIDC, sem service account key) quando o fluxo
  estabilizar.
- **Cloud CDN pra PWA.** Enquanto tráfego for baixo, GCS puro é
  suficiente. Adicionar CDN quando: (a) usuários fora do BR, ou (b)
  pico de tráfego sobrecarregar o bucket. CDN exige Load Balancer
  ($18/mês), então só vale a pena depois.

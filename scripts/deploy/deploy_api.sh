#!/bin/bash
# Deploy do FastAPI `fiscal-cidadao-api` no Cloud Run.
#
# ATENCAO: NAO execute antes de revisar flags. Ver docs/deploy.md pra
# sequencia completa (SA + bucket + Aura + build + deploy).
#
# Pre-req:
#   1. scripts/deploy/create_service_account.sh rodado (uma vez).
#   2. scripts/deploy/create_archival_bucket.sh rodado (uma vez).
#   3. Instancia Neo4j Aura Free criada; NEO4J_URI exportado e
#      password atualizada no secret `fiscal-cidadao-neo4j-password`.
#   4. Imagem buildada em gcr.io/$PROJECT_ID/$SERVICE:$DEPLOY_TAG, via:
#        gcloud builds submit api/ --tag gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:latest
#
# Uso:
#   NEO4J_URI=neo4j+s://xxxxx.databases.neo4j.io bash scripts/deploy/deploy_api.sh
#
# Opcional:
#   DEPLOY_TAG=v1a2b3c4  # default 'latest'
set -euo pipefail

PROJECT_ID="fiscal-cidadao-493716"
REGION="southamerica-east1"
SERVICE="fiscal-cidadao-api"
SA_EMAIL="${SERVICE}@${PROJECT_ID}.iam.gserviceaccount.com"
DEPLOY_TAG="${DEPLOY_TAG:-latest}"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE}:${DEPLOY_TAG}"

if [[ -z "${NEO4J_URI:-}" ]]; then
  echo "ERRO: NEO4J_URI nao setado."
  echo "Exporte a URI da sua instancia Aura Free antes de rodar:"
  echo "  export NEO4J_URI=neo4j+s://xxxxxxxx.databases.neo4j.io"
  exit 1
fi

# Password vem do Secret Manager (fiscal-cidadao-neo4j-password) —
# carregada dentro do app via bracc.secrets.load_secret. Cloud Run so
# precisa saber a URI e o user.
# NEO4J_USER default 'neo4j' (padrao Aura); override via env var se o
# Aura gerou username customizado.
NEO4J_USER="${NEO4J_USER:-neo4j}"
ENV_VARS="GCP_PROJECT_ID=${PROJECT_ID}"
ENV_VARS="${ENV_VARS},APP_ENV=prod"
ENV_VARS="${ENV_VARS},LOG_LEVEL=warning"
ENV_VARS="${ENV_VARS},NEO4J_URI=${NEO4J_URI}"
ENV_VARS="${ENV_VARS},NEO4J_USER=${NEO4J_USER}"
ENV_VARS="${ENV_VARS},PUBLIC_MODE=true"
ENV_VARS="${ENV_VARS},PUBLIC_ALLOW_PERSON=true"
ENV_VARS="${ENV_VARS},PUBLIC_ALLOW_ENTITY_LOOKUP=true"

echo "==> Deployando ${SERVICE} em ${REGION} (imagem: ${IMAGE})..."
gcloud run deploy "$SERVICE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --image="$IMAGE" \
  --service-account="$SA_EMAIL" \
  --set-env-vars="$ENV_VARS" \
  --allow-unauthenticated \
  --min-instances=1 \
  --max-instances=10 \
  --concurrency=40 \
  --memory=1Gi \
  --cpu=1 \
  --port=8080 \
  --timeout=60s

echo ""
echo "Deploy terminado. URL:"
gcloud run services describe "$SERVICE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --format='value(status.url)'

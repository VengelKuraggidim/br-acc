#!/bin/bash
# Deploy do FastAPI `fiscal-cidadao-api` no Cloud Run.
#
# ATENCAO: NAO execute antes de revisar flags. Ver docs/deploy.md pra
# sequencia completa (SA + bucket + build + deploy). Build da imagem
# vem ANTES desse script (gcloud builds submit) — ver DEPLOY_TAG abaixo.
#
# Pre-req:
#   1. scripts/deploy/create_service_account.sh rodado (uma vez).
#   2. scripts/deploy/create_archival_bucket.sh rodado (uma vez).
#   3. Imagem buildada em gcr.io/$PROJECT_ID/$SERVICE:$DEPLOY_TAG, via:
#        gcloud builds submit api/ --tag gcr.io/fiscal-cidadao-493716/fiscal-cidadao-api:latest
#
# Sem Neo4j Aura configurado ainda — ver TODO em docs/deploy.md.
set -euo pipefail

PROJECT_ID="fiscal-cidadao-493716"
REGION="southamerica-east1"
SERVICE="fiscal-cidadao-api"
SA_EMAIL="${SERVICE}@${PROJECT_ID}.iam.gserviceaccount.com"
DEPLOY_TAG="${DEPLOY_TAG:-latest}"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE}:${DEPLOY_TAG}"

echo "==> Deployando ${SERVICE} em ${REGION} (imagem: ${IMAGE})..."
gcloud run deploy "$SERVICE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --image="$IMAGE" \
  --service-account="$SA_EMAIL" \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},APP_ENV=prod,PUBLIC_MODE=true,PUBLIC_ALLOW_PERSON=true,PUBLIC_ALLOW_ENTITY_LOOKUP=true,LOG_LEVEL=warning" \
  --allow-unauthenticated \
  --max-instances=3 \
  --memory=512Mi \
  --cpu=1 \
  --port=8080 \
  --timeout=60s

echo ""
echo "Deploy terminado. URL:"
gcloud run services describe "$SERVICE" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --format='value(status.url)'

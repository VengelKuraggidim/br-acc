#!/bin/bash
# Cria a service account `fiscal-cidadao-api` e da acesso de leitura aos
# 3 secrets do Secret Manager usados pela API em producao.
#
# Idempotente: se a SA ja existe, apenas re-aplica o IAM binding (no-op).
# Nao deleta nem troca roles existentes.
#
# Pre-req: `gcloud auth login` + `gcloud config set project fiscal-cidadao-493716`.
set -euo pipefail

PROJECT_ID="fiscal-cidadao-493716"
SA_NAME="fiscal-cidadao-api"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

SECRETS=(
  "fiscal-cidadao-neo4j-password"
  "fiscal-cidadao-jwt-secret"
  "fiscal-cidadao-transparencia-key"
)

echo "==> Verificando/criando service account ${SA_EMAIL}..."
if gcloud iam service-accounts describe "$SA_EMAIL" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "    SA ja existe, pulando criacao."
else
  gcloud iam service-accounts create "$SA_NAME" \
    --project="$PROJECT_ID" \
    --display-name="Fiscal Cidadao API (Cloud Run)"
fi

echo "==> Concedendo secretAccessor aos 3 secrets..."
for secret in "${SECRETS[@]}"; do
  echo "    -> $secret"
  gcloud secrets add-iam-policy-binding "$secret" \
    --project="$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet
done

echo ""
echo "Service account pronta: $SA_EMAIL"
echo "Use esse email no --service-account do gcloud run deploy."

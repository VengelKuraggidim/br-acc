#!/bin/bash
# Cria o bucket `fiscal-cidadao-archival` (snapshots de provenance, prod)
# e da leitura pra SA da API. Escrita NAO vem dessa SA — pipelines ETL
# rodam com credencial separada com roles/storage.objectCreator (ver
# docs/deploy.md).
#
# Idempotente: falha silenciosa se o bucket ja existe (gcloud retorna
# exit 1 — a condicao abaixo captura isso).
#
# Pre-req: `gcloud auth login` + projeto setado.
set -euo pipefail

PROJECT_ID="fiscal-cidadao-493716"
REGION="southamerica-east1"
BUCKET="fiscal-cidadao-archival"
API_SA_EMAIL="fiscal-cidadao-api@${PROJECT_ID}.iam.gserviceaccount.com"

echo "==> Verificando/criando bucket gs://${BUCKET}..."
if gcloud storage buckets describe "gs://${BUCKET}" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "    bucket ja existe, pulando criacao."
else
  gcloud storage buckets create "gs://${BUCKET}" \
    --project="$PROJECT_ID" \
    --location="$REGION" \
    --uniform-bucket-level-access \
    --public-access-prevention
fi

echo "==> Concedendo objectViewer pra SA da API (${API_SA_EMAIL})..."
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${API_SA_EMAIL}" \
  --role="roles/storage.objectViewer" \
  --quiet

echo ""
echo "Bucket pronto: gs://${BUCKET} (regiao $REGION)"
echo "Pipelines ETL precisam de SA separada com roles/storage.objectCreator."
echo "Ver docs/deploy.md secao 'Archival ETL separado'."

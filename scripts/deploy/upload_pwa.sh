#!/bin/bash
# Publica os arquivos da PWA (pwa/*.html, sw.js, manifest.json) no
# bucket publico `fiscal-cidadao-pwa` — hospedagem estatica no GCS.
#
# Cache-Control ajustado:
# - index.html: no-cache (pra service worker pegar updates rapido)
# - sw.js: no-cache (mesmo motivo — SW precisa atualizar)
# - manifest.json: 1h
# - outros assets (se/quando tiver): 1 dia
#
# Idempotente: rerun sobrescreve. Bucket pode nao existir ainda (cria
# na primeira execucao).
#
# Pre-req: gcloud auth + projeto setado. API key do Transparencia e
# secrets NAO vao aqui — so arquivos estaticos.
#
# Uso:
#   bash scripts/deploy/upload_pwa.sh
#   # depois a PWA fica em:
#   #   https://storage.googleapis.com/fiscal-cidadao-pwa/index.html
set -euo pipefail

PROJECT_ID="fiscal-cidadao-493716"
REGION="southamerica-east1"
BUCKET="fiscal-cidadao-pwa"
PWA_DIR="$(cd "$(dirname "$0")/../../pwa" && pwd)"

echo "==> Verificando/criando bucket gs://${BUCKET}..."
if gcloud storage buckets describe "gs://${BUCKET}" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "    bucket ja existe."
else
  # public-access-prevention NAO setado — bucket precisa ser publico
  # pro web hosting funcionar. uniform-bucket-level-access continua on
  # (IAM controla acesso, sem ACLs por objeto).
  gcloud storage buckets create "gs://${BUCKET}" \
    --project="$PROJECT_ID" \
    --location="$REGION" \
    --uniform-bucket-level-access

  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --member="allUsers" \
    --role="roles/storage.objectViewer" \
    --quiet
fi

echo "==> Uploadando arquivos de ${PWA_DIR}..."

# index.html + sw.js: no-cache (updates tem que chegar rapido na PWA)
for f in index.html sw.js; do
  if [[ -f "${PWA_DIR}/${f}" ]]; then
    gcloud storage cp "${PWA_DIR}/${f}" "gs://${BUCKET}/${f}" \
      --cache-control="no-cache, max-age=0"
  fi
done

# manifest.json: cache curto
if [[ -f "${PWA_DIR}/manifest.json" ]]; then
  gcloud storage cp "${PWA_DIR}/manifest.json" "gs://${BUCKET}/manifest.json" \
    --cache-control="public, max-age=3600"
fi

echo ""
echo "PWA publicada em:"
echo "  https://storage.googleapis.com/${BUCKET}/index.html"
echo ""
echo "Se a PWA ja foi aberta antes, force-refresh (Ctrl+Shift+R) pra"
echo "invalidar o service worker antigo."

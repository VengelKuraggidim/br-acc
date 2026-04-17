#!/bin/bash
# =============================================================
# Fiscal Cidadão - Configurar HTTPS com Let's Encrypt
# Uso: sudo bash deploy/setup-ssl.sh seudominio.com
# =============================================================

set -euo pipefail

DOMAIN="${1:-}"
EMAIL="${2:-fernandoeq@live.com}"

if [ -z "$DOMAIN" ]; then
    echo "Uso: bash deploy/setup-ssl.sh seudominio.com [email]"
    echo "Exemplo: bash deploy/setup-ssl.sh fiscalcidadao.org fernandoeq@live.com"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "  Configurando HTTPS para: $DOMAIN"
echo "============================================"

# 1. Gerar nginx config com o domínio
echo "[1/3] Gerando configuração Nginx para $DOMAIN..."
sed "s/DOMAIN_PLACEHOLDER/$DOMAIN/g" \
    "$SCRIPT_DIR/nginx/nginx-ssl.conf" > "$SCRIPT_DIR/nginx/nginx.conf"

echo "Nginx configurado para $DOMAIN"

# 2. Reiniciar nginx para pegar a nova config
echo "[2/3] Reiniciando Nginx..."
cd "$PROJECT_DIR"
docker compose -f docker-compose.prod.yml restart nginx

# 3. Obter certificado
echo "[3/3] Obtendo certificado SSL..."
docker compose -f docker-compose.prod.yml run --rm certbot \
    certbot certonly --webroot \
    --webroot-path=/var/www/certbot \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email \
    -d "$DOMAIN"

# 4. Reiniciar nginx com HTTPS
docker compose -f docker-compose.prod.yml restart nginx

echo ""
echo "============================================"
echo "  HTTPS configurado com sucesso!"
echo "  Acesse: https://$DOMAIN"
echo "============================================"
echo ""
echo "O certificado sera renovado automaticamente."

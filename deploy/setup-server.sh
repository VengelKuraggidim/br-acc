#!/bin/bash
# =============================================================
# Fiscal Cidadão - Setup do servidor (Oracle Cloud Free Tier)
# Rodar como root: sudo bash deploy/setup-server.sh
# =============================================================

set -euo pipefail

echo "============================================"
echo "  Fiscal Cidadao - Setup do Servidor"
echo "============================================"

# --- 1. Atualizar sistema ---
echo "[1/5] Atualizando sistema..."
apt-get update -y
apt-get upgrade -y

# --- 2. Instalar Docker ---
echo "[2/5] Instalando Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
    # Adicionar usuario atual ao grupo docker
    usermod -aG docker ubuntu 2>/dev/null || usermod -aG docker $SUDO_USER 2>/dev/null || true
    echo "Docker instalado com sucesso!"
else
    echo "Docker ja instalado."
fi

# --- 3. Instalar Docker Compose (plugin) ---
echo "[3/5] Verificando Docker Compose..."
if ! docker compose version &> /dev/null; then
    apt-get install -y docker-compose-plugin
fi
echo "Docker Compose: $(docker compose version)"

# --- 4. Abrir portas no firewall (iptables Oracle) ---
echo "[4/5] Configurando firewall..."
# Oracle Cloud usa iptables internamente
iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT 2>/dev/null || true
iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT 2>/dev/null || true
netfilter-persistent save 2>/dev/null || iptables-save > /etc/iptables/rules.v4 2>/dev/null || true
echo "Portas 80 e 443 abertas."

# --- 5. Instalar ferramentas extras ---
echo "[5/5] Instalando ferramentas extras..."
apt-get install -y git htop

echo ""
echo "============================================"
echo "  Setup concluido!"
echo "============================================"
echo ""
echo "Proximos passos:"
echo "  1. Clone o repositorio ou copie os arquivos"
echo "  2. Copie deploy/.env.prod.example para .env e edite"
echo "  3. Rode: docker compose -f docker-compose.prod.yml up -d"
echo "  4. Para HTTPS, rode: bash deploy/setup-ssl.sh seudominio.com"
echo ""

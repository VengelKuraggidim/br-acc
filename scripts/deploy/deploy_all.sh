#!/bin/bash
# Wrapper de deploy — roda qualquer combinacao de PWA, API e grafo Aura.
#
# Uso:
#   bash scripts/deploy/deploy_all.sh --pwa           # so sobe o site estatico
#   bash scripts/deploy/deploy_all.sh --api           # build + deploy Cloud Run
#   bash scripts/deploy/deploy_all.sh --graph         # re-copia grafo local -> Aura
#   bash scripts/deploy/deploy_all.sh --all           # faz tudo na ordem certa
#
# Combina flags livremente (--pwa --api, --api --graph, etc).
#
# Pre-reqs (mantidos fora daqui pra esse script ser pequeno):
#   - gcloud autenticado (`gcloud auth login`) e projeto padrao em
#     fiscal-cidadao-493716 (`gcloud config set project fiscal-cidadao-493716`).
#   - docker rodando com o container `fiscal-neo4j` up (so pra --graph).
#   - `uv` instalado e etl/ com deps sincronizadas (`cd etl && uv sync`).
#
# Senhas:
#   - NEO4J_PASSWORD local: extraida do container fiscal-neo4j.
#   - Aura: baixada do Secret Manager pra /tmp/aura_pw, chmod 600,
#     apagada no final.
set -euo pipefail

# ----- cores no log (so se stdout e tty) -----
if [[ -t 1 ]]; then
  B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'
else
  B=""; G=""; Y=""; R=""; N=""
fi
log()  { echo -e "${B}==>${N} $*"; }
ok()   { echo -e "${G}✔${N} $*"; }
warn() { echo -e "${Y}!${N} $*"; }
err()  { echo -e "${R}✖${N} $*" >&2; }

# ----- parse args -----
DO_PWA=0; DO_API=0; DO_GRAPH=0
for arg in "$@"; do
  case "$arg" in
    --pwa) DO_PWA=1 ;;
    --api) DO_API=1 ;;
    --graph) DO_GRAPH=1 ;;
    --all) DO_PWA=1; DO_API=1; DO_GRAPH=1 ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *) err "flag desconhecida: $arg"; exit 2 ;;
  esac
done

if [[ $((DO_PWA + DO_API + DO_GRAPH)) -eq 0 ]]; then
  err "use --pwa, --api, --graph ou --all (roda $0 --help)."
  exit 2
fi

# ----- config fixa -----
PROJECT_ID="fiscal-cidadao-493716"
REGION="southamerica-east1"
AR_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/fiscal-cidadao/fiscal-cidadao-api"

# Aura (infos nao secretas)
AURA_URI="neo4j+s://5cb9f76f.databases.neo4j.io"
AURA_USER="5cb9f76f"
AURA_DB="5cb9f76f"

# Diretorio raiz do repo — resolve a partir do proprio script
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Ordem logica: graph antes (se API/PWA forem mudar contagens, o grafo
# precisa estar atualizado antes); API antes do PWA (se PWA comecar a
# depender de endpoint novo, API tem que ja estar servindo).

# =====================================================================
# --graph : re-copia grafo local -> Aura
# =====================================================================
if [[ $DO_GRAPH -eq 1 ]]; then
  log "Re-copiando grafo local -> Aura ($AURA_URI)"

  # Senha do Neo4j local
  LOCAL_PW_FILE="$(mktemp)"
  trap 'rm -f "$LOCAL_PW_FILE"' EXIT
  chmod 600 "$LOCAL_PW_FILE"
  if ! docker exec fiscal-neo4j env 2>/dev/null \
       | grep NEO4J_AUTH | cut -d/ -f2 > "$LOCAL_PW_FILE"; then
    err "nao foi possivel extrair senha do container fiscal-neo4j."
    err "verifique se ele esta up: docker compose up -d neo4j"
    exit 1
  fi

  # Senha do Aura (Secret Manager)
  AURA_PW_FILE="$(mktemp)"
  trap 'rm -f "$LOCAL_PW_FILE" "$AURA_PW_FILE"' EXIT
  chmod 600 "$AURA_PW_FILE"
  if ! gcloud secrets versions access latest \
       --secret=fiscal-cidadao-neo4j-password \
       --project="$PROJECT_ID" \
       --out-file="$AURA_PW_FILE" 2>/dev/null; then
    err "nao foi possivel ler secret fiscal-cidadao-neo4j-password."
    err "verifique: gcloud auth list && gcloud config get-value project"
    exit 1
  fi

  (cd "$REPO_ROOT/etl" && uv run python ../scripts/build_demo_graph.py \
    --source-password "$(cat "$LOCAL_PW_FILE")" \
    --target-uri "$AURA_URI" \
    --target-user "$AURA_USER" \
    --target-database "$AURA_DB" \
    --target-password "$(cat "$AURA_PW_FILE")" \
    --wipe-target)

  ok "grafo atualizado no Aura"
  warn "elementIds mudaram — links salvos podem ter quebrado"
fi

# =====================================================================
# --api : build no Cloud Build + deploy no Cloud Run
# =====================================================================
if [[ $DO_API -eq 1 ]]; then
  log "Buildando imagem da API (Cloud Build)"
  SHA="$(cd "$REPO_ROOT" && git rev-parse --short HEAD)"
  TAG="v${SHA}"
  log "  sha: $SHA  ->  $AR_IMAGE:$TAG"

  gcloud builds submit "$REPO_ROOT/api/" \
    --tag "$AR_IMAGE:$TAG" \
    --tag "$AR_IMAGE:latest" \
    --project="$PROJECT_ID"

  log "Deployando no Cloud Run"
  NEO4J_URI="$AURA_URI" \
  NEO4J_USER="$AURA_USER" \
  NEO4J_DATABASE="$AURA_DB" \
  bash "$REPO_ROOT/scripts/deploy/deploy_api.sh"

  ok "API atualizada"
fi

# =====================================================================
# --pwa : re-upload dos arquivos estaticos no bucket GCS
# =====================================================================
if [[ $DO_PWA -eq 1 ]]; then
  log "Uploadando PWA pro bucket"
  bash "$REPO_ROOT/scripts/deploy/upload_pwa.sh"
  ok "PWA atualizada — force-refresh (Ctrl+Shift+R) pra invalidar service worker"
fi

echo ""
ok "Deploy terminado."
echo ""
echo "URLs:"
echo "  Site: https://storage.googleapis.com/fiscal-cidadao-pwa/index.html"
echo "  API:  https://fiscal-cidadao-api-xfzjqhaisa-rj.a.run.app"

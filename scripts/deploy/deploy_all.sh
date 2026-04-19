#!/bin/bash
# Wrapper de deploy — roda qualquer combinacao de PWA, API e grafo Aura.
#
# Uso:
#   bash scripts/deploy/deploy_all.sh                 # auto-detecta o que mudou
#   bash scripts/deploy/deploy_all.sh --auto          # idem (explicito)
#   bash scripts/deploy/deploy_all.sh --pwa           # so sobe o site estatico
#   bash scripts/deploy/deploy_all.sh --api           # build + deploy Cloud Run
#   bash scripts/deploy/deploy_all.sh --graph         # re-copia grafo local -> Aura
#   bash scripts/deploy/deploy_all.sh --all           # faz tudo na ordem certa
#
# Combina flags livremente (--pwa --api, --api --graph, etc).
#
# Modo --auto (default): compara HEAD atual com os SHAs do ultimo
# deploy salvos em .last-deploy (gitignored) e liga as flags cujo
# path mudou (pwa/ -> --pwa; api/ -> --api). --graph sempre manual,
# pois depende do grafo LOCAL ter sido re-ingerido (mudanca em
# etl/src/ so significa que o codigo mudou, nao os dados).
#
# Pre-reqs:
#   - gcloud autenticado (`gcloud auth login`) e projeto padrao em
#     fiscal-cidadao-493716 (`gcloud config set project fiscal-cidadao-493716`).
#   - docker rodando com o container `fiscal-neo4j` up (so pra --graph).
#   - `uv` instalado e etl/ com deps sincronizadas (`cd etl && uv sync`).
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
DO_PWA=0; DO_API=0; DO_GRAPH=0; DO_AUTO=0
if [[ $# -eq 0 ]]; then
  DO_AUTO=1
fi
for arg in "$@"; do
  case "$arg" in
    --auto) DO_AUTO=1 ;;
    --pwa) DO_PWA=1 ;;
    --api) DO_API=1 ;;
    --graph) DO_GRAPH=1 ;;
    --all) DO_PWA=1; DO_API=1; DO_GRAPH=1 ;;
    -h|--help)
      sed -n '2,19p' "$0"
      exit 0
      ;;
    *) err "flag desconhecida: $arg"; exit 2 ;;
  esac
done

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
MARKER="$REPO_ROOT/.last-deploy"

# ----- helpers do marker -----
# Formato do .last-deploy (3 linhas, uma por componente):
#   pwa=<sha>
#   api=<sha>
#   graph=<sha>
# Sha vazio = nunca deployado.
get_marker() {
  local key="$1"
  [[ -f "$MARKER" ]] || { echo ""; return; }
  grep "^${key}=" "$MARKER" 2>/dev/null | cut -d= -f2 | head -1
}
set_marker() {
  local key="$1" sha="$2"
  local tmp; tmp="$(mktemp)"
  {
    echo "pwa=$([[ "$key" == "pwa" ]] && echo "$sha" || get_marker pwa)"
    echo "api=$([[ "$key" == "api" ]] && echo "$sha" || get_marker api)"
    echo "graph=$([[ "$key" == "graph" ]] && echo "$sha" || get_marker graph)"
  } > "$tmp"
  mv "$tmp" "$MARKER"
}

# =====================================================================
# --auto : detecta o que mudou via git diff
# =====================================================================
if [[ $DO_AUTO -eq 1 ]]; then
  log "Modo auto — detectando mudancas desde o ultimo deploy"
  HEAD_SHA="$(cd "$REPO_ROOT" && git rev-parse --short HEAD)"

  # Check working tree clean — se tem mudanca nao-commitada, git diff
  # contra o marker nao reflete o que vai ser uploadado. Avisar mas
  # nao bloquear (usuario pode estar testando deploy de pwa/ modificado).
  if ! (cd "$REPO_ROOT" && git diff --quiet HEAD) \
     || ! (cd "$REPO_ROOT" && git diff --cached --quiet); then
    warn "working tree tem mudancas nao-commitadas — auto-detect usa"
    warn "o estado do filesystem (incluindo as mudancas locais)"
  fi

  LAST_PWA="$(get_marker pwa)"
  LAST_API="$(get_marker api)"

  # PWA: se marker vazio (nunca deployou) OU se mudou algo em pwa/
  if [[ -z "$LAST_PWA" ]]; then
    warn "nenhum deploy anterior de pwa registrado — ativando --pwa"
    DO_PWA=1
  elif ! (cd "$REPO_ROOT" && git diff --quiet "$LAST_PWA" -- pwa/); then
    log "mudancas em pwa/ desde $LAST_PWA"
    DO_PWA=1
  fi

  # API: idem. Observar: mudancas em api/src/ sao o que importa
  # (Dockerfile, pyproject.toml, uv.lock tambem). Usar api/ inteiro
  # pra simplificar — .gcloudignore filtra caches/venv no upload.
  if [[ -z "$LAST_API" ]]; then
    warn "nenhum deploy anterior de api registrado — ativando --api"
    DO_API=1
  elif ! (cd "$REPO_ROOT" && git diff --quiet "$LAST_API" -- api/); then
    log "mudancas em api/ desde $LAST_API"
    DO_API=1
  fi

  if [[ $((DO_PWA + DO_API)) -eq 0 ]]; then
    ok "nada mudou desde o ultimo deploy (pwa@$LAST_PWA, api@$LAST_API)"
    warn "se voce re-rodou pipelines ETL, passe --graph explicito"
    exit 0
  fi

  log "auto selecionou: $([[ $DO_PWA -eq 1 ]] && echo --pwa) $([[ $DO_API -eq 1 ]] && echo --api)"
fi

# Sanity check final
if [[ $((DO_PWA + DO_API + DO_GRAPH)) -eq 0 ]]; then
  err "nada pra fazer. Use --auto, --pwa, --api, --graph ou --all"
  err "(rode: $0 --help)"
  exit 2
fi

HEAD_SHA="$(cd "$REPO_ROOT" && git rev-parse --short HEAD)"

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
  set_marker graph "$HEAD_SHA"
fi

# =====================================================================
# --api : build no Cloud Build + deploy no Cloud Run
# =====================================================================
if [[ $DO_API -eq 1 ]]; then
  log "Buildando imagem da API (Cloud Build)"
  TAG="v${HEAD_SHA}"
  log "  sha: $HEAD_SHA  ->  $AR_IMAGE:$TAG"

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
  set_marker api "$HEAD_SHA"
fi

# =====================================================================
# --pwa : re-upload dos arquivos estaticos no bucket GCS
# =====================================================================
if [[ $DO_PWA -eq 1 ]]; then
  log "Uploadando PWA pro bucket"
  bash "$REPO_ROOT/scripts/deploy/upload_pwa.sh"
  ok "PWA atualizada — force-refresh (Ctrl+Shift+R) pra invalidar service worker"
  set_marker pwa "$HEAD_SHA"
fi

echo ""
ok "Deploy terminado."
echo ""
echo "URLs:"
echo "  Site: https://storage.googleapis.com/fiscal-cidadao-pwa/index.html"
echo "  API:  https://fiscal-cidadao-api-xfzjqhaisa-rj.a.run.app"
echo ""
echo "Marker ($MARKER):"
[[ -f "$MARKER" ]] && cat "$MARKER" | sed 's/^/  /'

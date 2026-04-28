#!/usr/bin/env bash
# Backfill incremental de situacao cadastral / CNAE / porte nos :Company do
# grafo local via BrasilAPI. Rate limit gratuito da API: 500 req/dia.
#
# Uso:
#   scripts/backfill_brasilapi_cnpj.sh              # 1 batch (400 CNPJs) e sai
#   scripts/backfill_brasilapi_cnpj.sh --overnight  # repete ate bater quota ou
#                                                   # acabarem alvos elegiveis
#
# Cobertura: 20k+ doadores; em 1 batch/dia leva ~51 dias pra fechar 100%.
# Modo --overnight roda multiplos batches separados por 1h pra dar margem na
# quota diaria (500 - 400 = 100 reqs livres por batch). Pode deixar a noite
# toda — quando bater 429 ou esgotar elegiveis o script para sozinho.
#
# Idempotente: cache TTL de 7d em ``c.situacao_verified_at`` evita refetch
# do mesmo CNPJ no mesmo runout.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYTHON_BIN="${PYTHON_BIN:-$REPO_ROOT/.venv/bin/python}"
NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-changeme}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"
BATCH_SIZE="${BATCH_SIZE:-400}"
LOG_DIR="$REPO_ROOT/logs/brasilapi_cnpj"
mkdir -p "$LOG_DIR"

OVERNIGHT=0
if [[ "${1:-}" == "--overnight" ]]; then
  OVERNIGHT=1
fi

run_batch() {
  local stamp
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  local logfile="$LOG_DIR/$stamp.log"
  echo "[$stamp] running batch_size=$BATCH_SIZE -> $logfile"
  "$PYTHON_BIN" -m bracc_etl.runner run \
    --source brasilapi_cnpj_status \
    --neo4j-uri "$NEO4J_URI" \
    --neo4j-user "$NEO4J_USER" \
    --neo4j-password "$NEO4J_PASSWORD" \
    --neo4j-database "$NEO4J_DATABASE" \
    --data-dir ./data \
    --batch-size "$BATCH_SIZE" 2>&1 | tee "$logfile"

  if grep -q "rate limit hit (429)" "$logfile"; then
    return 2
  fi
  if grep -q "nenhum CNPJ elegivel" "$logfile"; then
    return 3
  fi
  return 0
}

if [[ "$OVERNIGHT" == "1" ]]; then
  # Loop overnight: roda batch, espera 1h, repete. Para se 429, alvos
  # esgotaram, ou Ctrl-C. 1h de espera deixa janela pra rodar 24 batches
  # = 9.600 CNPJs em 24h, mas a BrasilAPI gratuita corta em 500/dia, entao
  # na pratica a 2a tentativa do dia ja vai bater 429 e o loop encerra.
  echo "Modo overnight: rodando batches ate 429 ou alvos esgotarem."
  while true; do
    set +e
    run_batch
    rc=$?
    set -e
    case "$rc" in
      0) echo "Batch ok. Esperando 1h pra proxima tentativa..." ;;
      2) echo "BrasilAPI 429 — quota diaria atingida. Encerrando."; exit 0 ;;
      3) echo "Sem alvos elegiveis (todos verificados < 7d)."; exit 0 ;;
      *) echo "Batch falhou com rc=$rc. Encerrando."; exit "$rc" ;;
    esac
    sleep 3600
  done
else
  run_batch
fi

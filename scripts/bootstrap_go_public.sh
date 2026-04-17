#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "bootstrap-go runs full historical ingestion for Goias-scoped sources only."
echo "Contract: config/bootstrap_go_contract.yml (subset mode)."

exec python3 "${REPO_ROOT}/scripts/run_bootstrap_all.py" \
  --repo-root "${REPO_ROOT}" \
  --contract-path "config/bootstrap_go_contract.yml" \
  --compose-file "docker-compose.yml" \
  --stack-services "neo4j bracc-api" \
  --neo4j-container "fiscal-neo4j" \
  --output-label "bootstrap-go" \
  "$@"

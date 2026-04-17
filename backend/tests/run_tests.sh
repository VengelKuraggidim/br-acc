#!/usr/bin/env bash
# =============================================================================
# Script de testes do PlanoIA Backend
#
# Uso:
#   bash tests/run_tests.sh              # Testes unitarios (rapido, sem rede)
#   bash tests/run_tests.sh --all        # Unitarios + integracao (precisa backend rodando)
#   bash tests/run_tests.sh --integracao # Somente integracao
#   bash tests/run_tests.sh --verbose    # Com output detalhado
#
# Rode de dentro de backend/:  cd backend && bash tests/run_tests.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BACKEND_DIR"

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

MODE="unit"
VERBOSE=""

for arg in "$@"; do
    case $arg in
        --all)       MODE="all" ;;
        --integracao) MODE="integracao" ;;
        --verbose|-v) VERBOSE="-v" ;;
    esac
done

echo -e "${CYAN}=== PlanoIA Backend Tests ===${NC}"
echo -e "Modo: ${YELLOW}${MODE}${NC}"
echo ""

FAILED=0

run_suite() {
    local name="$1"
    local pattern="$2"
    echo -e "${CYAN}--- ${name} ---${NC}"
    if python3 -m pytest "$pattern" $VERBOSE -x --tb=short 2>&1; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FALHOU${NC}"
        FAILED=1
    fi
    echo ""
}

if [ "$MODE" = "unit" ] || [ "$MODE" = "all" ]; then
    run_suite "Analise (traducoes, alertas, anomalias)" "tests/test_analise.py"
    run_suite "APIs Externas (mocks, agrupamento, conversao)" "tests/test_apis_externas.py"
    run_suite "App (helpers, modelos, alertas)" "tests/test_app.py"
fi

if [ "$MODE" = "integracao" ] || [ "$MODE" = "all" ]; then
    echo -e "${YELLOW}Verificando se backend esta rodando...${NC}"
    if curl -s http://localhost:8001/status > /dev/null 2>&1; then
        echo -e "${GREEN}Backend OK${NC}"
    else
        echo -e "${RED}Backend NAO esta rodando em localhost:8001${NC}"
        echo "Inicie com: cd backend && python3 -m uvicorn app:app --port 8001"
        if [ "$MODE" = "integracao" ]; then
            exit 1
        fi
    fi
    echo ""
    run_suite "Integracao (endpoints reais + API Camara)" "tests/test_integracao.py"
fi

echo -e "${CYAN}=== Resultado ===${NC}"
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}Todos os testes passaram!${NC}"
else
    echo -e "${RED}Alguns testes falharam.${NC}"
    exit 1
fi

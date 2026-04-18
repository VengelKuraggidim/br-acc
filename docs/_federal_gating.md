# Federal-scope gating (`api/src/bracc/_federal/`)

Fiscal Cidadão serves Goiás only today. Code that was written for a
national (multi-state / federal) scope lives in
`api/src/bracc/_federal/` and is **not** mounted by the FastAPI app at
startup. Nothing is deleted — the mission is expansive and this code is
expected to be reactivated when/if the product scope grows.

## What is gated

| Path | Role |
|------|------|
| `api/src/bracc/_federal/routers/patterns.py` | `/api/v1/patterns/*` — engine de detecção de padrões federais (depende do módulo `pattern_service`, ausente desta árvore pública) |
| `api/src/bracc/_federal/routers/public.py` | `/api/v1/public/*` — "World Transparency Graph" endpoints de escopo nacional |
| `api/src/bracc/_federal/queries/public_company_lookup.cypher` | lookup público por CNPJ (usado só pelos routers acima) |
| `api/src/bracc/_federal/queries/public_graph_company.cypher` | grafo público da empresa |
| `api/src/bracc/_federal/queries/public_patterns_company.cypher` | padrões públicos por empresa |
| `api/tests/_federal/test_public_routes.py` | testes dos endpoints `/api/v1/public/*` acima |

O que **não** está gated (deliberadamente):

- `services/intelligence_provider.py` — a classe `CommunityIntelligenceProvider`
  é chamada por `routers/entity.py` (GO-relevante) via `.get_entity_exposure()`.
  Está INFRA_TRANSVERSAL na prática, apesar do relatório de auditoria
  marcar o arquivo inteiro como FEDERAL_DEAD_WEIGHT. `FullIntelligenceProvider`
  já se auto-gates via `_full_modules_available()` (o módulo
  `pattern_service` não existe na árvore pública, então `get_default_provider()`
  cai pro tier `community` automaticamente).
- `models/pattern.py` — importado por `intelligence_provider.py`.
- `queries/public_pattern_*.cypher` (8 arquivos) — referenciados por
  `CommunityIntelligenceProvider.run_pattern`. Dormentes enquanto
  o runtime padrão não monta routers que chamam `run_pattern`, mas
  ficam na pasta principal para o catálogo de testes continuar válido.
- `routers/meta.py` — tem endpoints mistos (GO + federal). Fatiar
  requer cirurgia fora do escopo desta rodada; permanece intacto.

## Como reativar

No ambiente de runtime:

```bash
# 1. Setar a flag (default é false).
export ENABLE_FEDERAL_ROUTES=true

# 2. Subir a API normalmente.
make api
# ou: cd api && uv run uvicorn bracc.main:app --reload

# 3. (Opcional) Instalar extras exclusivos federais quando existirem.
#    Hoje nenhum dep do pyproject é exclusivamente federal; este passo
#    passa a ser necessário quando houver um `[federal]` em
#    api/pyproject.toml::[project.optional-dependencies].
cd api && uv sync --extra federal
```

Quando a flag está on, `main.py` importa `bracc._federal.routers.patterns`
e `bracc._federal.routers.public` e os monta no `app`. Os endpoints
`/api/v1/patterns/*` e `/api/v1/public/*` voltam a responder.

## Como rodar os testes gated

Os testes abaixo de `api/tests/_federal/` são marcados automaticamente
com `@pytest.mark.federal` (via `api/tests/_federal/conftest.py`). O
`addopts` em `api/pyproject.toml` exclui `federal` por default, então
`make test-api` só roda o runtime GO/INFRA.

```bash
# Roda só os testes gated com a flag de runtime ligada.
make test-api-federal
```

## Por que gate em vez de delete

Decisão do dono do produto (2026-04-18): "pode comentar e não deletar
os arquivos federais, futuramente tudo pode entrar." A missão de
fiscalização é expansiva — começa por Goiás mas o caminho natural é
expansão. Deletar código funcional só para revertê-lo depois é
desperdício e perda de histórico útil. O gate resolve o objetivo
imediato (runtime enxuto, só GO) sem fechar portas para o futuro.

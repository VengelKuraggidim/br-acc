# Servidores estaduais — perfil simples OU tirar do clicável da busca

## Contexto
A busca devolve 165k `:StateEmployee` quando o usuário pesquisa nome comum. Hoje o item aparece no resultado **e o card é clicável** (`abrirPerfil`), mas o `perfil_service` rejeita com `EntityNotFoundError` porque `:StateEmployee` não está em `_POLITICIAN_LABELS`. Resultado: o cidadão clica no servidor → tela de erro 404. Pior UX que se nem fosse clicável.

## Duas saídas

### A. Não-clicável (mínimo viável, ~5min)
- `pwa/index.html::TIPOS_PERFIL` — não inclui `state_employee`/`stateemployee`.
- Já é o estado atual em código (confirmar). Se ainda for clicável, basta remover o `onclick` do branch `state_employee`. Card mostra cargo+salário+lotação na busca e pronto.

### B. Perfil próprio leve (1 dia)
Servidor estadual tem dados úteis — só não cabem no `PerfilPolitico` (que assume "tem mandato"). Criar:
- Endpoint novo `/servidor/{entity_id}` com shape `PerfilServidor`: nome, cargo, lotação, salário bruto, comissionado (sim/não), atos do DOU relacionados, vínculos com empresas (se sócio).
- Página dedicada no PWA `pageServidor` (similar ao `pagePerfil`).
- Reaproveita `DOUAct` (`PUBLICOU` rel) — nomeação/exoneração com data e link.

Comissionados (164k de 165k são `is_commissioned=true`) são especialmente sensíveis — quem indicou? Por isso o perfil do servidor é útil mesmo sem ele ter mandato.

## Recomendação
Fazer **A agora** (corrige o erro 404) e **B depois** quando tiver capacidade. A é fix de 1 linha; B vale a pena mas é escopo separado.

## Onde mexer (B)
- `api/src/bracc/services/servidor_service.py` (novo)
- `api/src/bracc/routers/pwa_parity.py` — novo endpoint paralelo a `/politico/{entity_id}`
- `api/src/bracc/models/perfil_servidor.py` (novo)
- `api/src/bracc/queries/perfil_servidor.cypher` (novo)
- `pwa/index.html` — `abrirServidor()`, `renderServidor()`, página `pageServidor`

## Esforço
A: trivial. B: 1-2 dias.

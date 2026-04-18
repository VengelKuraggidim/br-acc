# Validar se o CNPJ doador/sócio está ATIVO na Receita Federal — ✅ CONCLUÍDO (2026-04-18)

> Pipeline `brasilapi_cnpj_status` (commit `4d5808f`) ingere situação cadastral
> via BrasilAPI com archival. Propagado em ConexoesService + alerta grave para
> CNPJs BAIXADA/SUSPENSA/INAPTA (commits `8ddece3`, `8628f86`).

## Contexto
Hoje o perfil de um político mostra CNPJs que doaram pra campanha ou nos quais ele aparece como sócio — sem verificar **se essas empresas ainda existem**. Uma empresa BAIXADA, SUSPENSA ou INAPTA doando pra campanha é um sinal vermelho relevante (pode indicar laranja, caixa 2, fraude). Idem pra sócio: político sócio de empresa BAIXADA é menos suspeito que de empresa ativa com contratos públicos, mas ainda é dado útil.

Fonte: `status_cadastral` da Receita Federal (valores: ATIVA / BAIXADA / SUSPENSA / INAPTA / NULA).

## Arquivos relevantes
- `pwa/index.html` — cards "Quem financiou a campanha" e "Empresas em que é sócio(a)" — adicionar badge visual de status
- `backend/app.py` — `PerfilPolitico` model + loop de classificação (adicionar campo `situacao_cadastral` no `DoadorCNPJ` e `SocioConectado`)
- `api/src/bracc/queries/entity_connections.cypher` — já retorna o Company node; precisa expor `c.situacao_cadastral` nos props

## Fonte dos dados (2 caminhos)
1. **Bulk via Receita (depende de TODO 07)**: se TODO 07 foi feita e `c.situacao_cadastral` já está no grafo, esta tarefa vira só de UI (mostrar o badge). Baratíssima.
2. **Sob demanda via BrasilAPI**: https://brasilapi.com.br/api/cnpj/v1/{cnpj} — retorna situação cadastral em tempo real, rate limit 500/dia gratuito.
   - Só faz sentido pros top N doadores/sócios do político aberto (não pra todos os 52k do grafo).
   - Cache local por 7 dias pra não estourar o rate limit.

## Missão
1. **Se TODO 07 foi feita**: só mostrar o badge no PWA.
   - Verde ✅ "Ativa" (default, talvez nem mostrar).
   - Amarelo ⚠️ "Suspensa" ou "Inapta".
   - Vermelho 🚩 "Baixada" — acompanhado de alerta explicativo.
2. **Se TODO 07 não foi feita (fallback por API)**:
   - Adicionar endpoint `/cnpj/{cnpj}/status` no backend que consulta BrasilAPI (cache 7d em SQLite/Redis).
   - No PWA, ao expandir a seção de CNPJs, carregar status dos top 10 por valor doado.
   - Mostrar spinner → resultado.
   - Rate limit friendly: batching + throttle client-side.

## Critérios de aceite
- CNPJ baixado aparece visualmente diferente (badge vermelho) no perfil do político.
- Alerta textual no topo do card quando há 1+ doador baixado: "⚠️ Uma ou mais empresas doadoras estão baixadas/suspensas na Receita."
- Cache evita bater BrasilAPI repetidamente pro mesmo CNPJ.

## Guardrails
- BrasilAPI é gratuita mas tem rate limit. Respeitá-lo (throttle 2 req/s máximo).
- Se a chamada falhar/timeout: degradar silenciosamente, não quebrar o perfil.
- Cache com TTL 7d em disco (empresa baixada não volta a ser ativa; ativa raramente é baixada).
- `make pre-commit` verde.

## Dependência
- **Ideal depois da TODO 07**: se o backfill Receita rodou, esta tarefa vira só UI (1-2h).
- **Sem TODO 07**: cai no caminho BrasilAPI (viável só pros top doadores, ~3-4h pra infra de cache + UI).

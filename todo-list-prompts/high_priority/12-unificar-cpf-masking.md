# Unificar CPF masking — ✅ CONCLUÍDO (2026-04-18)

# Unificar CPF masking entre FormatacaoService e middleware

## Contexto

Hoje existem dois helpers de máscara CPF no `api/`:

- `bracc.services.formatacao_service.mascarar_cpf(cpf)` — retorna `***.***.***-44`
  (só últimos 2 dígitos). Usado pelo ConexoesService + PerfilService em código
  aplicação. Mais restritivo / LGPD-first.

- `bracc.middleware.cpf_masking.mask_formatted_cpf(cpf)` — retorna
  `***.***.789-00` (últimos 4+2 = 5 dígitos). Usado pelo middleware global
  como defesa-em-profundidade caso algum CPF escape do code-path explícito.
  Formato mais antigo, menos restritivo.

Sinalizado em 2026-04-18 pelo agent da Fase 04.A durante a consolidação do
FastAPI.

## Risco / valor

Risco: zero em runtime (os dois contextos não conflitam hoje). Valor:
consistência de UX (usuário vê dois formatos dependendo de onde o CPF
veio) + simplificação de auditoria LGPD.

## Missão

1. Decidir qual formato é o canônico (sugestão: `mascarar_cpf` do
   FormatacaoService — mais conservador).
2. Fazer o middleware delegar pro service: substitui
   `mask_formatted_cpf` por wrapper que chama `mascarar_cpf`.
3. Atualizar tests do middleware que esperam o formato antigo
   (`***.***.789-00`) pro novo (`***.***.***-00`).
4. Rodar `make test-api` verde.

## Tempo estimado

30-60 min. Refactor pequeno, bem localizado.

## Guardrails

- Não mudar o formato produzido por `mascarar_cpf` — é o alvo.
- Garantir que tests do middleware passam com o novo formato.
- `mask_raw_cpf` (masking de CPF de 11 dígitos sem pontuação) pode ser
  mantido como alias do service (decidir ao implementar).

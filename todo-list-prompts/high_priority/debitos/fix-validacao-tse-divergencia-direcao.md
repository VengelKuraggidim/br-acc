# Fix: divergência TSE ignora direção e mostra mensagem errada

## Contexto

Sessão 2026-04-19. Screenshot de um card "Confere com o TSE?" mostrava:

- Declarado ao TSE (2022): **R$ 4,66 mi**
- Ingerido no sistema: **R$ 14,05 mi**
- Divergência grande de **201,6%**

Mensagem exibida abaixo: *"algumas doações podem não ter sido carregadas
no nosso banco (especialmente repasses do fundo partidário). O valor
oficial é o do TSE."*

Essa mensagem **só faz sentido quando ingerido < declarado** (gap de
ingestão). No caso do screenshot é o oposto — ingerido está **3x maior**
que o declarado, ou seja, temos duplicação, não gap.

## Root cause (código)

1. `api/src/bracc/services/validacao_tse_service.py:42-43` computa:

   ```python
   div = declarado - ingerido
   pct = (abs(div) / declarado * 100) if declarado > 0 else 0.0
   ```

   Usa `abs(div)` → perde a direção. `status` é só `ok`/`atencao`/`divergente`,
   sem distinguir entre "faltou ingerir" e "ingerimos demais".

2. `pwa/index.html:2273-2277` tem **uma única mensagem** condicional ao
   `status !== 'ok'`, assumindo sempre a direção "faltou ingerir".

## Fix proposto

### Service

- Adicionar campo `direcao: Literal["gap_ingestao", "excesso_ingestao"]`
  no model `ValidacaoTSE` (`api/src/bracc/models/perfil.py`).
- Calcular em `gerar_validacao_tse`:

  ```python
  direcao = "gap_ingestao" if declarado > ingerido else "excesso_ingestao"
  ```

- Preservar sinal em `divergencia_valor` (remover o `abs`), mas manter
  `divergencia_valor_fmt` com `abs()` pra display.

### PWA

Duas mensagens distintas (neutras, sem rotular causa):

**`direcao === 'gap_ingestao'`** (ingerido < declarado):
> Nosso banco ingeriu menos doações que o TSE declarou. Alguns repasses
> (fundo partidário, FEFC) podem não ter sido carregados. O valor oficial
> é o do TSE — verifique a fonte abaixo.

**`direcao === 'excesso_ingestao'`** (ingerido > declarado):
> Estranho: nosso sistema mostra mais doações agregadas que o TSE
> declarou. Como nossa fonte primária é o próprio TSE, isso provavelmente
> indica duplicação na nossa agregação (o débito
> `investigar-duplicacao-doacoes-tse.md` está aberto). Verifique as
> fontes oficiais abaixo — não estamos acusando ninguém.

Ambas as mensagens devem incluir link pra `divulgacandcontas.tse.jus.br`
(página de prestação de contas do candidato).

### Tests

- `api/tests/unit/test_validacao_tse_service.py` — adicionar casos pros
  dois valores de `direcao` e garantir que `divergencia_valor` preserva
  sinal.

## Referências

- `api/src/bracc/services/validacao_tse_service.py:42-43`
- `api/src/bracc/models/perfil.py` (`ValidacaoTSE`)
- `pwa/index.html:2273-2277`
- `api/tests/unit/test_validacao_tse_service.py`

## Prioridade

**Alta** — a mensagem atual induz o usuário ao erro quando ingerido >
declarado. Como bug de UX + violação de clareza de proveniência, entra
no critério de "não quebrar o contrato com o cidadão".

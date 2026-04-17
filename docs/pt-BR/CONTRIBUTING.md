# Contribuindo com o Fiscal Cidadão

Idioma: [English](../../CONTRIBUTING.md) | **Português (Brasil)**

Obrigado por contribuir com o Fiscal Cidadão — fork de [`brunoclz/br-acc`](https://github.com/brunoclz/br-acc) (AGPL v3) reescopado para Goiás.

> **Nota sobre nomenclatura.** Contextos user-facing usam o nome "Fiscal Cidadão". Pacotes Python internos (`bracc`, `bracc_etl`), o entry point da CLI `bracc-etl` e import paths do upstream permanecem inalterados. Ao adicionar código, continue usando os identificadores `bracc` / `bracc_etl`; ajuste apenas strings visíveis (copy da UI, títulos de docs, cabeçalhos de página) para a nova marca.

## Regras Gerais

- Mantenha as mudanças alinhadas ao objetivo de transparência de interesse público.
- Não adicione segredos, credenciais ou detalhes de infraestrutura privada.
- Respeite defaults públicos de segurança, privacidade e compliance.

## Setup de Desenvolvimento

```bash
cd api && uv sync --dev
cd ../etl && uv sync --dev
cd ../frontend && npm install
```

## Checagens de Qualidade

Execute antes de abrir PR:

```bash
make pre-commit
```

`pre-commit` agrupa tudo o que o CI cobra em cada PR — lint,
type-check, testes unitários, auditoria de neutralidade e
governança do registro/docs — pra evitar surpresa de verde-local /
vermelho-CI.

Alvos individuais também estão disponíveis: `make check` (lint +
type + testes apenas), `make neutrality`, `make check-public-claims`,
`make check-pipeline-contracts`, `make check-pipeline-inputs`.

## Expectativas para Pull Request

- Mantenha o escopo da PR focado e explique o impacto para usuário.
- Inclua testes para mudanças de comportamento.
- Atualize documentação quando interfaces ou fluxos mudarem.
- Garanta todos os checks obrigatórios verdes no CI.

## Contribuições com Assistência de IA

Contribuições com assistência de IA são permitidas.  
Contribuidores humanos continuam responsáveis por:

- correção técnica,
- conformidade de segurança e privacidade,
- revisão final e aprovação antes do merge.

# Conectar Expulsion (CEAF) ↔ Person GO — 95 perfis com demissão por punição

## Contexto

Auditoria 2026-05-02: dos 4.066 nodes `:Expulsion` (CEAF — Cadastro de Expulsões da Administração Federal, fonte `ceaf`), **131 expulsões batem por CPF parcial com 95 Person GO**. Cada Expulsion tem `cpf` mascarado, `name`, `position`, `punishment_type` ("Demissão", "Cassação de aposentadoria"), `decree`, `date`. Hoje 100% dos 4k são órfãos.

Uso: selo "**Expulsão CEAF — demitido em DD/MM/YYYY por X**" no perfil de Person GO que tenha histórico de cargo federal. Forte sinal de risco em concursos, indicações políticas e candidaturas.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/cgu_ceaf.py` (ou nome similar)

## Missão

1. Criar rel `(p:Person)-[:EXPULSA_CEAF]->(ex:Expulsion)` por match de CPF parcial (3+3 dígitos visíveis em `***.NNN.NNN-**`).
2. **Reforçar match com nome** antes de criar rel: comparar `ex.name` com `p.name` por similaridade (Jaro-Winkler ou Levenshtein normalizado >0.85). Sem confirmação por nome, **não criar rel** — match só por CPF parcial tem ~1/1.000.000 colisão mas com 4k×45k pares isso vira ~180 falsos positivos esperados.
3. Adicionar selo "Demissão CEAF" no perfil; mostrar com tooltip o decreto, posição, data.

## Critérios de aceite

- 60-95 Person GO com rel `:EXPULSA_CEAF` (após filtro por nome).
- Zero falso positivo confirmado em revisão manual de todos os matches (são poucos).
- `make pre-commit` verde.

## Guardrails

- **Reputacional**: selo "demitida por improbidade" em pessoa errada é gravíssimo. Confirmação por nome é obrigatória.
- LGPD: dado é público (CGU CEAF), mas exibição precisa estar correta.
- Não publicar CPF completo derivado.

## Dependência

Independente.

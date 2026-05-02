# Fix loader CampaignDonation TSE — 119k doações GO órfãs (provável raiz do bug Amilton)

## Contexto

Auditoria do grafo (2026-05-02) descobriu que **TODOS os 118.727 nodes `:CampaignDonation` estão órfãos** (zero relationships) e **todos têm `uf=GO`**. O loader `tse_prestacao_contas_go` cria os nodes com `doador_id` (CNPJ se PJ, CPF se PF) e `candidato_cpf`, mas falha em criar a aresta `Person -[:DOOU]-> Person` ou `Company -[:DOOU]-> Person`.

Isso provavelmente é a raiz do bug "validacao_tse — excesso de ingestão" (Amilton saiu de R$ 0 → R$ 843k vs. R$ 421,5k declarado): doações são contadas tanto na rel `:DOOU` (caminho normal) quanto via `:CampaignDonation` órfã (que pode estar sendo somada por outro caminho), ou rels duplicadas em re-runs.

Esses 119k registros são valiosos: representam toda a prestação de contas eleitorais de candidatos GO 2022/2024. Não devem ser apagados, devem ser **reconectados**.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py` — loader (linha ~525 já tem fix de format_cnpj documentado em TODO 07)
- `etl/src/bracc_etl/pipelines/tse_donations.py` ou similar — onde a rel `:DOOU` é criada
- Memória: `project_tse_donation_id_idempotente.md`, `project_validacao_tse_excesso_ingestao.md`, `project_cnpj_format_canon_grafo.md`

## Missão

1. Investigar por que `tse_prestacao_contas_go` cria `:CampaignDonation` mas não cria a `:DOOU` correspondente. Comparar com o loader que cria `:DOOU` legítimo (que tem 1.77M rels).
2. Decidir: o `:CampaignDonation` é label paralelo desnecessário (só `:DOOU` basta) ou é um label intencional pra carregar metadados extras (origem_receita, bucket, etc)?
3. Se desnecessário: dropar o label `:CampaignDonation`, garantir que toda a info já está em `r:DOOU` props.
4. Se intencional: criar a rel `(donor)-[:GEROU]->(cd:CampaignDonation)-[:RECEBIDA_POR]->(person)` ou similar para conectar.
5. Verificar se isso resolve o caso Amilton (R$ 421,5k esperado).
6. Re-rodar `validacao_tse` e confirmar valores condizem com TSE.

## Critérios de aceite

- Zero `:CampaignDonation` órfãos depois do fix.
- Caso Amilton: valor total de doações recebidas == valor declarado no TSE (±1%).
- Re-run idempotente (não duplica rels nem nodes).
- `make pre-commit` verde.

## Guardrails

- **Backup do Neo4j local antes** (export de Person GO + rels :DOOU + :CampaignDonation).
- Não tocar o Aura (congelado por memória `project_aura_adiado_sem_grana`).
- Validar contra TSE Prestação de Contas oficial pelo menos 5 candidatos (Amilton, Zeli, Vanderlan, Wilder, Caiado).

## Dependência

Independente. Pré-requisito pra fechar `project_validacao_tse_excesso_ingestao.md`.

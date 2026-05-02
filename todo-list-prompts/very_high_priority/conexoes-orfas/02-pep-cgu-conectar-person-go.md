# Conectar PEPRecord (CGU) ↔ Person GO — selo PEP em 4.088 perfis

## Contexto

Auditoria 2026-05-02: dos 133.877 nodes `:PEPRecord` (CGU PEP, fonte `cgu_pep`), **134k são 100% órfãos** (zero rels). Mas o cruzamento por CPF parcial (`***.NNN.NNN-**`) com `Person {uf:'GO'}` retorna **4.614 PEPRecords casando com 4.088 Person GO** — basicamente todo vereador, prefeito, secretário e cargo comissionado GO atual.

Cada PEPRecord tem: `cpf` (mascarado 3+3 dígitos), `name`, `org` ("GOIÂNIA-GO", "JATAÍ-GO", etc), `role`/`role_description` ("VEREAD"/"VEREADOR", "DIR. SUPERINTENDENTE"), `start_date`, `end_date`, `grace_end_date` (período de carência LGPD/PEP).

Use case novo no app: **selo "PEP — cargo X em órgão Y, mandato Z–W, carência até T"** no perfil de qualquer político GO. É informação **regulatória obrigatória** (BCB Resolução 4.595, Lei 9.613/1998 antilavagem) que o app não mostra hoje.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/cgu_pep.py` (ou onde o loader vive)
- `api/src/bracc/queries/perfil.cypher` ou similar — onde o perfil do político é montado
- `pwa/index.html` — card do perfil

## Missão

1. Descobrir onde o loader `cgu_pep` cria PEPRecord e por que não cria a rel.
2. Criar relationship `(p:Person)-[:CLASSIFICADA_PEP]->(pep:PEPRecord)` quando dígitos parciais batem.
3. Estratégia de matching:
   - Pegar `pep.cpf` formato `***.NNN.NNN-**` → extrair pos 4-6 e 8-10.
   - Pegar `p.cpf` formato `XXX.YYY.ZZZ-WW` → comparar pos 3-5 e 6-8 (dígitos completos no meio).
   - **Cuidado com falsos positivos**: 6 dígitos têm ~1/1.000.000 colisão; reforçar com match de nome (`pep.name` é truncado em 60 chars, comparar prefixo) E `org` contendo `-GO`.
4. Adicionar etiqueta "PEP" no card do perfil no PWA com tooltip mostrando órgão, cargo, datas, carência.

## Critérios de aceite

- ≥4.000 Person GO com rel `:CLASSIFICADA_PEP` para pelo menos um PEPRecord.
- Zero falso positivo confirmado em amostra de 50 perfis revisados manualmente.
- PWA mostra selo no perfil (com tooltip explicativo: "PEP — exposição política, ver Lei 9.613").
- `make pre-commit` verde.

## Guardrails

- LGPD: PEP é dado público (CGU), pode exibir.
- CPF mascarado é o que o CGU divulga; **não publicar CPF completo derivado** mesmo que match seja exato.
- Re-run idempotente (MERGE na rel, não CREATE).

## Dependência

Independente.

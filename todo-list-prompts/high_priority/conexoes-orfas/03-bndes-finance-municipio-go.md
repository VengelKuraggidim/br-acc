# Aba "BNDES no município" — 202 contratos GO = R$ 9,6 bi

## Contexto

Auditoria 2026-05-02: dos 1.015.332 nodes `:Finance` (BNDES, fonte `bndes`), apenas **202 contratos têm `uf='GO'` ou `municipio` em município GO, totalizando R$ 9.636.204.502** (R$ 9,6 bi). O resto (~1M) é BNDES nacional não conectado a nenhum político GO via cadeia de doação Company→Person GO (apenas 233 contratos cruzam via doação).

Esses 202 contratos GO **não aparecem no app hoje** porque o `:Finance` está conectado só a `:Company` (rel `:DEVE`), e a Company tomadora do empréstimo raramente é doadora de campanha. Mas o dado é altíssimo valor jornalístico no perfil do município/prefeito: "Município X recebeu R$ Y bi do BNDES nos últimos N anos para projetos Z."

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/bndes.py`
- `api/src/bracc/queries/municipio.cypher` ou similar
- `pwa/index.html` — perfil do município/prefeito

## Missão

1. Confirmar que `:Finance.municipio` está populado para todos os 202 GO (já confirmado em audit).
2. Criar rel `(gm:GoMunicipality)-[:RECEBEU_BNDES]->(f:Finance)` ligando município GO ao contrato BNDES.
3. Adicionar aba "BNDES" ao perfil do município no PWA: lista de contratos com data, valor, tomador (Company), descrição do projeto, status (LIQUIDADO/EM ANDAMENTO).
4. No perfil de prefeito/vice, agregar: "Durante seu mandato (Y–Z), município recebeu R$ X em BNDES (N contratos)."

## Critérios de aceite

- 202 rels `:RECEBEU_BNDES` criadas em município GO.
- Aba BNDES visível no perfil de pelo menos 5 municípios GO grandes (Goiânia, Aparecida, Anápolis, Rio Verde, Luziânia).
- Soma agregada por mandato bate com soma manual.
- `make pre-commit` verde.

## Guardrails

- Cortar os ~1M Finance não-GO **só depois** desse fix — pode haver alguns sub-cruzamentos não vistos (ex: empresa GO com matriz fora). Marcar como Phase 2 separada.
- Match `municipio` ↔ `GoMunicipality.name` precisa normalizar acentos/maiúsculas.

## Dependência

Independente. Aproveita os 247 nodes `:GoMunicipality` já existentes.

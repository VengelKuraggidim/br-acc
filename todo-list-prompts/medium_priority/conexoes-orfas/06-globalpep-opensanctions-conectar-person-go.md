# Conectar GlobalPEP (OpenSanctions) ↔ Person GO — 1.130 matches por nome

## Contexto

Auditoria 2026-05-02: dos 21.648 nodes `:GlobalPEP` (OpenSanctions, dataset `br_pep`), **1.130 batem por nome exato com Person GO** (1.827 Person GO afetados). Os GlobalPEP são 100% órfãos hoje. Diferente do PEPRecord CGU, **GlobalPEP não tem CPF** (campo `cpf=""`) — só `name`, `original_name`, `country=br`.

Uso: complementar o selo PEP CGU com classificação OpenSanctions ("PEP global - exposição internacional"). Útil pra due diligence em compliance financeiro internacional. Inclui ex-políticos antigos (ex: IRAPUAN COSTA JUNIOR, ex-governador GO).

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/opensanctions.py`
- Memória: `reference_senado_sem_cpf_publico.md` (Tier 2 nome match patterns)

## Missão

1. Criar rel `(p:Person)-[:CLASSIFICADA_GLOBAL_PEP {match_type:'name'}]->(gp:GlobalPEP)` por match de nome exato (UPPER, normalizado).
2. Cuidar de homonímia: usar Tier 2 já existente (nome+UF, nome+partido) onde possível. Para nomes muito comuns (>3 Person GO mesmo nome), marcar match como `low_confidence` em vez de criar rel.
3. Adicionar selo "PEP Global (OpenSanctions)" no perfil quando há rel — pode ser combinado visualmente com selo PEP CGU.

## Critérios de aceite

- ≥800 Person GO com pelo menos 1 rel `:CLASSIFICADA_GLOBAL_PEP` (alguns dos 1.130 vão ser low_confidence e ignorados).
- Zero falso positivo confirmado em 30 perfis revisados.
- Selo aparece no PWA quando há match.
- `make pre-commit` verde.

## Guardrails

- Match só por nome → alto risco de homônimo. **Sempre validar com pelo menos 2 sinais** (cargo, partido, ano de eleição).
- Re-run idempotente.

## Dependência

Idealmente depois do item 02 (PEPRecord CGU) — o selo PEP CGU já cobre a maioria dos políticos atuais; GlobalPEP adiciona ex-políticos antigos não cobertos pelo CGU.

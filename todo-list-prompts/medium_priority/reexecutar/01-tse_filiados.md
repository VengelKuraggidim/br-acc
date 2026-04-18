# Refazer bootstrap do `tse_filiados` — ⏳ PENDENTE (2026-04-18)

> Pipeline falhou no download durante a rodada de bootstraps GO entre
> 2026-04-17 (noite) e 2026-04-18. Todos os outros pipelines da leva
> executaram com sucesso; só `tse_filiados` ficou pendente.

## Contexto

Estado do Neo4j local em 2026-04-18 (container `fiscal-neo4j` healthy há ~19h):

- Bootstraps GO que rodaram OK: `folha_go`, `pncp_go`, `tse`, `tse_bens`,
  `comprasnet`, `cvm`, `pgfn`, `transparencia`, `tcm_go`, `camara_goiania`.
- Dados em disco em `data/` pra ~20 pipelines.
- **Falhou no download**: `tse_filiados`.

Não há erro capturado/logado na sessão — só constatação de que o
download não completou. Precisa reproduzir, diagnosticar e refazer.

## Missão

1. Investigar o que quebrou no download do `tse_filiados`:
   - Rodar manualmente (`bracc-etl run tse_filiados` ou via runner).
   - Checar se é problema de URL upstream (TSE mudou path/esquema),
     timeout, tamanho do payload, rate-limit, ou bug no
     `script_download` do pipeline.
   - Olhar `etl/src/bracc_etl/pipelines/tse_filiados.py` +
     `scripts/download_tse_filiados.py` (se existir).
2. Corrigir a causa raiz (sem `file_manifest` fallback — ver guardrail
   "automate everything" em CLAUDE.md §3).
3. Reexecutar o bootstrap e confirmar ingestão no Neo4j.
4. Garantir que archival + provenance estão ativos (padrão pós
   retrofit 2026-04-18).

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/tse_filiados.py`
- `scripts/download_tse_filiados.py` (se aplicável)
- `etl/tests/test_tse_filiados*.py`
- `docs/source_registry_br_v1.csv` (entry do `tse_filiados`)
- `config/bootstrap_all_contract.yml` / `docs/bootstrap_go.md`

## Critérios de conclusão

- [ ] Causa raiz identificada e documentada no commit.
- [ ] Download roda de ponta-a-ponta sem intervenção manual.
- [ ] Nós/rels de filiação partidária populados no Neo4j local.
- [ ] Archival snapshots gravados em `archival/tse_filiados/...`.
- [ ] `make test-etl` verde.

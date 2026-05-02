# Aba "Embargos IBAMA" no município GO — 4.544 registros

## Contexto

Auditoria 2026-05-02: dos 101.956 nodes `:Embargo` (IBAMA, fonte `ibama`), **4.544 têm `uf='GO'`** com `municipio` populado (Crixás é exemplo). Hoje os Embargos só têm rel `EMBARGADA` para `:Company` (a empresa autuada), mas não há ligação direta com `:GoMunicipality`. Resultado: o dado existe mas nunca aparece num perfil do app.

Use case: aba "**Embargos ambientais**" no perfil de município GO mostrando lista de autos de infração, área embargada (hectares), bioma, infrator, data. Útil pra prefeito acompanhar passivos no território, e pra cidadão fiscalizar.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/ibama_embargos.py`
- `pwa/index.html` — perfil do município
- Memória: nenhuma específica

## Missão

1. Criar rel `(gm:GoMunicipality)-[:TEM_EMBARGO]->(e:Embargo)` para os 4.544 Embargos GO.
2. Match `e.municipio` ↔ `gm.name` com normalização (Crixás vs CRIXAS vs crixas).
3. Adicionar aba "Ambiental" no perfil do município no PWA: lista paginada de embargos com link pro auto de infração no IBAMA.
4. Bonus: se Company embargada for doadora de candidato GO (cruzar via :DOOU), criar selo "doadora com embargo ambiental" no perfil do candidato.

## Critérios de aceite

- 4.544 rels `:TEM_EMBARGO` criadas para municípios GO.
- Aba Ambiental funcional em pelo menos 5 municípios GO com embargos relevantes.
- `make pre-commit` verde.

## Guardrails

- Os outros ~97k embargos não-GO (PA, MT, AM, RO — Amazônia) podem ficar no banco por ora, ou ser cortados em Phase 2 separada.
- Validar que match município-name não cria duplicação (ex: "Crixás" e "Crixás de Goiás" são municípios diferentes).

## Dependência

Independente.

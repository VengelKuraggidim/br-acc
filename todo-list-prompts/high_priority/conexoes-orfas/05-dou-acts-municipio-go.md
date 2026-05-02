# Feed "DOU desta semana" — 1.094 atos federais mencionando município GO

## Contexto

Auditoria 2026-05-02: dos 68.984 nodes `:DOUAct` (Imprensa Nacional, fonte `imprensa_nacional`), **1.094 têm `text_excerpt` ou `title` mencionando algum município goiano**. Os DOUActs são 97% órfãos hoje (67k/69k sem rels) — o loader cria o node mas não extrai entidades para conectar.

Os atos incluem: portarias liberando recursos federais a municípios GO, nomeações/exonerações de servidores federais com lotação GO, transferências voluntárias, etc. Use case: feed "**DOU mencionou seu município nesta semana**" no perfil do município GO — alerta jornalístico forte, especialmente próximo a eleição/transição.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/imprensa_nacional.py` (ou nome similar)
- `pwa/index.html` — perfil do município

## Missão

1. Implementar extração de entidades simples no loader `imprensa_nacional`:
   - Para cada DOUAct, regex `\b[A-ZÁÉÍÓÚÃÕÂÊÔÇ\s]+/GO\b` ou cruzar com lista de 246 municípios GO.
   - Criar rel `(gm:GoMunicipality)-[:MENCIONADO_EM]->(d:DOUAct)`.
2. No perfil do município no PWA, adicionar aba "DOU" (ordem: data desc) com:
   - Data, agency, title, link pro DOU oficial.
   - Filtro por categoria (Portaria, Decreto, Edital).
3. Bonus: NER em nome de pessoa para conectar `Person -[:CITADA_EM]-> DOUAct` quando o ato menciona político GO específico (mais complexo, deixar para Phase 2).

## Critérios de aceite

- 1.094+ rels `:MENCIONADO_EM` criadas.
- Aba DOU visível em perfil de Goiânia, Anápolis, Aparecida com pelo menos 10 atos cada.
- `make pre-commit` verde.

## Guardrails

- Falsos positivos do regex: "GO" pode bater com "GOVERNO" ou outras siglas. Validar com lista canônica de municípios.
- Imprensa Nacional ingere dados diários — pipeline precisa rodar incrementalmente, não destruir DOUActs antigos.

## Dependência

Independente.

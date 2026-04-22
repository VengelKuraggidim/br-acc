# TCE-GO "Contas irregulares" + "Fiscalizações" via scraper Qlik Sense — ⏳ PENDENTE (2026-04-22)

> Extracted from todo 01-tce_go durante recon. As decisões (acórdãos,
> despachos, resoluções — 10k rows) foram carregadas via REST oficial do
> TCE-GO em `iago-search-api.tce.go.gov.br/decisions/search`. Dois
> datasets restantes ficam num painel **Qlik Sense** embedado, sem
> endpoint REST equivalente.

## Contexto

A página `https://portal.tce.go.gov.br/contas-irregulares` embute o
painel Qlik Sense via URL `single/`:

```
https://paineis.tce.go.gov.br/single/?appid=67f0715a-2d34-4d94-9ff4-3d96777233ca&sheet=5caeae7c-be2d-4a6f-9180-19ba014cce9f&lang=pt-BR
```

`paineis.tce.go.gov.br/qrs` (Qlik Repository Service) responde 200 — o
servidor está exposto. O serviço real de dados é o **Qlik Engine API**
via WebSocket em `wss://paineis.tce.go.gov.br/app/`.

## Por que não vai como os outros

Recon completo em 2026-04-21 descartou:

- Subdomínios `ws.tce.go.gov.br`, `irregulares.tce.go.gov.br`,
  `fiscalizacoes.tce.go.gov.br` — não existem.
- Catálogo de CKAN próprio do TCE-GO
  (`dadosabertos.tce.go.gov.br/api/3/action/package_list`) só tem
  indicadores sociais (saúde/água/educação), nada de judicial.
- CKAN estadual (`dadosabertos.go.gov.br`) busca `tce` → 2 resultados
  (1 PDF ABC, 1 dataset de imóveis) — nada útil.
- API `contas.tce.go.gov.br` (swagger `/swagger/docs/v1`) é para *entrada*
  de demonstrativos pelos jurisdicionados, não consulta.

O que existe é o painel Qlik, que requer:

1. Handshake WebSocket com `paineis.tce.go.gov.br/app/`.
2. Criar "ObjectGen" pra cada tabela visível no sheet.
3. `GetLayout` / `GetHyperCubeData` pra puxar linhas.
4. Parse do formato hypercube (cell matrix).

## Fontes de referência

- Qlik Engine API docs: `https://help.qlik.com/en-US/sense-developer/November2023/Subsystems/EngineAPI/Content/Sense_EngineAPI/introducing-engine-API.htm`.
- Libs prontas: `websocket-client` + algum wrapper tipo `python-qlik` (já
  há PoCs em pip; avaliar tamanho/licença antes).

## Missão

1. Abrir o painel em browser com DevTools → Network → WS, capturar as
   mensagens `CreateSessionObject` + `GetHyperCubeData` do sheet de
   contas-irregulares. Salvar como fixture.
2. Implementar cliente mínimo em `bracc_etl.pipelines.tce_go` com
   `websocket-client`: open `wss://paineis.tce.go.gov.br/app/{appid}`,
   abrir doc, criar hypercube com as dimensões da tabela, ler páginas
   de ~1000 linhas, fechar.
3. Parsear `qMatrix` (lista de linhas de lista de células com `qText`)
   em `irregulares.csv` / `fiscalizacoes.csv` matching os aliases que
   `_transform_irregular` e `_transform_audits` já aceitam.
4. Descobrir o `appid`/`sheet` correspondente às fiscalizações
   (provavelmente linkado na página `/fiscalizacao-dos-controles-internos`
   do portal).
5. Fixtures com snippets reais do WS handshake em
   `etl/tests/fixtures/tce_go/qlik_hypercube_*.json`.

## Cuidados

- `portal.tce.go.gov.br/robots.txt` = `User-Agent: *, Disallow:` (permite
  scraping do portal). `paineis.tce.go.gov.br/robots.txt` precisa ser
  verificado antes (pode divergir).
- Qlik Engine pode ter rate limiting server-side; o painel embedado é
  anonymous-access (sem sessão) então é robust o suficiente pra pelo
  menos scraping manual de 1x/mês. Se exigir cookie de sessão,
  precisará de handshake inicial em `/internal_forms_authentication/`.
- Volume esperado: contas irregulares ~milhares de rows (TCE-GO julga
  246 municípios × N anos). Fiscalizações: dezenas a centenas por ano.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/tce_go.py` — aceita CSVs para ambos
  datasets via `_transform_irregular` / `_transform_audits`.
  `fetch_to_disk` atual só toca decisões; estender pra orquestrar os 3.
- `scripts/download_tce_go.py` — CLI wrapper.
- `etl/src/bracc_etl/archival.py` — `archive_fetch` aceita `url=wss://...`
  mas gera snapshot dos bytes brutos; preservar o hypercube raw JSON seria
  o snapshot apropriado aqui.

## Critérios de aceite

- [ ] `data/tce_go/irregulares.csv` e `data/tce_go/fiscalizacoes.csv`
      populados via scraper automatizado.
- [ ] Nós `TceGoIrregularAccount` e `TceGoAudit` criados no grafo.
- [ ] Rels `IMPEDIDO_TCE_GO` entre Company e IrregularAccount (quando
      CSV trouxer CNPJ).
- [ ] Dependência nova (`websocket-client`) em group opcional
      `qlik = [...]` do pyproject — não em core.
- [ ] Teste offline com WS mockado via `pytest-asyncio` + fixture WS.

## Prioridade

Média-alta. Decisões já carregadas cobrem a maior parte do uso (10k
acórdãos/resoluções com ementa + relator = material rico pra
investigação). Contas irregulares + fiscalizações fecham a lacuna
institucional mas têm volume menor e peso informacional parcialmente
redundante com os outros pipelines de sanções (`tcmgo_sancoes`,
`sanctions`).

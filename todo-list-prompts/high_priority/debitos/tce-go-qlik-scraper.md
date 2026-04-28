# TCE-GO "Contas irregulares" + "Fiscalizações" via scraper Qlik Sense — ✅ DONE (2026-04-27)

> Concluído em 2026-04-27 com **Selenium + Firefox headless** (não via WS
> Engine API como o plano original previa). Detalhes do pivot abaixo.

## Resumo

- **Decisões** (~10k acórdãos/despachos/resoluções): REST oficial em
  `iago-search-api.tce.go.gov.br/decisions/search` (já estava ingerido).
- **Contas Irregulares** (8 PDFs anuais): Selenium scrape do painel
  `appid=67f0715a-…&sheet=5caeae7c-…`. Grava
  `data/tce_go/irregulares.csv` com URL do PDF preservada por linha.
  PDF parsing fica como fase 2 (extrair CNPJs/nomes individuais de dentro
  dos PDFs).
- **Fiscalizações em Andamento** (~50 processos): Selenium scrape do
  painel `appid=16a63cbf-…&sheet=6f2407d5-…`. Sheet tem 2 tabelas
  (summary + detail) com schemas diferentes — parser detecta por número
  de colunas. Grava `data/tce_go/fiscalizacoes.csv`.

## Por que Selenium e não WS Engine API

Recon confirmou que o endpoint WS existe em
`wss://paineis.tce.go.gov.br/app/<appid>` mas **openresty na frente
exige um query param `qlik-csrf-token=<token>`** — comportamento Qlik
Sense May 2024+. Esse token é **minted client-side por JS bootstrap**
quando o `/single/` carrega; não é cookie, não está em headers de
resposta, não vem de nenhum endpoint REST estável (`/qrs/csrftoken`,
`/api/v1/csrf-token`, `/qps/<vp>/csrftoken` todos retornam 404 ou redirect).

Reverse-engineering o bootstrap pra obter o token headless é frágil
(o shape muda entre Qlik patch versions). Como um browser real já faz
o bootstrap inteiro corretamente, o caminho de menor custo a longo
prazo é dirigir Firefox via Selenium e ler o DOM renderizado — o que
também serve de defesa contra mudanças menores no painel (recolocar
colunas, novo dataset por ano, etc).

## Como rodar

```bash
# Tudo (decisões via REST + irregulares + fiscalizações via Selenium):
uv run --project etl python scripts/download_tce_go.py \
    --output-dir data/tce_go --include-qlik

# Só painéis Qlik:
uv run --project etl python scripts/download_tce_go.py \
    --output-dir data/tce_go --no-decisoes --include-qlik

# Smoke test irregulares (2 min em headless Firefox):
uv run --project etl python scripts/download_tce_go.py \
    --output-dir /tmp/smoke --no-decisoes --include-irregulares
```

## Pré-requisitos de sistema

- Firefox instalado (em Ubuntu via snap: já vem por default).
- `geckodriver` no PATH (apt: `apt install firefox-geckodriver` OU
  release oficial em github.com/mozilla/geckodriver/releases).
- Path do binário do Firefox pode ser sobrescrito via env
  `BRACC_FIREFOX_BIN` (necessário em distros que empacotam Firefox via
  snap, onde `/usr/bin/firefox` é um shell stub e Selenium reclama).

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/tce_go_qlik.py` — novo módulo com:
  - Pure parsers `parse_irregulares_dom` / `parse_fiscalizacoes_dom`
    (testáveis offline via fixtures, sem selenium).
  - `fetch_panel_dom`, `fetch_irregulares_to_disk`,
    `fetch_fiscalizacoes_to_disk` (Selenium-driven).
- `etl/src/bracc_etl/pipelines/tce_go.py` — `fetch_to_disk` ganhou
  flags `include_irregulares=False` / `include_fiscalizacoes=False`
  (kwargs opt-in, back-compat preservada).
- `scripts/download_tce_go.py` — CLI com `--include-irregulares`,
  `--include-fiscalizacoes`, `--include-qlik`, `--no-decisoes`.
- `etl/pyproject.toml` — novo extra `qlik = ["selenium>=4.40.0"]`.
- `etl/tests/test_tce_go_qlik.py` — 16 testes offline contra fixtures
  capturados em `etl/tests/fixtures/tce_go/qlik_dom_*.json`.

## Critérios de aceite — status

- [x] `data/tce_go/irregulares.csv` populado via scraper automatizado
      (8 linhas, uma por ano, com PDF URL por linha).
- [x] `data/tce_go/fiscalizacoes.csv` populado via scraper automatizado
      (~50-60 linhas com numero, ano, tipo, status, descrição,
      relator, jurisdicionado, objetivo, lace).
- [x] Nós `TceGoIrregularAccount` e `TceGoAudit` criados pelo pipeline
      legado (transform inalterado — schemas dos CSVs continuam
      compatíveis com `_transform_irregular` / `_transform_audits`).
- [ ] Rels `IMPEDIDO_TCE_GO` entre Company e IrregularAccount — só
      acontece quando o CSV trouxer CNPJ. Como o índice TCE-GO não
      expõe CNPJ direto, fica para a **fase 2 (parsing dos PDFs)**.
- [x] Dependência nova (`selenium`) em group opcional `qlik` do
      pyproject — não em core.
- [x] Testes offline com payload DOM mockado em
      `etl/tests/fixtures/tce_go/qlik_dom_*.json`.

## Próxima fase (fora deste PR)

**PDF parsing dos arquivos de irregulares** — cada um dos 8 PDFs lista
internamente os servidores responsabilizados (CPF, nome, processo,
motivo). Extrair via `pypdf` (já em deps core) + heurística de tabela.
Quando feito, o pipeline ganha relacionamentos `IMPEDIDO_TCE_GO` reais.
Prioridade: média — o índice atual já dá visibilidade às listas e o
clique no PDF é UX trivial pra usuário curador.

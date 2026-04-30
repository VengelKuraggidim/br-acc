# TCM-GO "Impedidos de licitar" via scraper JSF — 🚫 BLOQUEADO POR robots.txt (2026-04-22, recheck 2026-04-27, recheck 2026-04-29)

> Commit `0f1b3c6` entrega o scraper completo (detalhes abaixo).
>
> **Produção barrada**: verificado em 2026-04-22, recheck em 2026-04-27,
> recheck em 2026-04-29 — tanto `https://tcmgo.tc.br/robots.txt` quanto
> `https://www.tcmgo.tc.br/robots.txt` terminam com `User-agent: *\nDisallow: /`. Sob a interpretação conservadora
> de combinar grupos com mesmo User-agent (Google/RFC 9309 em caso de
> conflito), isso proíbe crawling de qualquer path do subdomínio — inclusive
> o widget `/portalwidgets/xhtml/impedimento/impedimento.jsf`. O scraper
> não deve ser executado contra este host enquanto o robots.txt estiver
> nesse estado.
>
> **Smoke-test 2026-04-27**: 25/25 tests do pipeline passam contra a fixture
> `etl/tests/fixtures/tcmgo_sancoes/impedidos_licitar.csv` (extract +
> transform + load), confirmando que o caminho de ingestão está pronto pra
> receber o CSV real assim que a LAI retornar — basta soltar o arquivo em
> `data/tcmgo_sancoes/impedidos_licitar.csv` no shape
> `nome;cpf_cnpj;data_inicio;data_fim;orgao;processo;situacao` (mesmo
> separador `;` e header).
>
> **Fallback indicado pela própria TODO**: pedido LAI / e-SIC ao TCM-GO
> (`ouvidoria@tcmgo.tc.br`) requisitando export da lista em CSV. Quando
> chegar, dropar o CSV em `data/tcmgo_sancoes/impedidos_licitar.csv`
> e rodar o pipeline normalmente (caminho de ingestão já existe).
>
> **Rascunho do pedido LAI** pronto em
> `docs/legal/lai-tcmgo-impedidos-licitar.md` (2026-04-29) — preencher
> CPF + protocolo antes do envio.
>
> **O scraper fica guardado**: não foi removido do código porque:
>   1. A TODO histórica permanece útil como referência de padrão JSF
>      PrimeFaces (ViewState + partial-response XML) que outros
>      pipelines do repo podem reaproveitar.
>   2. Se o TCM-GO relaxar o robots.txt no futuro (já aconteceu com outros
>      portais GO), re-habilitar basta remover este aviso.
>
> ### Entregue em `0f1b3c6`:
>
> - `fetch_impedidos_jsf(output_dir, client=?)` em `tcmgo_sancoes.py` — GET
>   inicial pra ViewState + POST pagination PrimeFaces ate empty sentinel
>   (cap 500 paginas, rate-limit 1s).
> - CLI `scripts/download_tcmgo_sancoes.py --include-impedidos-jsf` ou
>   `--jsf-only` pra habilitar sem regredir o REST.
> - Extract/transform do pipeline le `impedidos_licitar.csv` e loada como
>   `:TcmGoImpedido` + IMPEDIDO_TCMGO com `list_kind='impedidos_licitar'`
>   (distingue da contas-irregulares ja existente que ganha
>   `list_kind='contas_irregulares'`).
> - 3 fixtures offline (HTML inicial + XML partial-response com dados +
>   XML empty) + 9 unit tests novos cobrindo parser + scraper end-to-end
>   com MockTransport.

## Original


> Extracted from todo 03-tcmgo_sancoes durante recon. A parte
> "contas-irregulares" (rejeitados) foi carregada via REST oficial
> `ws.tcm.go.gov.br/api/rest/dados/contas-irregulares` (1422 rows no
> grafo). **A lista de impedidos-de-licitar continua fora** porque fica
> num widget PrimeFaces sem REST endpoint correspondente.

## Contexto

A página `https://www.tcmgo.tc.br/site/tcm-em-acao/impedidos-de-licitar-ou-contratar/`
embute um iframe `https://tcmgo.tc.br/portalwidgets/xhtml/impedimento/impedimento.jsf`.
Esse widget é um PrimeFaces DataTable paginado:

- **Widget**: `form:impedimentos` (widgetVar `tabelaImpedimentos`)
- **Rows per page**: 20
- **Schema da tabela**: Nome | CPF/CNPJ | Início | Término | Órgão | Nº Proc. | Situação

Varredura completa do catálogo `GET /api/rest/servicoRest/all` do TCM-GO
(79 serviços) **não encontrou endpoint REST** pra essa lista. Os
serviços que mencionam licit-/contrat-/sanç- são sobre empenhos,
contratos ou licitações COLARE — não sobre pessoas/empresas impedidas.

## Missão

1. **Confirmar total de registros** no widget (clicar "última página" no
   portal; estimativa inicial: centenas).
2. **Escolher estratégia de scraping**:
   - **Opção A — POST com `_rows=99999`**: PrimeFaces DataTables
     frequentemente aceitam override do tamanho da página via parâmetros
     `form:impedimentos_rows=<N>` + `javax.faces.source=form:impedimentos`
     + `javax.faces.partial.ajax=true` + `javax.faces.ViewState=<token>`.
     Dump inteiro em 1 request. Mais simples se aceitar.
   - **Opção B — paginar rows**: GET inicial extrai ViewState, loop
     `first=0,20,40,...` até `response-has-no-rows`. Cada iteração POSTa
     os mesmos payload ajax com `form:impedimentos_first=<N>`. Lento mas
     estável.
3. **Parsear resposta**: PrimeFaces partial-response é XML
   `<partial-response><changes><update id="form:impedimentos">...html
   fragment...</update></changes></partial-response>`. Dentro do update
   vem o tbody com as linhas. `selectolax` consegue extrair.
4. **Schema de saída**: reusar `etl/src/bracc_etl/pipelines/tcmgo_sancoes.py`.
   Adicionar entrada em `impedidos_licitar.csv` (nome separado —
   **NÃO** misturar com `impedidos.csv` que hoje guarda contas irregulares
   do REST!). Atualizar pipeline pra ler o novo arquivo e emitir nós
   `TcmGoImpedido` + rels `IMPEDIDO_TCMGO` pros CNPJs (aqui ambos CPFs
   *e* CNPJs aparecem na mesma tabela, então a ramificação por
   `doc_kind` finalmente se torna útil).
5. **Semantic fix paralelo**: renomear `impedidos.csv` → `contas_irregulares.csv`
   dentro do pipeline (é o que de fato vem do REST). Hoje o nome é
   historicamente errado e confunde quem for estender.

## Arquivos relevantes

- `etl/src/bracc_etl/pipelines/tcmgo_sancoes.py` — hoje só consome REST.
- `scripts/download_tcmgo_sancoes.py` — CLI wrapper (precisaria ganhar
  `--include-impedidos-jsf` ou script separado).
- Memo `project_fotos_politicos_pendente.md` mostra padrão de scraper JSF
  com ViewState em outro pipeline, se precisar de referência.

## Cuidados

- `robots.txt` do TCM-GO tem `User-agent: *, Disallow: /` no bottom. Esse
  widget fica em subdomínio `tcmgo.tc.br` diferente do `www.tcmgo.tc.br`
  — o robots.txt do subdomínio específico precisa ser re-verificado
  antes de scrapear. Se proibir, fallback é LAI.
- Rate limit ≥ 1s entre requests.
- User-Agent identificado.

## Critérios de aceite

- [ ] Total de impedidos-de-licitar conhecido e persistido como
      `TcmGoImpedido {source:'tcmgo_sancoes', list_kind:'impedidos_licitar'}`.
- [ ] Rels `IMPEDIDO_TCMGO` criadas pra CNPJs dessa lista.
- [ ] Fixture com snippet JSF real em
      `etl/tests/fixtures/tcmgo_sancoes/impedidos_jsf.html` pra teste
      offline.
- [ ] Renome de `impedidos.csv` → `contas_irregulares.csv` aplicado sem
      quebrar archival existente (backward-compat via checar ambos os
      nomes no `_read_csv_optional`).

## Prioridade

Média-alta. Sem isso, "impedidos de licitar" (decisão administrativa
ativa, geralmente mais grave que "conta rejeitada") não aparece no
produto. Valor informacional alto pra investigação de contratos com
empresas já barradas.

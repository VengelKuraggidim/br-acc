# Camara Municipal de Goiania — endpoints JSON sao stubs; dados reais requerem scraping HTML + PDF

## Contexto

O pipeline `camara_goiania` (em `etl/src/bracc_etl/pipelines/camara_goiania.py`)
foi desenhado assumindo que o portal Plone do Legislativo Municipal de
Goiania expoe **3 endpoints JSON estaveis** (`@@portalmodelo-json`,
`@@transparency-json`, `@@pl-json`) com listas de vereadores, despesas e
proposicoes. IngestionRun de 2026-04-17 registrou `rows_loaded=0`.

Investigacao 2026-04-19 (prompt 04): os endpoints **respondem 200 OK mas
devolvem apenas stubs de metadata**, nao dados reais:

- `https://www.goiania.go.leg.br/@@portalmodelo-json` — devolve um dict
  unico com campos vazios (`{address:"", title:"", ...}`) descrevendo a
  Camara como instituicao, nao listagem de vereadores. `_unwrap_records`
  trata como 1 "row" espuria.
- `https://www.goiania.go.leg.br/@@transparency-json` — devolve 5
  categorias de conteudo (`atos-administrativos`, `licitacoes-e-contratos`,
  `orcamento-e-financas`, `parlamentares-e-gabinetes`, `recursos-humanos`)
  cada uma com URIs para subpaginas HTML (ex.: "Contratos e convenios
  2024" -> URL). Nao tem valores de despesa nenhum.
- `https://www.goiania.go.leg.br/@@pl-json` — devolve 1 legislatura
  antiga (17a, 2013-2016) com 1 membro null + 1 parlamentar legado
  ("Anselmo Pereira", PMDB, 2006). Totalmente desatualizado comparado
  aos ~28 vereadores da legislatura atual.

Resultado: extract reporta `vereadores=1 expenses=1 proposicoes=1`, todos
lixo; transform nao encontra chaves esperadas (`nome`/`partido`/`valor`)
porque o payload usa schema Plone nativo (`full_name`/`party_affiliation`);
load cria 0 nos (dedup + empty filter).

## Por que nao conserto no prompt 04

Conforme instrucao do prompt ("se scraping inviavel, NAO invente workaround
fragil"), o caminho real exige reescrita substancial:

1. **Listagem de vereadores** — `GET /institucional/parlamentares/` devolve
   HTML com ~28 links `/institucional/parlamentares/<slug>`. Extraivel
   via regex, mas cada perfil e HTML separado sem endpoint JSON
   (`@@portalmodelo-json` naquelas paginas retorna 404). Partido, foto,
   biografia exigem scraping HTML por-vereador.
2. **Despesas** — nao tem endpoint JSON. Estao em subpaginas sob
   `/transparencia/parlamentares-e-gabinetes/...`, muitas das quais
   sao PDFs (ex.: `quadro-de-cargos-e-direcao-2024.pdf`). Parsing
   PDF + navegacao entre subpaginas = scraping fragil.
3. **Proposicoes** — formato desconhecido; provavel HTML tambem.

Cada passo viola "everything automated + robusto". Acumular selectors
CSS + PDF parsing sem contrato estavel da fonte gera debito crescente.

## Opcoes pra retomar (em ordem de viabilidade)

1. **Querido Diario (queridodiario.ok.org.br)** — ja ingerido via
   pipeline `querido_diario_go`. Verificar se captura Resolucoes da
   Camara Municipal de Goiania (verba indenizatoria, subsidios) e
   cruzar com regex de atos administrativos. Zero scraping custom.
2. **Portal da Transparencia federal (SICONFI)** — tem despesas
   agregadas de Camaras Municipais por ente federativo. Granularidade
   menor (nao por-vereador), mas dado real e estavel.
3. **basedosdados.org** — verificar se tabelas `camara_goiania_despesas`
   ou `vereador_goiania_gabinete` ja existem (plataforma consolida muita
   lei organica). Zero scraping.
4. **Scraping HTML + PDF full** (ultima opcao) — se 1-3 falharem,
   implementar scraper robusto com: (a) listagem via regex em
   `/institucional/parlamentares/`, (b) perfil individual via HTML
   parser (BeautifulSoup), (c) PDF parsing para despesas via `pdfplumber`,
   (d) archival por URL de cada fetch. Custo: semanas; debito continuo
   porque o portal Plone pode reestruturar a qualquer release.

## Status pos-2026-04-19

- `docs/source_registry_br_v1.csv`: `camara_goiania` marcado
  `status=blocked_external`, `load_state=not_loaded`, notas detalhadas.
- `docs/pipeline_status.md`: linha atualizada pra `blocked_external`.
- Pipeline `camara_goiania` e fetch_to_disk ficam no repo — sem delete,
  pra retomada futura nao perder a casca. Testes continuam verdes
  (fixtures offline cobrem o caso "registros presentes no disco").
- Nenhum codigo novo — commit inteiramente de docs.

## Audit 2026-04-22 — viabilidade da opção 1 (querido_diario_go)

Recon via `api.queridodiario.ok.org.br`:

- **Cobertura**: Goiânia aparece no QD com `territory_id=5208707`,
  `level=3` (full-text search habilitado), disponível desde 2020-11-24.
  `publication_urls` aponta **APENAS** pra
  `https://www.goiania.go.gov.br/casa-civil/diario-oficial/` — ou seja,
  **Diário da Prefeitura** (executivo). A CMG **não tem Diário Oficial
  autônomo** (confirmado em `goiania.go.leg.br/` — só "Atos Normativos"
  em HTML) e também não tem `territory_id` separado no QD.
- **Quantidade de matches** (queries full-text pelo endpoint público):

  | Query | Total gazettes | Fonte |
  |---|---:|---|
  | `"Câmara Municipal" vereador` | 5.285 | Prefeitura diário |
  | `"verba indenizatória"` | 1.173 | Prefeitura diário |
  | `"subsídio vereador"` | 3.357 | Prefeitura diário |
  | `"resolução da Mesa"` | 8.063 | Prefeitura diário |

  O diário da Prefeitura **inclui** acts CMG-referenciados (transferências
  pro Legislativo, resoluções da Mesa publicadas cruzadas com executivo,
  subsídios fixados por lei municipal). Não substitui o portal CMG, mas
  preenche parte do buraco.

### O que QD pode entregar (com retrofit de `querido_diario_go`)

Adicionar em `_ACT_TYPE_PATTERNS` (hoje cobre só nomeação / exoneração
/ contrato / licitação):

```python
("ato_vereador", re.compile(
    r"verba\s+indenizat[oó]ria|subs[ií]dio\s+vereador|"
    r"resolu[cç][aã]o\s+da\s+Mesa",
    re.IGNORECASE,
)),
```

Com isso, `MunicipalGazetteAct` ganha rows com `act_type='ato_vereador'`
cobrindo **parcialmente** (1) fixação de subsídios por lei municipal e
(2) transferências de verba indenizatória publicadas no executivo.

### O que QD **NÃO** entrega (ainda precisa scraper CMG próprio)

| Necessidade | Por quê não está no QD |
|---|---|
| Listagem ativa de vereadores (28 da legislatura atual) | CMG publica em seu próprio portal (`/institucional/parlamentares/`), não no diário executivo |
| Detalhe por-vereador (foto, biografia, partido corrente) | idem — só HTML do portal CMG |
| Proposições (PL, PD, PR) | Tramitam só em `camaragoiania.sapl.com.br` ou equivalente; não viram acto de executivo |
| Despesas por fornecedor × vereador | Publicadas por resolução em PDF no portal CMG (`/transparencia/...pdf`), raramente replicadas no diário executivo |

## Criterio de desbloqueio (revisado 2026-04-22)

Plano em duas camadas:

**Camada 1 — ✅ ENTREGUE em 2026-04-22 (commit `98f016e`)**: retrofit do
`querido_diario_go` adicionando regex `ato_vereador` (incluindo
"câmara municipal de goiânia" — cobertura mais ampla que o plano
original). Prioridade sobre `nomeacao`/`contrato` via ordering dos
patterns. 5 testes novos em
`etl/tests/test_querido_diario_go_pipeline.py::TestClassifyAct`
(verba_indenizatória / subsídio / resolução_mesa / prioridade +
integration). `MunicipalGazetteAct {act_type='ato_vereador'}` passa a
ser populado automaticamente em runs futuros do `querido_diario_go`.

**Camada 2 — re-escopada em 2 fases (2026-05-02)**:

### Fase 2a — vereadores 20ª Legislatura ✅ ENTREGUE (2026-05-02)

Scraping HTML do portal CMG, sem PDF parser. Substituiu integralmente
os 3 endpoints Plone stub (`@@portalmodelo-json` / `@@transparency-json`
/ `@@pl-json`) que ficaram dead code desde 2026-04-19.

Implementação em `etl/src/bracc_etl/pipelines/camara_goiania.py`:

- `fetch_to_disk(output_dir, limit, archival)`:
  1. `GET /institucional/parlamentares/` → extrai slugs ativos (28 na 20ª
     Legislatura), filtra `legislaturas-anteriores`.
  2. `GET /institucional/parlamentares/<slug>` por perfil; parser regex
     extrai `Partido`, `Nascimento`, `Telefones`, `E-mail`, `Gabinete`,
     biografia (após `Natural`) e foto (`/Fotos-de-parlamentares/...`).
  3. Salva HTML cru em `data/camara_goiania/raw/` (1 listagem + N perfis).
  4. Salva JSON consolidado em `data/camara_goiania/vereadores.json`.
  5. Archival opt-in via `archive_fetch` por HTML (URI carimbada em
     `__snapshot_uri` por vereador, propagada ao :GoVereador via
     `attach_provenance`).
  6. UA realista (`Mozilla/5.0 ... Chrome/124.0`) + 0.5s entre requests
     pra não martelar o portal Plone.
- `extract` lê só `vereadores.json` local — sem fallback online no
  pipeline (download é responsabilidade do `fetch_to_disk` via
  `scripts/download_camara_goiania.py`).
- `transform` cria `:GoVereador` rico: `name`, `party`, `photo_url`,
  `gabinete`, `phones`, `email`, `birth_date` (ISO), `bio_summary`,
  `profile_url`, `legislature='20'`, + provenance.
- 24 testes em `etl/tests/test_camara_goiania_pipeline.py` cobrindo
  parsers (slugs, perfil HTML, bio truncation), pipeline offline,
  archival round-trip e cap de `--limit`.

`docs/pipeline_status.md` e `docs/source_registry_br_v1.csv` passam pra
`implemented_partial / partial`.

**Para rodar (não rodado ainda no Neo4j local):**

```bash
uv run --project etl python scripts/download_camara_goiania.py \
    --output-dir data/camara_goiania
cd etl && uv run bracc-etl run --source camara_goiania
```

### Fase 2b — despesas / folha / diárias (PENDENTE)

Despesas de gabinete, combustível, folha de pagamento e diárias **não
estão** no portal CMG (`www.goiania.go.leg.br`). A página
"Gastos de gabinetes" da transparência redireciona pro NúcleoGov:

`https://camaragoiania.nucleogov.com.br/cidadao/transparencia/...`

NúcleoGov é SPA RequireJS — HTML inicial só diz "Habilite o JavaScript".
Endpoints disponíveis na navegação JS:

- `/cidadao/transparencia/mp/id=18` — Duodécimo
- `/cidadao/transparencia/mp/id=23` — Diárias
- `/cidadao/transparencia/mp/id=34` — Devolução de Duodécimo
- `/cidadao/transparencia/cntservidores` — Folha de Pagamento (302)
- `/cidadao/transparencia/padraoremuneratorio` — Estrutura Remuneratória
- `/cidadao/transparencia/tabeladiarias` — Tabela de Diárias
- (combustível / gastos de gabinete sob outras rotas internas)

**Plano da Fase 2b** (escopo: dias, igual TCE-GO Qlik):

1. Scraper Selenium/Playwright reaproveitando infra do
   `etl/src/bracc_etl/pipelines/tce_go_qlik.py` (mesma stack de driver
   headless + waits).
2. Mapear cada rota NúcleoGov pra um schema canônico
   (`GoCouncilExpense`, `GoCouncilDiaria`, `GoCouncilPayroll`).
3. Reintroduzir `expenses` / rels `DESPESA_GABINETE` no pipeline
   `camara_goiania` (foram removidos na Fase 2a por não terem fonte).
4. Resolver entity match `:GoVereador` ↔ despesa por nome
   normalizado (CPF não publicado pelo NúcleoGov, pelo padrão de outras
   câmaras GO).
5. Archival por URL/screenshot HTML pré-render do estado SPA.

Reabrir este TODO quando começar. Trigger: usuária pedir (ou Fase 2a
gerar interesse de stakeholder em ver R$ no perfil de cada vereador).

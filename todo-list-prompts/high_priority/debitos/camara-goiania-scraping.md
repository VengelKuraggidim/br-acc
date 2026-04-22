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

**Camada 1 — quick win (~1h)**: retrofit do `querido_diario_go` adicionando
regex `ato_vereador` como descrito acima. Não resolve o produto "perfil de
vereador", mas bota sinais CMG-fuzzy-matched no grafo (`MunicipalGazetteAct
{act_type='ato_vereador'}`), consultáveis por quem investigar.

**Camada 2 — solução completa**: continua valendo a opção (4) — scraper
dedicado do portal CMG. Re-avaliar ROI a cada 6 meses; escopo (a) listagem
parlamentares via HTML simples, (b) PDF parser pras despesas via `pypdf`
(já dep), (c) archival por URL.

Se alguma das fontes alternativas materializar mais dado equivalente
(basedosdados.org `vereador_goiania_despesas` é o candidato mais realista
à médio prazo), a camada 2 vira deletable.

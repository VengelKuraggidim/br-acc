# Custo de mandato — esfera municipal (prefeito + vereador)

## Status 2026-04-22 — MVP Goiânia entregue

- **Pipeline** `etl/src/bracc_etl/pipelines/custo_mandato_municipal_go.py`
  cobre `prefeito_goiania` e `vereador_goiania`. Mesmo padrão de
  `custo_mandato_br`; `archive_fetch` das fontes legais;
  `CustoMandato` + `CustoComponente` + `TEM_COMPONENTE`.
- **Valores**: vereador = 75% × subsídio dep estadual GO (CF Art. 29 VI)
  = R$ 26.080,98/mês. Prefeito fica `None` com observação "Lei Orgânica
  Municipal; consulte DOM-GYN" (padrão do `governador_go`).
- **API**: router `GET /custo-mandato/{cargo}` aceita os 2 cargos novos
  via `CargoEnum`. Service `CARGOS_SUPORTADOS` idem. Modelo ganhou
  campo `municipio`.
- **Testes**: 18 novos em `etl/tests/test_custo_mandato_municipal_go.py`
  + atualizados em `api/tests/unit/test_custo_mandato_service.py`. Todos
  passam.
- **Runner + registry**: registrado em `runner.py::SOURCES` e
  `docs/source_registry_br_v1.csv`.
- **Rodado no Docker Neo4j local**: 2 cargos + 4 componentes + 4 rels
  gravados. API local responde `/custo-mandato/vereador_goiania`
  devolvendo R$ 26,1 mil/mês × 35 cadeiras = **R$ 10,95 mi/ano**.
- **Aura prod NÃO rodado** — bloqueado por quota Free (200k nodes
  atingidos). Ver `aura-free-quota-estourada.md`. Assim que o Aura
  liberar espaço, re-rodar o pipeline em prod.

**Escopo restante** (big project separado, não bloqueia o MVP): os
245 municípios goianos restantes. Cada lei orgânica publicada em DOM
municipal próprio sem API — mesmo padrão do débito original permanece.
Candidato a fallback: `basedosdados.org` se materializar tabela
consolidada.

## Contexto

O endpoint `GET /custo-mandato/{cargo}` (pipeline `custo_mandato_br`)
materializa o custo dos cargos eletivos federal e estadual GO no grafo,
substituindo o card hardcoded "Quanto custa um deputado federal?" da home
do PWA. Cobertura entregue (MVP):

- `dep_federal` (subsídio + CEAP + gabinete + auxílio-moradia + opacos)
- `senador` (subsídio derivado constitucionalmente)
- `dep_estadual_go` (subsídio = 75% federal por CF Art. 27 §2°)
- `governador_go` (subsídio capped em Min STF por CF Art. 37 XI)

**Fora do escopo do MVP:** prefeito e vereador. Esta nota descreve por
quê e como retomar.

## Por que ficou de fora

Cada município tem **lei orgânica própria** que fixa subsídio de prefeito
e vereador. Não existe API consolidada que devolva esses valores em
formato máquina-legível. O Brasil tem ~5.570 municípios; Goiás tem 246.
Limitações descobertas:

- **Câmara Municipal de Goiânia** — não expõe API pública consultável
  (limitação já anotada no card "Cand. Vereador" da home, em
  `pwa/index.html:1620` via tooltip). PWA hoje só conhece **candidatos**
  a vereador via TSE (último pleito), não vereadores em exercício.
- **Lei orgânica** — publicada em diário oficial municipal; cada
  município segue formato próprio. Tetos derivados (CF Art. 29 VI cap
  vereador a % do subsídio do dep. estadual; CF Art. 29 V cap prefeito
  a % do governador) ajudam, mas o valor exato é de lei municipal.
- **Resoluções de Câmara Municipal** — fixam verba de gabinete,
  diárias, etc. — também sem API.

## Opções pra retomar (em ordem de viabilidade)

1. **Base dos Dados (basedosdados.org)** — verificar se já consolidaram
   `municipio_subsidio_prefeito` / `municipio_subsidio_vereador` (a
   plataforma tem várias tabelas derivadas de SICONFI/SIOPS). Se sim,
   ingerir via BigQuery (pipeline existente `siconfi`/`siop` é referência
   de padrão BQ + creds GCP do Asgard Studio).
2. **OCR + parser de Querido Diário** — pipeline `querido_diario_go` já
   ingere diários oficiais municipais GO. Estender pra extrair valores
   de subsídio via regex/LLM nos PDFs publicados a cada início de
   legislatura. ROI alto pra Goiânia (1 município = 1 PDF/4 anos);
   baixo pros 245 municípios restantes.
3. **Formulário manual operacional** — pra cidades sem dados abertos,
   admin colaborador insere valor + URL da Lei Orgânica; pipeline lê de
   um YAML em `data/custo_mandato_municipal_go/*.yml` versionado no
   repo. Híbrido honesto: cobre Goiânia + Aparecida + grandes municípios
   GO sem prometer cobertura nacional.
4. **CF cap derivado** — exibir só o **teto constitucional** (% do dep.
   estadual / % do governador) sem alegar valor exato. Honesto mas
   limitado: usuário não sabe quanto **realmente** ganha o vereador
   da cidade dele.

## Recomendação

Começar por (4) pra cobertura imediata + (2) pra Goiânia (alto ROI:
um município, um PDF do Diário Oficial Municipal de Goiânia em mandato
2025-2028). (1) só se Base dos Dados já consolidou; senão (3) como
fallback pra municípios estratégicos.

## Critério de retomada

- Quando `cargo_municipal` aparecer no perfil de algum político GO
  (prefeito de Goiânia, vereador eleito) e o PWA precisar mostrar
  contexto de "quanto custa esse cargo" no perfil dele.
- Quando Base dos Dados expor a tabela consolidada (verificar
  trimestralmente).
- Quando a feature `custo_mandato_br` pegar tração e usuários pedirem
  paridade pros cargos municipais.

## Onde tocar

- Pipeline novo: `etl/src/bracc_etl/pipelines/custo_mandato_municipal_go.py`
  (mesmo padrão de `custo_mandato_br`: `:CustoMandato` por
  `(cargo, municipio)` + `:CustoComponente` ligados).
- Service/router: estender `bracc.services.custo_mandato_service` +
  `bracc.routers.custo_mandato` pra aceitar `?municipio=goiania` no
  query string (ou path `/custo-mandato/{cargo}/{municipio}` se virar
  feature de primeira classe).
- Registry + bootstrap: replicar entries do `custo_mandato_br`.
- PWA: adicionar municípios no seletor de cargo (ou novo seletor de
  município quando cargo é `prefeito`/`vereador`).

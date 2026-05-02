# Custo de mandato — esfera municipal (prefeito + vereador)

## Status 2026-05-02 — Fase 2 (top-10 GO) entregue

Expansão de Goiânia (MVP fase 1, abr/2026) pra top-10 cidades GO por
população (Censo IBGE 2022): Aparecida de Goiânia, Anápolis, Rio Verde,
Águas Lindas de Goiás, Luziânia, Valparaíso de Goiás, Trindade, Formosa
e Senador Canedo. Cobertura via Option 4 (CF cap derivado) — caminho
recomendado lá embaixo.

- **Pipeline** `etl/src/bracc_etl/pipelines/custo_mandato_municipal_go.py`
  ganhou `_GO_MUNICIPIOS` (tabela de cidades + população) +
  `_vereador_pct_tier` (% CF Art. 29 VI por faixa) + `_vereador_min_seats`
  (CF Art. 29 IV mínimo de cadeiras) + `_build_components_and_meta`
  (gera os 20 cargos do `_COMPONENTS`/`_CARGO_META` programaticamente).
  Goiânia preserva URLs específicas (DOM-GYN, transparência, CMG); as
  outras 9 cidades caem no padrão genérico (CF + Casa Civil GO).
- **Valores cap por faixa** (CF Art. 29 VI; base = R$ 34.774,64 do dep
  estadual):
  - Goiânia (1.43M) + Aparecida (591k): >500k → 75% → R$ 26.080,98
  - Anápolis (391k): 300-500k → 60% → R$ 20.864,78
  - Rio Verde (245k), Águas Lindas (218k), Luziânia (211k), Valparaíso
    (170k), Trindade (134k), Formosa (123k), Senador Canedo (115k):
    100-300k → 50% → R$ 17.387,32
- **n_titulares** vem do `n_vereadores` da tabela quando conhecido
  (Goiânia=35, legislatura 2025-2028) ou cai no mínimo CF Art. 29 IV
  pela faixa populacional (ex.: Anápolis tier 300-450k → 23, Aparecida
  tier 450-600k → 25).
- **Prefeitos**: continuam `valor_mensal=None` em todas as 10 cidades —
  Lei Orgânica Municipal não tem formato consolidado. Observação textual
  aponta pro Diário Oficial Municipal/Casa Civil GO.
- **API**: router `GET /custo-mandato/{cargo}` migrou de `CargoEnum`
  (StrEnum gigante) pra Path pattern + validação contra
  `CARGOS_SUPORTADOS` (frozenset). 422 = slug malformado, 404 = slug
  bem-formado fora do conjunto. Service `CARGOS_SUPORTADOS` enumera os
  24 cargos (4 fed/est + 20 municipais) via `_MUNICIPIOS_GO`.
- **Testes**: 42 cases em `etl/tests/test_custo_mandato_municipal_go.py`
  (incluindo `TestTierFormula` parametrizado em todas as faixas) +
  `api/tests/unit/test_custo_mandato_service.py` atualizado pra cobrir
  o novo conjunto. Todos passam (`pytest etl/tests/test_custo_mandato_municipal_go.py`
  → 42 pass; `pytest api/tests/unit/test_custo_mandato_service.py` →
  10 pass).
- **Runner + registry**: linha do `custo_mandato_municipal_go` em
  `docs/source_registry_br_v1.csv` reescrita pra refletir top-10 GO.
- **Rodado no Docker Neo4j local** (bolt://localhost:7687): 20 cargos
  + 40 componentes + 40 rels gravados. API local
  `/custo-mandato/vereador_anapolis` devolve R$ 20,9 mil/mês × 23
  cadeiras = **R$ 5,76 mi/ano** com proveniência clicável (CF Art. 29
  VI no planalto).
- **PWA**: seletor `QUANTO_CUSTA_CARGOS` em `pwa/index.html` ainda só
  expõe os 4 cargos federal/estadual. Adicionar municípios depende de
  decisão UX (seletor de município? Sub-menu "GO municipal"?). Não
  bloqueia: o backend serve, basta o front consumir.
- **Aura prod NÃO rodado** — segue bloqueado por quota Free e congelado
  por decisão 2026-05-02 (`aura-adiado-sem-grana.md`). Ambiente atual
  é localhost.

**Escopo restante (Fase 3)**: os 236 municípios goianos restantes (todos
com população ≤ 100k habitantes — caem nas faixas 20%/30%/40% do CF
Art. 29 VI). Inflar o backend com ~470 cargos sem demanda do PWA é
ruído sem usuário, então fica como débito. Quando precisar (PWA mostrar
"custo do vereador da minha cidade"), basta estender `_GO_MUNICIPIOS` —
toda a geração é mecânica. Mesma fórmula CF Art. 29 VI. Candidato
adicional: `basedosdados.org` se materializar tabela consolidada de
subsídio efetivo (não só teto).

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

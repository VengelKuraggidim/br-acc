# Mostrar "quanto o político gasta com a política" para deputados estaduais e vereadores de GO — ✅ CONCLUÍDO (2026-04-18)

> ALEGO routing implementado via `PerfilService.roteia_despesas` (commits `f0bd87d`,
> `23559b3`, `5d00152`). Cota de vereadores GYN coberta no follow-up (prompt 13).

## Contexto
Hoje o bloco "Despesas de gabinete" no perfil do político só aparece para **deputados federais**, porque o backend busca CEAP (cota parlamentar) via API da Câmara Federal (`backend/apis_externas.py` `buscar_despesas_deputado`). Resultado: políticos como Cairo Salim (deputado estadual GO/PSD) ou vereadores de Goiânia abrem o perfil e **a seção "gastos com política" fica vazia**, mesmo eles tendo verba indenizatória estadual (ALEGO) ou cota de vereador (Câmara de Goiânia).

Usuário leigo compara com Caiado (que mostra CEAP) e acha que "o app quebrou" — quando na verdade a fonte de dados é outra.

## Objetivo pra usuário leigo
Todo político com mandato (federal, estadual ou municipal em GO) deve mostrar uma seção **"Quanto gasta com a política"** — com valor total mensal/anual e top 3 categorias — independente de qual casa legislativa ele pertença. Copy: "Esse valor é dinheiro público que o político pode usar pro trabalho parlamentar (passagens, aluguel de escritório, material, etc.)."

## Fontes de dados
1. **Deputados estaduais GO** → verba indenizatória da ALEGO.
   - **Depende do pipeline `alego`** já na TODO (`02-alego.md`). Hoje o pipeline é scaffold e não carrega CSV real. Sem isso, não tem dados no grafo.
   - Caminho: `(:StateLegislator)-[:GASTOU_COTA_GO]->(:LegislativeExpense)` (schema previsto no 02).
2. **Vereadores de Goiânia** → cota de vereador da Câmara Municipal de Goiânia.
   - Sem pipeline hoje. Câmara de Goiânia publica alguns dados em `camaragyn.go.gov.br`. Pode virar novo pipeline `camara_goiania_cota` (distinto do `camara_goiania` atual que é só legislativo).
   - Como fallback rápido: scraping do portal de transparência da Câmara Municipal.
3. **Deputados federais** → já funciona (não tocar).

## Arquivos relevantes
- `backend/app.py` (`perfil_politico`, linhas ~630-730):
  - Hoje: `buscar_deputado_camara` → `buscar_despesas_deputado` → preenche `despesas_gabinete`. Só funciona pra federal.
  - Precisa: detectar cargo/tipo do político e rotear pra fonte correta.
- `backend/apis_externas.py`: adicionar `buscar_despesas_deputado_estadual_go` (consulta BRACC se pipeline `alego` carregado) e `buscar_despesas_vereador_goiania` (idem para pipeline a criar).
- `backend/analise.py` `traduzir_despesa`: expandir com categorias estaduais/municipais.
- `pwa/index.html` seção "Despesas de gabinete": trocar título pra "Quanto gasta com a política" + 1 linha de explicação dependendo da fonte ("vem da cota da Câmara Federal"/"vem da verba da ALEGO"/"vem da cota da Câmara de Goiânia").

## Missão
1. **Fase 1 — Roteamento**:
   - No backend, detectar o tipo do político a partir das propriedades do nó Person (`uf`, `cargo`/`role`, labels secundárias como `:StateLegislator`).
   - Se for `:StateLegislator` de GO → buscar no Neo4j via relação `GASTOU_COTA_GO`.
   - Se for vereador de Goiânia → buscar via relação do pipeline novo (ou fallback HTTP).
   - Se for federal → fluxo atual intacto.
2. **Fase 2 — Dados ALEGO** (bloqueada por TODO 02):
   - Aproveitar schema já desenhado no `02-alego.md` (`LegislativeExpense` + `GASTOU_COTA_GO`).
   - Adicionar helper no `backend/app.py` que faz Cypher query → agrupa por categoria → formata BRL.
3. **Fase 3 — Vereadores Goiânia** (novo pipeline):
   - Criar `etl/src/bracc_etl/pipelines/camara_goiania_cota.py`.
   - Fonte: identificar endpoint no portal da Câmara de Goiânia (provavelmente `transparencia.camaragyn.go.gov.br` ou similar) — primeiro mapear.
   - Mesmo padrão: nó `MunicipalLegislator` / `MunicipalExpense` + rel `GASTOU_COTA_GOIANIA`.
4. **Fase 4 — PWA**:
   - Título dinâmico: "Quanto gasta com a política (cota parlamentar federal/estadual/municipal)".
   - Copy pra leigo: "O [cargo] tem direito a uma verba pública pra custear o trabalho — aluguel de escritório, combustível, passagens, etc."
   - Se não tiver fonte pra aquele tipo de político: mostrar aviso honesto: "Ainda não temos os dados de gastos da [casa legislativa]. Em breve."

## Critérios de aceite
- Perfil do Cairo Salim (estadual GO) mostra gastos ALEGO depois de TODO 02 concluído.
- Perfil de um vereador de Goiânia mostra gastos da Câmara Municipal.
- Perfil do Caiado (federal) continua mostrando CEAP como hoje — nenhuma regressão.
- Político sem fonte disponível mostra aviso honesto, não bloco vazio.
- Copy leigo em toda a seção: nenhum jargão ("CEAP", "verba indenizatória", "cota parlamentar") sem explicação contextualizada.

## Dependências
- **TODO 02 (ALEGO pipeline)** precisa estar no estado `loaded` para Fase 2 funcionar.
- Fase 3 pode ir em paralelo com a 02, mas é trabalho novo (scraping + schema).

## Guardrails
- Respeitar `project_dual_frontends.md`: UI em `/pwa/index.html`, não `/frontend`.
- `backend/tests/test_integracao.py`: adicionar teste mockando os 3 tipos de político.
- Não remover o fluxo CEAP federal atual — ele é a referência funcional.
- `make pre-commit` verde.

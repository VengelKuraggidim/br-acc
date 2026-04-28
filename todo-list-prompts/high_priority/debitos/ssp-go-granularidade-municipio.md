# SSP-GO — granularidade municipal das estatísticas criminais

## Estado atual (2026-04-27)

**Parcialmente resolvido**: pipeline `mjsp_municipios` (commit em curso)
ingere a série municipal de **Homicídio Doloso** publicada pelo MJSP em
`dados.mj.gov.br/dataset/sistema-nacional-de-estatisticas-de-seguranca-publica`.
Schema: `Cód_IBGE | Município | Sigla UF | Região | Mês/Ano | Vítimas`,
filtrado pra UF=GO. Goiânia, Anápolis e Aparecida de Goiânia cobertos
com séries mensais (2018-01 a 2022-12 quando do levantamento). Coexiste
com o `ssp_go` state-level porque `stat_id` inclui `cod_ibge`.

**Restam abertas as 14 naturezas no nível município** (estupro,
feminicídio, latrocínio, lesão seguida de morte, roubo a transeunte,
roubo de veículos, roubo em comércio, roubo em residência, roubo de
carga, roubo a instituição financeira, furto de veículos, furto em
comércio, furto em residência, furto a transeunte). Nenhuma fonte
pública federal ou estadual machine-readable cobre essas naturezas em
nível município pra GO. Único caminho restante: **LAI à SSP-GO**
(observatorio.ssp@goias.gov.br) — pendência manual da usuária. Quando o
CSV chegar, dropar em `data/ssp_go/ocorrencias.csv` (path de override
já implementado no pipeline `ssp_go`).

**Opções rejeitadas em 2026-04-27:**
- **FBSP Anuário (XLSX 2024/2025)** — só capitais (Goiânia única
  cidade GO em T06) + rankings top-N (sem cidades GO no top 10 MVI
  2024). Não passa o critério de aceite.
- **SINESP/MJSP por UF** — 8 indicadores publicados em nível UF, mas
  só 1 (Homicídio Doloso) com granularidade municipal. As outras 7
  naturezas do feed UF (estupro, furto/roubo de veículo, roubo de
  carga, etc.) duplicariam a cobertura estadual já feita por `ssp_go`
  com menos naturezas — não vale o pipeline.

## Contexto

O pipeline `ssp_go` carrega, desde 2026-04-22, ~1.440 rows de
`GoSecurityStat` (8 anos × 15 naturezas × 12 meses) parseadas dos
boletins PDF anuais publicados em
`https://goias.gov.br/seguranca/estatisticas/`. Cobertura entregue:

- 15 naturezas (HOMICIDIO DOLOSO, FEMINICIDIO, ESTUPRO, LATROCINIO,
  LESAO SEGUIDA DE MORTE, ROUBO A TRANSEUNTE, ROUBO DE VEICULOS,
  ROUBO EM COMERCIO, ROUBO EM RESIDENCIA, ROUBO DE CARGA,
  ROUBO A INSTITUICAO FINANCEIRA, FURTO DE VEICULOS, FURTO EM COMERCIO,
  FURTO EM RESIDENCIA, FURTO A TRANSEUNTE).
- Período 2018–2025 (fonte: sistema ODISSEU/RAI, conforme nota de
  rodapé dos boletins).
- **Granularidade: estado inteiro** (`cod_ibge=5200000`,
  `municipality='ESTADO DE GOIAS'` em todas as rows).

**Fora do escopo do MVP:** breakdown por município. Esta nota descreve
por quê e como retomar.

## Por que ficou de fora

A SSP-GO **não publica** o recorte por município em canal
machine-readable. Levantado em 2026-04-22:

- **Portal `goias.gov.br/seguranca/estatisticas/`** — só PDFs de 1 página
  com total estadual por naturaza × mês. Nenhum anexo por município.
- **CKAN `dadosabertos.go.gov.br`** — a organização "Secretaria de
  Estado da Segurança Pública" só mantém o dataset `doacoes-recebidas-ssp`
  (doações recebidas, não crime). Polícia Civil publica só a taxonomia
  de 14 crimes (`crimes-registrados-pela-delegacia-virtual`), sem contagens.
- **Sem Power BI embed, sem API REST oculta** — portal é WordPress
  estático, sem painel dinâmico reconhecível.

A base RAI (Registro de Atendimento de Inteligência) no ODISSEU tem o
dado no nível de BO/município — ela só não sai pra fora da SSP.

## Opções pra retomar (em ordem de viabilidade)

1. **LAI / e-SIC para a SSP-GO** — pedir ao contato oficial
   `observatorio.ssp@goias.gov.br` (endereço que aparece no rodapé do
   portal `estatisticas/`) um dump mensal agrupando
   `(municipio × naturaza × mês)`. Linha de base: pedir os últimos 2
   anos pra validar schema antes de automatizar. Risk: SSP pode
   responder com CSV ad-hoc (formato variável a cada pedido) — nesse
   caso bastará dropar o CSV em `data/ssp_go/ocorrencias.csv` e o
   pipeline cobre (caminho de override já implementado).

2. **Painel "Observatório de Segurança Pública"** — existe menção em
   `observatorio.ssp@goias.gov.br`, mas o subdomínio
   `observatorio.ssp.go.gov.br` não resolve publicamente. Consultar via
   ticket se o observatório tem painel público não-linkado do portal
   principal, ou se parte só para órgãos conveniados.

3. **SINESP (federal)** — o Sistema Nacional de Informações de Segurança
   Pública consolida os estados e tem download público por UF em
   `sinesp.gov.br/sinesp-sd/index.xhtml`. A série "Vítimas
   Homicídio" permite filtro por município em alguns cortes; confirmar
   se a cobertura vai até nível município-mês pra todas as naturezas
   (histórico mostra que homicídio dolois é o único quase sempre
   granular nesse portal).

4. **Anuário Brasileiro de Segurança Pública (FBSP)** — `forumseguranca.org.br`
   publica anuário consolidado em PDF e, recentemente, XLSX por
   município. Cobertura: 2013 até 2024 (atraso de ~12 meses). Formato
   estável. Seria um 2º pipeline (`fbsp_anuario`), não uma extensão do
   `ssp_go`.

## Critérios de aceite (quando retomar)

- `GoSecurityStat` passa a aceitar `municipality != 'ESTADO DE GOIAS'` e
  `cod_ibge != '5200000'` sem regredir os nós estaduais já carregados
  (compat: stat_id hash inclui cod_ibge, então granularidades
  coexistem).
- Cobertura mínima: pelo menos 3 municípios de teste (Goiânia, Anápolis,
  Aparecida de Goiânia) validados manualmente contra boletins SSP
  (quando/se publicados) ou resposta LAI arquivada em
  `etl/archival/ssp_go/`.
- Registry: o campo `notes` deixa de mencionar "state-level only" e a
  origem municipal fica apontada.

## Dica pra quem pegar

O parser PDF já existe e é genérico: se a SSP passar a publicar anexos
por município no mesmo formato tabular, estender
`_parse_bulletin_pdf` pra aceitar uma coluna extra "MUNICIPIO" é
trivial. Se o formato mudar radicalmente (Excel, dashboard), o override
`ocorrencias.csv` é o caminho sem tocar em código.

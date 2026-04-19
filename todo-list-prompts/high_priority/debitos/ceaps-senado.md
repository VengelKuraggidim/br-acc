# CEAPS — Cota pra Exercicio da Atividade Parlamentar dos Senadores

## Contexto

O perfil do politico exibe um card **"Gastos com a politica"** com a fonte
da cota parlamentar. Hoje cobrimos 3 casas legislativas:

- **Camara Federal** (CEAP) — pipeline `camara_ceap` + service
  `obter_ceap_deputado`.
- **ALEGO** (verba indenizatoria estadual GO) — pipeline `alego` +
  service `obter_verba_indenizatoria_alego`.
- **Camara Municipal de Goiania** (CMG) — pipeline `camara_goiania` +
  service `obter_ceap_vereador_goiania`.

**Senadores ficaram de fora do MVP.** Ao abrir o perfil de Marconi
Perillo Junior (PSDB-GO, Senador 2015-2022) o card mostra
"Dados de gastos parlamentares nao disponiveis pra essa casa
legislativa." — texto fallback em
`api/src/bracc/services/perfil_service.py::_build_aviso_despesas`.

## Por que ficou de fora

O Senado Federal tem cota propria — **CEAPS** (Cota pra Exercicio da
Atividade Parlamentar dos Senadores) — regulada pelo Ato da Comissao
Diretora n° 3/2016. Nao e o mesmo sistema da Camara (CEAP), entao o
pipeline `camara_ceap` nao cobre. Valor mensal em 2025 e de
R$ 44.276,39 (variavel por UF — Goias usa tabela base).

## Fonte de dados (CEAPS)

- **Portal da Transparencia do Senado**:
  `https://www12.senado.leg.br/transparencia/dados-abertos`
- **Dataset CEAPS**: `https://www6g.senado.leg.br/transparencia/sen/lista`
  e CSV anual em
  `http://www.senado.gov.br/transparencia/LAI/verba/<ANO>.csv`
  (formato `;`-delimited, encoding latin-1, 1 arquivo por ano desde 2008).
- Campos relevantes: `ANO`, `MES`, `SENADOR`, `CNPJ_CPF`, `FORNECEDOR`,
  `DOCUMENTO`, `DATA`, `DETALHAMENTO`, `VALOR_REEMBOLSADO`, `COD_DOCUMENTO`.

## Opcoes pra retomar

1. **Pipeline dedicado `senado_ceaps`** (recomendado) — mesmo padrao do
   `camara_ceap`. Baixa CSV anual via `script_download`, parseia,
   carrega rel `:GASTOU_CEAPS` conectando `(:FederalLegislator
   {cargo:'senador'})` a `(:Empresa)/(:Pessoa)` por CNPJ/CPF. Archival
   obrigatorio (CSV anual do portal).
2. **Filtrar so senadores GO** — MVP rapido: so ingerir rows onde
   `SENADOR` bate com algum senador GO conhecido no grafo (3 por
   legislatura). Reduz ~95% do volume.

## Dependencias no codigo

- **Service novo**: `obter_ceaps_senador` em
  `api/src/bracc/services/despesas_service.py` (ou service separado).
- **Flag nova** em `perfil_service.py`: `is_senador_federal` (label
  `:FederalLegislator` + `cargo == 'senador'` nos props, confirmar como
  o pipeline TSE ja grava).
- **Branch novo** no `_build_aviso_despesas` com mensagem especifica
  ("Cota para Exercicio da Atividade Parlamentar dos Senadores
  (CEAPS)").
- **Query nova**: `api/src/bracc/queries/ceaps_senador.cypher`.
- **Registry**: `docs/source_registry_br_v1.csv` + runner entry +
  bootstrap contract.
- **Teste**: `etl/tests/test_senado_ceaps.py` +
  `api/tests/unit/test_despesas_service.py` (CEAPS branch) +
  `api/tests/unit/test_perfil_service.py` (branch `is_senador_federal`).

## Criterio de retomada

- Quando um senador GO aparecer no perfil via TSE ou quando o alerta
  de teto de campanha `Senador` aparecer e o usuario clicar pra ver
  "gastos com a politica" do mandato (hoje cai no fallback generico).
- Volume baixo de senadores GO (3 por legislatura) torna o ROI alto se
  combinado com (2) filtro GO.

## Onde tocar (resumo)

- Pipeline novo: `etl/src/bracc_etl/pipelines/senado_ceaps.py`
- Script download: `scripts/download_senado_ceaps.py`
- Service: `api/src/bracc/services/despesas_service.py::obter_ceaps_senador`
- Perfil: `api/src/bracc/services/perfil_service.py`
  (flag `is_senador_federal` + branch de `_build_aviso_despesas` +
  chamada do service na `obter_perfil`).
- Query: `api/src/bracc/queries/ceaps_senador.cypher`
- Registry: `docs/source_registry_br_v1.csv`,
  `etl/src/bracc_etl/runner.py`, `config/bootstrap_all_contract.yml`.

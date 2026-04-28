# Backfill `r.ano` em ~1,65M rels `DOOU` sem ano (pipeline `tse.py`) — bloqueia ampliar `validacao_tse` além de 2022

## Sintoma

`conexoes_service.classificar` recebe `ano_doacao=2022`
(`api/src/bracc/services/perfil_service.py:560`) e descarta
silenciosamente toda rel `DOOU` cuja `r.ano` seja `NULL` ou diferente
de 2022 (`api/src/bracc/services/conexoes_service.py:438-447`). Hoje a
maioria das `DOOU` no grafo está sem `ano`, então o card "Confere com
o TSE" da PWA vê 0 doações pra muitos candidatos mesmo quando o grafo
tem milhares de rels relevantes.

## Distribuição global no Neo4j local (2026-04-27)

| `r.ano` | n           |
|---------|------------:|
| **NULL** | **1.654.900** |
| 2020    | 63.692      |
| 2024    | 51.306      |
| 2022    | 24.024      |

Quebrando por rótulo do doador → ano:

| dlabel        | ano   | n           |
|---------------|-------|------------:|
| Person        | NULL  | 1.240.832   |
| Company       | NULL  | 414.068     |
| Person        | 2020  | 40.724      |
| Person        | 2024  | 30.703      |
| CampaignDonor | 2022  | 24.024      |
| Company       | 2020  | 22.968      |
| Company       | 2024  | 20.603      |

Os 24.024 com `ano=2022` vêm do `tse_prestacao_contas_go` (pipeline
moderno, carimba `r.ano`). Os 1,65M sem `ano` vêm do `tse.py`
(pipeline mais antigo que ingere TSE candidatos/doações sem carimbar
ano na rel).

## Causa-raiz

`etl/src/bracc_etl/pipelines/tse.py` constrói as rels `DOOU` sem
adicionar `ano` ao `SET r.<...>`. CSV TSE original tem `ANO_ELEICAO`
mas o transform/loader não propaga.

## Fix

1. **Pipeline `tse.py`** — adicionar `r.ano = row.ano` (do
   `ANO_ELEICAO`) no `SET` da query de `DOOU`. Validar com
   `pytest etl/tests` que o test que cobre esse pipeline ainda passa
   e que o `ano` aparece nas rels carregadas.

2. **Backfill das 1,65M rels existentes** — duas opções:

   **2a) Re-rodar `tse.py` em modo idempotente** com snapshots
   arquivados em `archival/tse/...`. Como o load usa MERGE por
   `donation_id`, o re-run só adiciona `r.ano` (não duplica rels).
   Custo: tempo de I/O dos ZIPs do TSE (vários GB). Risco: baixo.

   **2b) Backfill direto via Cypher** — pra cada `DOOU` sem `ano`,
   pegar `ano` do `:CampaignDonation` ligado por `donation_id` ou do
   `:Election` na cadeia `(d)-[:CANDIDATO_EM]->(:Election {ano})`.
   Custo: query única, ~30min em 1,65M rels com index. Risco: médio
   — depende de ter `:Election` consistente, e algumas rels antigas
   podem estar sem o caminho.

   Recomendação: 2a se o snapshot do `tse.py` ainda for parseável
   (verificar `archival/tse/`); senão 2b.

## Pré-requisito pra o que

* Card "Confere com o TSE" no PWA hoje só compara 2022. Pra estender
  pra 2014/2018/2020/2024 (`validacao_tse_service` aceita `ano`
  parametrizado), precisamos das rels com `ano` carimbado. Sem isso,
  `total_doacoes` do classificador zera pros anos ≠ 2022.

* `conexoes_service` filtro `ano_doacao` é guard contra somar
  doações de 2014+2018+2022 num mesmo `valor_total`. Sem o filtro
  voltaria o bug histórico de 201,6% de divergência (registrado em
  `todo-list-prompts/high_priority/debitos/investigar-duplicacao-doacoes-tse.md`).

## Relacionado

* Frente A do
  `todo-list-prompts/high_priority/debitos/tse-doou-campaigndonor-stubs-orfaos.md`
  resolve 2022 sozinha (já tem `r.ano`). Esta tarefa é a frente B
  (anos antigos).

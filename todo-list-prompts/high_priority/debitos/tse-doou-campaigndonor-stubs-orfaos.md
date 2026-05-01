# `tse_prestacao_contas_go` cria `:CampaignDonor` órfão por `donation_id` — classificador da API enxerga R$ 0 em 2022

> **Code-side: ✅ DONE (2026-04-29).** Os dois bugs em código foram fechados:
>
> - **Bug 1 (loader)** — commit `61196e7` (`feat(tse,api): :CampaignDonor stub merge + classifier dual-path PF/PJ`):
>   `tse_prestacao_contas_go.py` agora roteia `donation_rels` em 3 buckets por
>   `doador_tipo` antes do MERGE: PJ → `:Company {cnpj: <formatado>}`,
>   PF mascarado → `:CampaignDonor {doador_id: <CPF mascarado>}`,
>   anônimo → `:CampaignDonor` por donation_id (escopo pequeno, mantido como
>   fallback). Implementação em `tse_prestacao_contas_go.py:982-1041`.
> - **Bug 2 (classifier)** — mesmo commit; `conexoes_service.classificar`
>   agora trata `target_type in ("person", "campaigndonor")` no mesmo branch
>   PF (`conexoes_service.py:526-533`). Card "Confere com o TSE" passa a ler
>   stubs PF mascarados como pessoa física.
> - **Backfill script** — commit `45fd486` entrega
>   `scripts/backfill_doou_campaign_donor_stubs.py`. Re-aponta DOOU dos
>   `:CampaignDonor` PJ pra `:Company {cnpj}`, agrega PF mascarado por
>   `doador_id`, deixa anônimos como estão. Idempotente, requer APOC.
>
> **Bug 3 (`r.ano = NULL` em 1,65M rels)** ✅ resolvido em 2026-04-30 via
> backfill Cypher `SET r.ano = r.year` (apoc.periodic.iterate, 34 batches,
> 0 falhas). Distribuição final: 2024 = 1.169.638; 2022 = 548.583;
> 2020 = 63.692; NULL = 0. Card "Confere com TSE" passa a cobrir 2020/2024.
>
> **✅ Backfill operacional rodado (2026-04-30).** Estado atual no Neo4j local:
> - PJ: 0 stubs (já tinha sido migrado pra :Company antes / pipeline novo já merge correto)
> - PF mascarado: 1.424 stubs, 12.193 rels, 1.424 doador_ids únicos (1:1, sem duplicatas pra agregar)
> - Desconhecido: 274 stubs, 274 rels (caminho frio, fica como está por design)
>
> **Validação** — `/politico/4:da0ec56f-cb5d-454a-b730-78a989eacdb6:7089`
> (Amilton):
> - `total_doacoes` saiu de R$ 0 → **R$ 843.001,44** (classifier agora
>   enxerga `:CampaignDonor` PF mascarado ✅).
> - `validacao_tse.status` = `"divergente"` com `direcao: "excesso_ingestao"`.
>   TSE declara R$ 421,5k mas grafo tem R$ 843k (contém :Company 2022 = R$ 222,9k
>   que provavelmente é repasse partidário, e :CampaignDonor 2022 = R$ 620,1k).
>   **Esse é outro bug**, ortogonal ao stub fix — agora vira escopo separado:
>   ou o `total_declarado_tse` exclui repasse partidário do somatório, ou o
>   loader duplica receita. Não cabe neste TODO; criar TODO novo.

## Sintoma

PWA do Amilton Batista de Faria Filho (CPF `002.180.041-33`,
elementId `4:da0ec56f-cb5d-454a-b730-78a989eacdb6:7089`) — relatado
em 2026-04-27 com 3 políticos atingidos:

* **Card "Confere com o TSE? Divergência grande de 100%"** — diz
  Declarado ao TSE R$ 421,5 mil vs Ingerido no sistema **R$ 0,00**.
* **Card "Contas batem?"** — diz receitas R$ 421,5 mil vs despesas
  R$ 412,0 mil (diferença 2,3%, "ok").

Os dois cards medem coisas diferentes (Card 1 = TSE escalar vs grafo;
Card 2 = TSE escalar vs TSE escalar — não toca grafo). O Card 1 está
errado: o candidato TEM 91 rels `DOOU` no grafo somando R$ 1.010.743,64,
mas o `conexoes_service.classificar` enxerga R$ 0.

## Causa-raiz

Quebra do `DOOU` por rótulo do doador no grafo local (Amilton):

| Rótulo do doador | rels | total       | `r.ano` |
|------------------|-----:|------------:|:-------:|
| `:CampaignDonor` (stub) | 68 | R$ 843.001,44 | 2022 |
| `:Person` (real)        | 14 | R$ 159.390,00 | NULL |
| `:Company` (real)       | 3  | R$ 8.352,20   | NULL |

Em escala global no Neo4j local:

| dlabel        | ano   | n           |
|---------------|-------|------------:|
| Person        | NULL  | 1.240.832   |
| Company       | NULL  | 414.068     |
| Person        | 2020  | 40.724      |
| Person        | 2024  | 30.703      |
| **CampaignDonor** | **2022** | **24.024** |
| Company       | 2020  | 22.968      |
| Company       | 2024  | 20.603      |

**100% das rels `DOOU` com `ano=2022`** apontam pra `:CampaignDonor`
stub — ou seja, o card de divergência TSE está errado pra TODO
candidato 2022 do grafo, não só pros 3 que a usuária encontrou.

### Bug 1 — loader do `tse_prestacao_contas_go.py` cria stub por
`donation_id` em vez de mergir doador

`etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py:970-986`:

```cypher
UNWIND $rows AS row
MATCH (p:Person {cpf: row.target_key})
MERGE (p)<-[r:DOOU {donation_id: row.donation_id}]-(d)
ON CREATE SET d:CampaignDonor,
   d.doador_id = row.source_key,
   d.doador_tipo = row.doador_tipo
SET r.valor = ..., r.ano = ..., ...
```

`d` está **não-vinculado** dentro do `MERGE`, então cada `donation_id`
gera um novo `:CampaignDonor` órfão. Mesmo doador (mesmo CPF/CNPJ) com
N doações vira N stubs. Os stubs:

* Não mergem com o `:Person`/`:Company` real que já existe no grafo
  (pipelines CNJ, RFB, TSE candidatos, etc).
* O CPF/CNPJ aparece só como `d.doador_id` (string), não como `d.cpf`
  ou `d.cnpj` — então o classificador não consegue agregar nem
  gambiarrando.

### Bug 2 — `conexoes_service.classificar` não reconhece `:CampaignDonor`

`api/src/bracc/services/conexoes_service.py:427-590` só processa
`target_type in {"company", "person"}`. Como `head(labels(:CampaignDonor))
= "CampaignDonor"` e `norm_type` lowercase pra `"campaigndonor"`, todas
as 68 rels caem no fallback "DOOU mas target inesperado: ignora"
(linha 591) silenciosamente.

### Bug 3 — `r.ano = NULL` em rels antigas

As 14+3 rels com target `:Person`/`:Company` (que o classificador
RECONHECERIA) têm `r.ano = NULL` — vêm do pipeline `tse.py` (não do
`tse_prestacao_contas_go`). O filtro `ano_doacao=2022` em
`conexoes_service.py:438-447` descarta `r.ano = NULL` porque
`None != 2022`.

Escopo: 1.654.900 rels `DOOU` no grafo local com `ano = NULL`.

## Frentes de fix

### A) Loader do `tse_prestacao_contas_go` deve mergir doadores reais
**(prioritário — resolve o card 2022 sozinho)**

Trocar o `MERGE ... (d)` solto por roteamento por `doador_tipo`:

```cypher
// Doador PJ — mergir no :Company existente por CNPJ formatado.
WITH row WHERE row.doador_tipo = 'pj'
MATCH (p:Person {cpf: row.target_key})
MERGE (d:Company {cnpj: row.cnpj_formatted})
MERGE (p)<-[r:DOOU {donation_id: row.donation_id}]-(d)
SET r.valor = ..., r.ano = ..., ...
```

```cypher
// Doador PF com CPF pleno — mergir no :Person existente.
// (Quando TSE 2022+ trouxer CPF pleno na CSV de receitas, o que é
// raro pós-mascaramento. Caminho frio mas correto quando aparece.)
WITH row WHERE row.doador_tipo = 'pf' AND row.cpf_pleno <> ''
MATCH (p:Person {cpf: row.target_key})
MERGE (d:Person {cpf: row.cpf_pleno})
MERGE (p)<-[r:DOOU {donation_id: row.donation_id}]-(d)
SET r.valor = ..., ...
```

```cypher
// Doador PF mascarado / anônimo — fallback :CampaignDonor agrupado por
// doador_id (não donation_id), pra que múltiplas doações do mesmo
// doador caiam no mesmo stub.
WITH row WHERE row.doador_tipo IN ['pf','desconhecido']
  AND coalesce(row.cpf_pleno,'') = ''
MATCH (p:Person {cpf: row.target_key})
MERGE (d:CampaignDonor {doador_id: coalesce(row.source_key, '')})
MERGE (p)<-[r:DOOU {donation_id: row.donation_id}]-(d)
SET r.valor = ..., ...
```

Se mantivermos `:CampaignDonor` como rótulo de fallback, o
`conexoes_service` precisa aprender a ler ele (ou agregamos por
`doador_tipo` no rel direto). Caminho mais simples: trocar `:CampaignDonor`
mascarado por `:Person {cpf: <mascarado>}` — preserva o `target_type=person`
do classificador. (Risco: colidir com `:Person` de candidato que tem
CPF mascarado — Branch D do `perfil_politico_connections.cypher`.)

**Backfill** das `:CampaignDonor` stubs já existentes: re-apontar
`DOOU` da stub pra `:Company {cnpj=stub.doador_id (formatado)}` quando
`doador_tipo='pj'`; pros PFs mascarados, agregar stubs por
`doador_id` e deixar ou converter pra `:Person`. Detach delete dos
stubs órfãos no fim.

### B) Backfill `r.ano` nas 1,65M rels antigas

Pipeline `tse.py` precisa carimbar `ano` (eleição) no `DOOU`. CSV TSE
original tem `ANO_ELEICAO`; reprocessar do snapshot ou backfill pelo
ano do `:Election` linkado. Necessário pra ampliar `validacao_tse`
(hoje só 2022) pra 2014/2018/2020/2024.

Frente A sozinha resolve o card que a usuária está vendo (2022).
Frente B é pré-requisito pra estender o cross-check pra outros anos.

## Validação após fix

Curl no `/politico/4%3Ada0ec56f-cb5d-454a-b730-78a989eacdb6%3A7089`
deve retornar `total_doacoes` ≈ R$ 421,5 mil (bate com `total_tse_2022`)
e `validacao_tse.status = "ok"`. PWA: card "Confere com o TSE" deve
ficar verde.

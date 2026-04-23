# Contador "X conexões" no `/politico` soma coisas que nunca aparecem nas listas

## Contexto

No endpoint `/politico/{id}` (`api/src/bracc/services/perfil_service.py`)
o campo `conexoes_total` é calculado via `len(conexoes_norm)` e depois
exibido na PWA como "X conexões" na seção de perfil do político. O
`conexoes_norm` é o resultado de `_adapt_connections` sobre o retorno
bruto do Cypher `perfil_politico_connections.cypher`, que traz **toda
aresta que toca qualquer nó-irmão do cluster canônico**.

O problema: a seguir, `conexoes_service.classificar` (`api/src/bracc/services/conexoes_service.py:321`)
separa essas arestas em 7 categorias
(`empresas`, `contratos`, `doadores_empresa`, `doadores_pessoa`,
`socios`, `familia`, `emendas`), e **apenas essas 7 listas viram
conteúdo visível**. Arestas que não caem em nenhuma categoria ficam
invisíveis — mas já foram contadas em `conexoes_total`.

## Caso concreto — Adriano do Baldy (id_camara=121948)

Medido no Neo4j local em 2026-04-22:

| Tipo de aresta | Count |
|---|---|
| `INCURRED` → `LegislativeExpense` (CEAP) | 1.386 |
| `AUTOR_EMENDA` → `Amendment` | 84 |
| **Total** | **1.470** |

A PWA mostra **"1.470 conexões"** em destaque. Mas:

- As 1.386 CEAP não caem em nenhum branch do `classificar` — viram 0
  em todas as 7 listas.
- As 84 emendas caem no bucket `emendas` do `classificar`, mas esse
  bucket é **descartado** pelo endpoint (o campo top-level `emendas`
  da resposta vem de query dedicada, não do classificar).
- Resultado: `empresas=0, contratos=0, doadores_empresa=0,
  doadores_pessoa=0, socios=0, familia=0`.

Do ponto de vista de UX: a usuária lê "1470 conexões" e rola a tela
esperando achar algo ligado — não acha nada, sensação de "os dados
não se ligam".

## Impacto

Todos os perfis de parlamentar onde a massa de arestas é CEAP/emendas
exibem esse teatro. Conferido manualmente em 2026-04-22:

- Adriano do Baldy (FederalLegislator): 1.470 contador, 0 em todas as
  listas de conexão.
- Ronaldo Caiado (Senator): 178 contador, 4 empresas doadoras + 47
  pessoas doadoras aparecem (caso saudável — tem base TSE ingerida).
- Flavia Morais (FederalLegislator): 618 contador, 2 empresas + 3
  pessoas (saudável também — tem `:Person` TSE irmão com DOOU).

O sintoma só aparece quando o cluster do deputado não tem `:Person`
TSE com doações costurado (ver débito irmão
`tse-orfaos-federais-go.md`).

## Fixes possíveis

### A. Trocar o contador por um detalhamento honesto (recomendado)

Em vez de "X conexões", mostrar as categorias reais que a UX já
renderiza, pré-calculadas no backend:

- "84 emendas" (vem do campo `emendas` top-level — já existe)
- "1.386 gastos de gabinete" (vem do `despesas_gabinete` — já existe)
- "4 empresas doadoras" (vem do `doadores_empresa`)
- "47 pessoas doadoras" (vem do `doadores_pessoa`)
- "0 sócios, 0 familiares" (ou suprimido se zero)

Remove o campo enganoso `conexoes_total` do response, ou renomeia
pra `conexoes_brutas` (uso debug apenas) e não expõe na PWA.

### B. Ampliar o `classificar` pra reconhecer CEAP

Adicionar branch pra `target_type == "legislativeexpense"` classificando
em um bucket `gastos_gabinete`. Aí a lista cresce e o contador reflete
algo visível.

**Contra:** duplica a query dedicada de despesas gabinete (que já
roteia por federal/estadual/vereador em `perfil_service.py:545+`).
Não é compatível com a estrutura atual.

### C. Subtrair CEAP/emendas do contador

No `_adapt_connections` (ou pós-`classificar`), ignorar arestas cujo
target_type é `amendment` ou `legislativeexpense`. Aí `conexoes_total`
vira "# de conexões com pessoas/empresas/contratos" — mais próximo do
que o rótulo "conexões" sugere.

**Contra:** hidden knowledge. A regra de "CEAP/emenda não é conexão"
é semântica, não óbvia no código.

## Recomendação

Opção **A**. Editar `pwa/index.html` (modelo de perfil político) pra
parar de exibir `conexoes_total` como número grande e renderizar só
as categorias já disponíveis. Menor superfície de mudança, resolve o
sintoma direto.

## Prioridade

Alta — é um dos 3 bugs que fez a usuária reclamar em 2026-04-22 que
"os dados não se ligam" no app.

## Arquivos envolvidos

- `api/src/bracc/services/perfil_service.py:813` — calcula `conexoes_total`
- `api/src/bracc/services/conexoes_service.py:321` — `classificar()`
- `pwa/index.html` — renderização do card de perfil político

# `/buscar-tudo` devolve 2-3 linhas para o mesmo político (Person + cargo + Person TSE)

## Contexto

Em 2026-04-22 o fulltext `entity_search` passou a cobrir os labels
`FederalLegislator`, `StateLegislator`, `Senator`, `GoVereador` (commit
de hoje, a partir do fix do ícone "Pessoa publica" no perfil). Efeito
colateral: a busca agora retorna **tanto** o nó de cargo **quanto** os
`:Person` irmãos do mesmo cluster canônico.

Exemplo real (query "Flavia Morais", Neo4j local):

```
person            | FLAVIA MORAIS                      | Pessoa publica
federallegislator | FLAVIA MORAIS                      | Deputado(a) Federal - PDT
person            | FLAVIA CARREIRO ALBUQUERQUE MORAIS | Pessoa publica
```

Três linhas para a mesma pessoa. O cluster canônico tem `canon_camara_160598`
ligando o `:FederalLegislator` + os dois `:Person` (um com CPF pleno do
TSE, um "fantasma" sem CPF do pipeline da Câmara).

## Impacto

UX confusa — a usuária lê 3 resultados, tenta escolher o "certo", e não
há feedback de que os 3 levam ao mesmo perfil (clicar em qualquer um
resolve pro focal via Branch B do `perfil_politico_connections.cypher`).

Pré-fix era "1 linha só do :Person sem contexto"; pós-fix é "3 linhas
redundantes". Melhoramos o caso em que o FedLeg existia mas não aparecia;
agora precisamos dedup.

## Proposta

Pós-processar resultados no `pwa_parity.py::_run_search` ou
`_format_item`: depois de materializar a lista, fazer `collect` por
cluster canônico (`MATCH (n)<-[:REPRESENTS]-(cp)` — 1 Cypher extra por
request, barato). Para cada cluster, manter apenas o nó de maior
oficialidade (mesma ranking do `perfil_politico_connections.cypher`:
Senator > FederalLegislator > StateLegislator > Person). Nós sem
cluster passam direto.

Alternativa: incluir o `canonical_id` como campo retornado na query
`search.cypher`, ordenar + colapsar no Python. Menos Cypher extra.

## Prioridade

Alta — saiu de direito de trabalho de hoje com saldo UX negativo. Deve
fechar a mesma janela do débito `contador-conexoes-enganoso.md`.

## Arquivos envolvidos

- `api/src/bracc/queries/search.cypher` — potencialmente adicionar
  lookup de `canonical_id`
- `api/src/bracc/routers/pwa_parity.py` — `_run_search` / `_format_item`
- `api/src/bracc/queries/perfil_politico_connections.cypher` — referência
  da hierarquia de oficialidade

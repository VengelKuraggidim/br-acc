# 3 dos 17 deputados federais GO não têm `:Person` TSE no cluster canônico

## Contexto

Dos 17 `:FederalLegislator` de GO no Neo4j local (2026-04-22), **14 têm**
um `:Person` TSE irmão no cluster canônico com CPF preenchido e
relações `DOOU` apontando pra ele — esses mostram doadores no
`/politico/{id}` normalmente.

Os outros **3 não têm**: Adriano do Baldy (id_camara=121948), id_camara
204412 e id_camara 220571. O cluster deles tem apenas um `:Person`
"fantasma" sem CPF, sem nome resolvido pelo TSE e sem nenhuma `DOOU`
apontando pra ele.

Consequência: o perfil desses 3 deputados aparece com zero doadores
empresa, zero doadores pessoa, zero total de doações — parece vazio
mesmo tendo CEAP e emendas normais.

## Como ver

```cypher
MATCH (fl:FederalLegislator {uf:'GO'})<-[:REPRESENTS]-(cp)-[:REPRESENTS]->(p:Person)
OPTIONAL MATCH (d)-[:DOOU]->(p)
WITH fl.id_camara AS idc, p, count(d) AS doacoes_para
RETURN idc, p.cpf AS p_cpf, doacoes_para
ORDER BY idc;
```

Linhas com `p_cpf=NULL` e `doacoes_para=0` são os órfãos.

## Hipótese de causa

A regra de entity resolution `cpf_suffix_name` (criada 2026-04-22 em
`entity_resolution_politicos_go.py`, ver memória
`project_ceap_federal_ingerido`) casa Câmara (CPF mascarado `*****NN-NN`
+ nome abreviado) com TSE (CPF pleno + nome pleno). Funciona pra 14
casos, falha pros 3 órfãos provavelmente porque:

- **Adriano do Baldy**: pode ser primeiro mandato federal, sem
  candidatura TSE anterior ingerida. Ou o nome completo TSE
  ("Adriano José Ribeiro da Silva" ou similar) não compartilha tokens
  contentfuls com "Adriano do Baldy".
- **204412 e 220571**: não inspecionados em detalhe, podem ter
  homônimo no TSE que foi descartado pela trava anti-colisão
  (`name match >1`).

Alternativas a investigar:
- Buscar no TSE GO todos os candidatos a Deputado Federal das eleições
  2014/2018/2022 com id_candidato vs id_camara conhecido.
- Adicionar uma 4ª regra ER baseada em urna (nome de urna do Câmara +
  nome de urna do TSE, exato).
- Caso seja 1º mandato sem histórico TSE, aceitar que não haja doadores
  (não é bug, é ausência legítima de dado).

## Impacto

Baixo em volume (3/17 = 18%), alto em saliência — Adriano do Baldy é
o caso que o usuário testou primeiro e ficou com a impressão de
"nada se liga". Resolver eleva a qualidade percebida do perfil.

## Prioridade

Alta — faz parte do trio de bugs 2026-04-22 onde a usuária perdeu
confiança no app.

## Arquivos envolvidos

- `etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py` —
  adicionar regra 4 ou ampliar tolerância da `cpf_suffix_name`
- `api/src/bracc/queries/perfil_politico_connections.cypher` — o fix
  de "união de edges do cluster" já está correto; este débito é só de
  ingesta/ER.

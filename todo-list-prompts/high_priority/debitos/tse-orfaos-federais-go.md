# 4 dos 17 deputados federais GO não têm `:Person` TSE no cluster canônico

## Contexto

Dos 17 `:FederalLegislator` de GO no Neo4j local (re-auditado 2026-04-23),
**13 têm** um `:Person` TSE irmão no cluster canônico com CPF preenchido
e relações `DOOU` apontando pra ele — esses mostram doadores no
`/politico/{id}` normalmente.

Os outros **4 não têm**: id_camara 121948 (Adriano do Baldy), 204390
(Professor Alcides), 204412 (Dr. Zacharias Calil), 220571 (Daniel
Agrobom). O cluster deles tem apenas um `:Person` "fantasma" sem CPF,
sem nome resolvido pelo TSE e sem nenhuma `DOOU` apontando pra ele.
(Débito original listava 3; Professor Alcides foi adicionado depois.)

Consequência: o perfil desses 4 deputados aparece com zero doadores
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

## Diagnóstico por caso (atualizado 2026-04-23)

Investigado contra Neo4j local (busca por sufixo CPF + cargo "Deputado
Federal" entre `:Person` GO com sq_candidato):

| id_camara | Nome cargo | CPF mascarado | Sufixo | Match TSE encontrado |
|---|---|---|---|---|
| 204390 | PROFESSOR ALCIDES | `***.***.*31-49` | 3149 | **`092.426.431-49`, ALCIDES RIBEIRO FILHO** (Deputado Federal) |
| 204412 | DR. ZACHARIAS CALIL | `***.***.*01-00` | 0100 | **`118.330.501-00`, ZACARIAS CALIL HAMU** (Deputado Federal) |
| 121948 | ADRIANO DO BALDY | `***.***.*31-53` | 3153 | nenhum candidato Deputado Federal GO bate sufixo |
| 220571 | DANIEL AGROBOM | `***.***.*11-49` | 1149 | nenhum candidato Deputado Federal GO bate sufixo |

**Por que `cpf_suffix_name` falha pros 2 com match:**

- PROFESSOR ALCIDES: tokens cargo `["PROFESSOR", "ALCIDES"]` — `PROFESSOR`
  não aparece em `["ALCIDES", "RIBEIRO", "FILHO"]`. Falha o "todos os
  tokens contentfuls do cargo no Person".
- DR. ZACHARIAS CALIL: tokens cargo `["ZACHARIAS", "CALIL"]` (ignora
  "DR.") — `ZACHARIAS` não aparece em `["ZACARIAS", "CALIL", "HAMU"]`
  por diferença de grafia (Z vs ZH).

**Por que ADRIANO DO BALDY e DANIEL AGROBOM não têm match nenhum:**

Provavelmente 1º mandato federal eleito 2022 sem candidatura TSE GO
prévia ingerida no grafo. Pode ter sido suplente que assumiu, ou
candidato em outra UF. Ausência legítima de dado — não é bug.

## Fix proposto (regra 4 ER)

Adicionar fase 4 em `entity_resolution_politicos_go.py` antes da
`shadow attach`:

```
def _attach_cpf_suffix_cargo(persons_go, claimed):
    # Pra cada cluster com cargo de label conhecido (FederalLegislator,
    # StateLegislator, Senator) e CPF mascarado:
    #   candidates = persons_go com cpf_suffix matching + cargo_tse_2022
    #     mapeado pra mesmo nivel ("Deputado Federal" / "Deputado
    #     Estadual" / "Senador")
    #   se len(candidates) == 1 e nao claimed: attach (conf=0.85,
    #     method="cpf_suffix_cargo")
    #   se >1: audit
```

Cuidados:
- Confiança menor que `cpf_suffix_name` (0.92) porque não valida
  tokens de nome — só sufixo CPF + categoria cargo. Sufixo de 4 dígitos
  tem 1/10000 de colisão por candidato; com `~5k Person` GO Deputado
  Federal acumulados, esperar ~0-1 falsos por sufixo. A trava de
  "match único" segura.
- Mapeamento estrito: `cargo_tse_2022 == "Deputado Federal"` casa com
  `:FederalLegislator`. Não tentar fuzzy.
- Rodar contra local primeiro pra contar quantos clusters mudam de
  estado. Resultado esperado: +2 attaches (ALCIDES, ZACHARIAS) sem
  regressão nos 13 casos saudáveis.

Esforço estimado: 45-60min (codar + testes + rerun ER local + auditar).
Casos restantes (Adriano, Daniel) = ausência legítima, fora de escopo
do fix.

## Workaround sem fix

Manter como débito P1 e aceitar que 4/17 perfis de federal GO mostrem
zero doadores. Resolver o caso ALCIDES/ZACHARIAS dobra a cobertura
pra 15/17.

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

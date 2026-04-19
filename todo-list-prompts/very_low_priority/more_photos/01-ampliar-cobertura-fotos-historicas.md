# Ampliar cobertura de fotos para políticos históricos GO — ⏳ PENDENTE (2026-04-19)

> Cobertura atual de foto em `:Person` GO está em **~50,7%**
> (2.156 de 4.249 ex-candidatos em 2026-04-19). O gap é quase todo
> **pré-2018**: o CDN do TSE (`divulgacandcontas.tse.jus.br/.../img/`)
> só hospeda foto consistentemente a partir de 2018, e o Wikidata só
> resolve nomes famosos.

## Contexto

Caso motivador: busca por `"marconi perillo"` devolve 7 resultados; 3
aparecem com placeholder gradiente (sem `foto_url` nem `url_foto` em
`:Person`):

- **NILTON PERILLO RIBEIRO** — candidato em **2002** (CPF
  `123.703.711-53`), filiado PT (`tse_filiados` 2007), doador em
  2002/2006. Perfil no PWA mostra campanha financiada, mas foto não
  existe no divulga atual.
- **MARCONI JOSE CRUZ** e **MARCONI MOURA DE LIMA** — idem: Persons
  criadas por `CANDIDATO_EM`/`DOOU` antigos, `source_id=NULL`, ciclo
  eleitoral pré-2018.

Root cause já auditada: nem placeholder defeito nem pipeline quebrado.
É limitação da fonte primária (portal novo do TSE não tem foto de
eleições antigas) + Wikidata só cobrir Q-ids conhecidos.

## O que já existe (não duplicar)

Pipelines de foto já implementados e rodando em `scripts/refresh_photos.py`:

- `camara_politicos_go` — deputados federais GO ativos (greenfield).
- `alego_deputados_foto` — deputados estaduais GO ativos.
- `senado_senadores_foto` — senadores ativos.
- `tse_candidatos_foto` — candidatos TSE via URL canônica
  `{cd_eleicao}/{sq_candidato}/{uf}`, **só 2018/2020/2022/2024**.
  Hardcoded em `_ANO_CD_ELEICAO`. Filtro defensivo de placeholder TSE
  (SHA `267865f1...`) já ativo.
- `wikidata_politicos_foto` — fallback P18 via SPARQL. Stop em
  ambiguidade (>1 hit pula, log warning). Cobre ex-governadores/
  ex-senadores famosos (Marconi Perillo Q6757791, Iris Rezende, etc.).
- `propagacao_fotos_person` — costura foto de labels de cargo
  (`:FederalLegislator`, `:StateLegislator`, `:Senator`) pro `:Person`
  homônimo. Idempotente, stop em ambiguidade.

Arquivos relevantes:

- `etl/src/bracc_etl/pipelines/tse_candidatos_foto.py` — seção
  "Mapeamento ano → cd_eleicao" é onde novos IDs históricos entrariam.
- `etl/src/bracc_etl/pipelines/wikidata_politicos_foto.py` — estratégia
  SPARQL + etiqueta (User-Agent, throttle 1s).
- `pwa/index.html:1558-1563` — consumidor final (`item.foto_url`).
- `api/src/bracc/routers/pwa_parity.py:119-121` — API surfacing.

## Missão

Fechar (parte d)o gap pré-2018 sem violar CLAUDE.md §3 ("nunca chutar/
acusar/inventar"). Cada foto adicionada precisa ter proveniência
rastreável + archival da fonte original.

Vetores candidatos (explorar em ordem de custo/retorno):

1. **TSE histórico — dados abertos de candidaturas antigas**
   (`cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/`). Zips por
   ano/UF com colunas incluindo às vezes `NM_URL_FOTO` para ciclos
   2014/2010/2006. Validar empiricamente contra GO 2002/2006/2010 — se
   existir, estender `tse_candidatos_foto._ANO_CD_ELEICAO` ou criar
   pipeline `tse_candidatos_foto_historico` que lê do ZIP em vez de
   URL canônica do divulga.

2. **Câmara histórica** — API de deputados federais tem imagem pra
   legislaturas passadas (`/api/v2/deputados/{id}`). O pipeline
   `camara_politicos_go` hoje só puxa ativos. Estender pra varrer
   legislaturas 51-56 (1999-2022) buscando ex-deputados federais GO
   ainda como `:Person` sem foto. Novo pipeline
   `camara_politicos_go_historico` ou extensão de parâmetro.

3. **Senado histórico** — API `/dadosabertos/senador/lista/afastado`
   + `/senador/{id}` tem foto. Similar ao vetor 2.

4. **Wikidata expansão** — revisar o SPARQL atual. O fallback pula
   quando >1 hit; muitos ex-candidatos municipais são homônimos. Não
   mexer na política de stop — mas adicionar filtro extra por
   `wdt:P39` (cargo ocupado) ou `wdt:P768` (candidato em) quando
   disponível pode desambiguar legitimamente. Cuidado: expansão do
   SPARQL aumenta rate limit e risco de match errado.

5. **Portais oficiais estaduais históricos**
   (ex-governadores/vice-governadores GO via `goias.gov.br`
   "Galeria de governadores") — scraping dirigido, 1 fonte por lista
   pequena. Baixo retorno vs. esforço.

Fora de escopo destes 5 vetores (risco > benefício):

- News photo APIs (Google Images, Bing) — licenciamento e ruído
  (fotos de homônimos). **NÃO implementar**.
- OCR de boletins de urna/material de campanha escaneado — esforço
  muito alto pra dezenas de fotos.
- Scraping de sites partidários — fragmentado, autenticação/captcha,
  vida útil curta.

## Contrato mínimo por vetor implementado

Cada pipeline novo deve:

- Subclasse `Pipeline` (`etl/src/bracc_etl/base.py`), `run_id`
  canônico, `extract/transform/load`.
- Cada HTTP fetch via `archive_fetch(...)` com `source_id` dedicado
  (ex: `tse_candidatos_foto_historico`, `camara_deputados_historico_foto`).
- `:Person` alvo só é tocado se `foto_url IS NULL AND url_foto IS NULL`
  (idempotência; respeita pipelines upstream).
- Stop em ambiguidade: ≥2 `:Person` casando com o mesmo match key →
  skip com warning (sem mesclar, sem escolher "o mais provável").
- Filtro de placeholder/silhueta (estender o SHA-check de
  `tse_candidatos_foto` se a nova fonte tiver seu próprio placeholder).
- Registry entry em `docs/source_registry_br_v1.csv` + runner entry
  + bootstrap contract + tests em `etl/tests/` com
  `TestArchivalRetrofit`.
- Propagação pra `:Person` via `propagacao_fotos_person` — já genérico,
  só precisa rodar depois. Confirmar que matcher por `name` do
  propagador cobre o label de cargo novo (se houver).

## Trade-offs explicitamente aceitos

- **Cobertura nunca chega em 100%**: candidatos municipais obscuros de
  2000/1996 não têm foto em lugar nenhum digital. Aceitável porque o
  placeholder gradiente já é UX razoável e nunca é acusatório.
- **Custo de banda**: fetch de milhares de fotos históricas é
  ~centenas de MB em archival. `BRACC_ARCHIVAL_ROOT` precisa ter
  espaço; rodar offline/batch, não em request path.
- **Re-run caro**: cada novo ciclo do pipeline toca só quem ainda não
  tem foto (idempotência), então re-runs são quase-no-op. OK.

## Por que very low priority

- Falta de foto nunca bloqueia feature crítica. Placeholder funciona,
  UX é ok, usuário navega normalmente.
- Débitos high/medium (TCE-GO, ALEGO verba indenizatória, tetos
  campanha 2026, separar doadores de empresas) têm impacto muito
  maior em rastreabilidade/compliance.
- Cada vetor acima é um pipeline novo com custo ≥1 dia +
  manutenção. Só faz sentido quando high/medium estiverem fechados
  OU quando uma lane autônoma ociosa precisar de trabalho.

## Critério de sucesso (quando for atacado)

- Cobertura GO sobe de 50,7% para ≥70% (ganho marginal mensurável).
- Nenhum `:Person` recebe foto com `match_confidence < high` sem
  revisão humana (CSV de override se necessário, espelhando o pattern
  de `very_low_priority/name_corrections/01-apelidos-campanha-override-csv.md`).
- Audit log novo (`foto_historica_applied` / `_skipped_ambiguidade` /
  `_skipped_placeholder`) permite operador revisar mensalmente.

## Referência rápida para quem pegar esta TODO

Query pra medir o gap atual:

```cypher
MATCH (p:Person) WHERE p.uf = 'GO'
RETURN count(p) AS total_go,
       sum(CASE WHEN p.foto_url IS NOT NULL OR p.url_foto IS NOT NULL
                THEN 1 ELSE 0 END) AS com_foto,
       sum(CASE WHEN EXISTS {(p)-[:CANDIDATO_EM]->()}
                THEN 1 ELSE 0 END) AS ex_candidatos;
```

Query pra listar top-N ex-candidatos GO sem foto, por ciclo:

```cypher
MATCH (p:Person)-[:CANDIDATO_EM]->(e:Election)
WHERE p.uf = 'GO' AND p.foto_url IS NULL AND p.url_foto IS NULL
RETURN e.year AS ano, count(p) AS sem_foto
ORDER BY ano DESC;
```

Use essa segunda query pra priorizar vetores: se 80% do gap está em
2002/2006, vetor 1 (TSE histórico) é o primeiro a tentar.

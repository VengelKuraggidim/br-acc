# Mostrar "status do julgamento das contas" do TSE (aprovada / desaprovada / ressalvas / rejeitada)

> **BLOQUEADO (2026-04-18)** — tentativa de implementação via API
> `divulgacandcontas.tse.jus.br/divulga/rest/v1/*` abortada. Ver seção
> **"Status técnico 2026-04-18"** abaixo. Requer playwright/headless
> (fora de escopo atual) ou NLP de PDFs de acórdãos TRE-GO.

## Contexto
Quando o TSE termina a análise da prestação de contas de cada candidato, ele emite um **parecer**: contas **aprovadas**, **aprovadas com ressalvas**, **desaprovadas** ou **rejeitadas**. Esse é o indicador mais forte de irregularidade que o próprio TSE identificou — é o "sinal vermelho gigante" que hoje não mostramos no perfil do político.

Hoje, só mostramos as receitas/despesas declaradas. Falta o **veredito**.

## Status técnico 2026-04-18 (investigação Opus 4.7)

Investigação completa da API TSE em 2026-04-18. **Endpoint JSON público direto**
**NÃO expõe o parecer de julgamento**. Evidências coletadas:

1. **Swagger não-oficial** (`augusto-herrmann/divulgacandcontas-doc`) lista
   apenas 8 endpoints REST. O único relacionado a contas é
   `GET /divulga/rest/v1/prestador/consulta/{eleicao}/{ano}/{UE}/{cargo}/90/90/{idCandidato}`.
2. **Chamada real** ao endpoint (testada com Ronaldo Caiado equivalente e
   outros deputados federais GO 2022, todos os cargos 3/5/6/7/8) retorna
   sistematicamente **`HTTP 200 Content-Length: 0`** — status de sucesso
   mas corpo vazio. Sugere que a resposta real é servida via JS client-side
   (SPA) ou requer token de sessão que o front-end injeta.
3. O endpoint `GET /candidatura/buscar/{ano}/{UE}/{eleicao}/candidato/{id}`
   devolve JSON populado com `numeroProcessoPrestContas` (número do
   processo no TRE) mas **nenhum campo de julgamento**: `codigoSituacaoCandidato`,
   `descricaoSituacao` (candidatura=Deferido), `codigoSituacaoPartido`,
   `processosCassacao[]`, `processosDesconstituicao[]` — todos sobre a
   candidatura, não sobre as contas.
4. O dataset oficial `dadosabertos.tse.jus.br` para 2022 publica receitas,
   despesas, bens, extratos bancários e CNPJs — **sem parecer**.
   Confirmado via https://dadosabertos.tse.jus.br/dataset/dadosabertos-tse-jus-br-dataset-prestacao-de-contas-eleitorais-2022.
5. Busca por `stJulgCc`/`dsJulgCc`/`situacaoJulgamento` em documentação
   e exemplos públicos retornou zero hits.

### Decisão

- **NÃO implementar** pipeline ETL ou service nesta fase. Os guardrails
  do projeto (`feedback_everything_automated.md`) exigem aquisição via
  `script_download`/ETL batch; scraping live está vetado.
- Se o usuário precisar desta feature, os caminhos viáveis são:
  1. **Playwright/headless** contra o front-end da SPA (fora do escopo
     atual de `script_download`, viola "everything automated" simples).
  2. **NLP de PDFs de acórdãos TRE-GO** (baixa cobertura, alto custo de
     parsing; cada acórdão é um PDF digitalizado com estrutura variável).
  3. **LAI à ASEPA** (Assessoria de Exame de Contas Eleitorais e
     Partidárias) pedindo dump CSV. Canal burocrático, não automatizável.

### Próximos passos sugeridos

- Manter este prompt arquivado em `high_priority/` como débito conhecido.
- Reavaliar em 2026-10 (pós-eleições 2026) se o TSE publicar dataset novo.
- Quando/se o dataset estruturado existir: pipeline ETL batch similar a
  `tse_prestacao_contas_go.py` (filtro UF=GO, archival do ZIP, carimbo
  `status_contas_tse_{YYYY}` / `data_julgamento_tse_{YYYY}` / `url_fonte_status_{YYYY}`
  em `:Person`), depois service + alerta grave + card PWA. Sem retrabalho
  do schema de dados: `StatusContasTSE` model proposta abaixo já cobre
  o contrato.

## Desafio de dados
Pesquisei em 2026-04-17 o Portal de Dados Abertos do TSE (`dadosabertos.tse.jus.br`) e a conclusão é:
- O dataset **"Prestação de Contas Eleitorais 2022"** tem apenas receitas, despesas, extratos bancários e CNPJs — **não tem parecer de julgamento**.
- O parecer técnico é elaborado pela ASEPA (Assessoria de Exame de Contas Eleitorais e Partidárias) e publicado caso a caso em acórdãos dos TREs/TSE.
- O TSE leva **anos** pra consolidar pareceres pós-eleição (muitos de 2022 ainda estavam em análise em 2026).

## Fontes possíveis (em ordem de viabilidade)
1. **Portal de Consulta Pública do TSE (scraping)**: cada candidato tem uma página em `divulgacandcontas.tse.jus.br` com status da prestação de contas. Scraping sob demanda pros candidatos abertos no perfil (cache local).
2. **Acórdãos TRE/TSE (NLP)**: PDFs públicos dos acórdãos de julgamento. Parseá-los é trabalho grande, baixa cobertura.
3. **Base dos Dados / projetos de pesquisa**: `basedosdados.org` às vezes consolida datasets complementares — checar se tem parecer de contas.
4. **LAI**: pedir à ASEPA o arquivo estruturado de pareceres via lei de acesso à informação.

## Arquivos relevantes (no fork)
- `backend/apis_externas.py` — adicionar `buscar_status_contas_tse(sq_candidato, ano)` que faz scraping/cache do portal TSE.
- `backend/app.py` `PerfilPolitico` — adicionar `status_contas_tse: StatusContasTSE | None = None` com campos `status` ("aprovada"/"aprovada_com_ressalvas"/"desaprovada"/"rejeitada"/"pendente"), `data_julgamento`, `url_fonte`.
- `backend/app.py` `gerar_alertas_completos` — quando status = "desaprovada" ou "rejeitada", gerar alerta vermelho prioritário.
- `pwa/index.html` — card destacado no topo do perfil quando contas desaprovadas/rejeitadas (urgência visual).

## Missão (em ordem)
1. **Mapear endpoint do TSE** (15 min): abrir `divulgacandcontas.tse.jus.br`, procurar deputado de exemplo, inspecionar rede pra achar o JSON. Testar URL direta por `SQ_CANDIDATO`.
2. **Implementar scraping + cache** (~1.5h):
   - `httpx` GET, parse do JSON/HTML, extrair status.
   - Cache SQLite/Redis com TTL 30 dias (status raramente muda depois de emitido).
   - Rate limit cortês (1 req/s).
3. **Backend integration** (~30min): chamar lookup ao montar perfil, só pros candidatos abertos (não pra listagem).
4. **PWA** (~30min): card vermelho no topo se "desaprovada"; verde discreto se "aprovada"; amarelo se "com ressalvas"; cinza "pendente" se não encontrado.

## Critérios de aceite
- Perfil do Caiado 2018/2022 mostra status oficial das contas (exemplo referência).
- Candidato com contas **desaprovadas** aparece com alerta vermelho no topo e badge visível na lista de busca.
- Cache evita bater o TSE repetidamente.
- Se o TSE estiver fora do ar, o perfil carrega normalmente (degradação silenciosa).

## Guardrails
- Respeitar robots.txt do TSE; rate limit.
- Não cachear dado pessoal sensível (só status público das contas).
- `make pre-commit` verde.

## Prioridade
Alta — é o sinal mais forte de irregularidade disponível em fonte oficial. Quando "desaprovada", o cidadão precisa saber na hora.

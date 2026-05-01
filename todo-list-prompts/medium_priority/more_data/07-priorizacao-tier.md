# Priorização das fontes não-carregadas

## Contexto

Snapshot **original 2026-04-19** do registry `docs/source_registry_br_v1.csv`
(127 entradas `in_universe_v1=true`) cruzado com estado do Neo4j local.
Resultado: **94 fontes catalogadas que nunca rodaram nem baixaram**.

**Atualização 2026-04-22**: progresso significativo desde o snapshot — a
tabela PX (blockers conhecidos) foi reescrita no fim do doc pra refletir:

- 7 fontes que saíram do snapshot pra `loaded` (bndes, transferegov, inep,
  icij, cpgf, holdings, mides via conversão `script_download` ou fetch
  programático).
- 3 fontes GO que saíram de `not_loaded` pra `loaded`: `alego` (API JSON
  oculta via Angular bundle), `tcmgo_sancoes` (REST + scraper JSF
  bloqueado por robots.txt, REST entrega ~1.4k rows), `ssp_go` (parser
  `pypdf` dos boletins anuais de estatísticas criminais, ~1.4k rows).
- 1 fonte GO em `partial` (`tce_go` carrega decisões via
  `iago-search-api`; irregulares + fiscalizações continuam em Qlik —
  ver `debitos/tce-go-qlik-scraper.md`).
- 1 fonte federal deprecated: `querido_diario` substituída por
  `querido_diario_go` (commit `7208381`).
- Arquivos de TODO referenciados pela coluna "Débito em" foram
  **deletados** quando a fonte virou `loaded` ou quando o débito foi
  consolidado em nota diferente. Linha na tabela PX abaixo reflete só
  os débitos que **ainda existem**.

As seções P1 / P2 / P3 mantém a planilha de priorização intacta — são
guias pra futuros sprints, não invalidadas pelo progresso.

Escopo Fiscal Cidadão: rebrand de `brunoclz/br-acc`, entidades-alvo são
políticos/gastos GO; grafo ingere nacional pra cruzar conexões. Fontes
puramente estaduais de outros UFs têm impacto baixo salvo indicação
contrária (conexões cross-estado quando um político GO tem vínculo com
pessoa/empresa de outra UF).

Convenção de custo:

- **Baixo** — API JSON, download <1 GB, pipeline existente ou trivial
- **Médio** — CSV multi-GB, múltiplos endpoints, schema a mapear
- **Alto** — milhões de registros, múltiplas etapas, credenciais, joins
- **Blocker** — form-wall, CAPTCHA, `.7z`, credencial inacessível

Impacto descrito pelo que a fonte **entrega**, não pelo que "revelaria".
Tom neutro.

---

## Tier P1 — alto impacto, custo baixo (fazer primeiro)

Fontes já implementadas + baixado mas o `IngestionRun` nunca foi disparado,
OU fontes com pipeline existente e apenas precisam de rodada.

| Source | Entrega | Custo | Pipeline existe? |
|---|---|---|---|
| `dou` | Atos do Diário Oficial da União (nomeações, portarias, decretos) — cadência diária | Baixo (BigQuery) | sim (`etl/.../dou.py`) |
| `sanctions` | CEIS + CNEP sanções administrativas federais (CSV pequeno, portal transparência) | Baixo | sim (`sanctions.py`) |
| `ceaf` | Servidores expulsos por PAD (CGU) | Baixo | sim (`ceaf.py`) |
| `cepim` | ONGs impedidas de receber repasse federal | Baixo | sim (`cepim.py`) |
| `leniency` | Acordos de leniência firmados com o Estado | Baixo | sim (`leniency.py`) |
| `pep_cgu` | PEPs CGU — baseline doméstico | Baixo | sim (`pep_cgu.py`) |
| `opensanctions` | PEPs globais + listas internacionais | Baixo | sim (`opensanctions.py`) |
| `ofac` | Sanções OFAC US | Baixo | sim (`ofac.py`) |
| `eu_sanctions` | Sanções consolidadas UE | Baixo | sim (`eu_sanctions.py`) |
| `un_sanctions` | Sanções ONU | Baixo | sim (`un_sanctions.py`) |
| `world_bank` | Empresas inidôneas World Bank | Baixo | sim (`world_bank.py`) |
| `icij` | ICIJ offshore leaks (entidades + officers) | Baixo | sim (`icij.py`) |
| `holdings` | Estrutura de holdings Brasil IO | Baixo | sim (`holdings.py`) |
| `ibama` | IBAMA áreas embargadas | Baixo | sim (`ibama.py`) |
| `bndes` | Financiamentos BNDES (tomador, valor) | Baixo-médio | sim (`bndes.py`) |
| `bcb` | Processos administrativos sancionadores BCB | Baixo | sim (`bcb.py`) |
| `cvm_full_ownership_chain` | Cadeias de controle acionário CVM (derivado de `cvm_funds`) | Baixo | não (expansão sobre `cvm_funds.py`) |
| `stj_dados_abertos` | Decisões STJ — `implementation_state=implemented`, `load_state=not_loaded` | Baixo | sim (`stj_dados_abertos.py`) |
| `tesouro_emendas` | Execução de emendas Tesouro — `implemented`, `load_state=not_loaded` | Baixo | sim (`tesouro_emendas.py`) |
| `stf` | Dados abertos STF (base dos dados) | Baixo (BigQuery) | sim (`stf.py`) |

**Observação**: `sanctions`, `ceaf`, `cepim`, `leniency`, `pep_cgu` têm
prompts de `script_download` em
`todo-list-prompts/very_high_priority/script-download-conversions/easy-recovery/`
(ver PX abaixo). Rodar depois que conversão estiver feita, ou aceitar
`file_manifest` legado pra carga inicial.

---

## Tier P2 — alto impacto, custo médio/alto

Fontes com alto valor pra fiscalização GO mas exigem trabalho maior:
novo pipeline, credenciais, volume, ou scraping.

| Source | Entrega | Custo | Pipeline existe? |
|---|---|---|---|
| `bolsa_familia_bpc` | Pagamentos Bolsa Família + BPC mensais (CPF masked) | Alto (volume, masking) | não |
| `estban` | Saldos bancários por município BCB — base fiscal municipal | Médio | não |
| `if_data` | KPIs por instituição financeira BCB (trimestral) | Médio | não |
| `bcb_liquidacao` | Bancos em liquidação/intervenção BCB | Baixo | não |
| `cnciai_improbidade` | Condenações por improbidade CNJ/CNCIAI | Médio (API CNJ) | não |
| `carf_tax_appeals` | Recursos fiscais CARF | Médio | não |
| `anm_mining_rights` | Direitos minerários + titulares ANM | Médio | não |
| `mapbiomas_alertas` | Alertas de desmatamento MapBiomas (relevante cerrado GO) | Médio | não |
| `sicar_rural_registry` | Cadastro Ambiental Rural (limites + proprietários) | Alto (multi-estado, GB) | não |
| `receita_dirbi` | Declarações DIRBI de benefícios fiscais | Médio | não |
| `camara_votes_bills` | Votações + projetos Câmara Federal (cadência diária) | Médio | não |
| `senado_votes_bills` | Votações + projetos Senado Federal | Médio | não |
| `siga_brasil` | Traços de execução orçamentária federal SIGA Brasil | Médio | não |
| `state_portal_go` | Contratos + fornecedores + sanções GO (CKAN) — `implementation_state=implemented`, `load_state=not_loaded` | Médio | sim (`state_portal_go.py`) |
| `pncp` | PNCP nacional (completo, não só UF=GO) — `implementation_state=implemented`, `load_state=partial` | Médio | sim (`pncp.py`) |
| `camara_inquiries` | Requerimentos/inquéritos Câmara Federal — `implemented`, `load_state=partial` | Baixo-médio | sim (`camara_inquiries.py`) |
| `senado_cpis` | CPIs Senado — `implemented`, `load_state=partial` | Baixo-médio | sim (`senado_cpis.py`) |
| `siconfi` | Finanças municipais SICONFI (sem link CNPJ direto) — `partial` | Médio (API ORDS) | parcialmente (via `tcm_go.py`) |

---

## Tier P3 — baixo impacto pra escopo GO (adiar)

Fontes onde entidades-alvo GO raramente aparecem, OU informação
altamente agregada sem granularidade individual, OU concessões
setoriais específicas.

### Reguladoras setoriais (P3 no registry)

| Source | Entrega | Motivo de P3 |
|---|---|---|
| `anp_royalties` | Royalties petróleo/gás ANP | GO não é UF produtora relevante |
| `aneel_concessions` | Concessões energéticas | Cruzamento com empresários GO é raro |
| `antt_transport_concessions` | Concessões rodoviárias ANTT | Baixa intersecção com políticos GO |
| `ans_health_plans` | Operadoras de planos de saúde ANS | Regulatório, baixa intersecção |
| `anvisa_registrations` | Registros de produtos ANVISA | Regulatório, baixa intersecção |
| `anac_aviation_concessions` | Concessões aviação ANAC | Regulatório, baixa intersecção |
| `antaq_port_contracts` | Concessões portuárias ANTAQ | GO sem porto |
| `ana_water_grants` | Outorgas de uso de água ANA | Médio (cerrado GO tem conflito hídrico); pesquisar |
| `anatel_telecom_licenses` | Licenças telecom ANATEL | Regulatório |
| `susep_insurance_market` | Entidades do mercado de seguros SUSEP | Baixa intersecção |
| `icmbio_cnuc` | Unidades de conservação ICMBio | Baixa intersecção política |

### Portais estaduais de UFs fora-escopo (P2/P3)

| Source | Tier registry | Motivo |
|---|---|---|
| `state_portal_sp` | P2 | Fora de GO; relevante só se político GO tem vínculo SP |
| `state_portal_mg` | P2 | Fora de GO; fronteira com GO — talvez promover |
| `state_portal_ba` | P2 | Fora de GO |
| `state_portal_ce` | P2 | Fora de GO |
| `state_portal_pr` | P2 | Fora de GO |
| `state_portal_sc` | P2 | Fora de GO |
| `state_portal_rs` | P2 | Fora de GO |
| `state_portal_pe` | P2 | Fora de GO |
| `state_portal_rj` | P2 | Fora de GO; capital federal histórica pode capturar conexões |

### TCEs de UFs fora-escopo (P2/P3 — 25 estados)

Os 25 TCEs estaduais não-GO estão todos aqui. TCEs entregam sanções de
gestores municipais + contratos auditados da UF. Um político GO só
cairia aqui se tivesse filial/atuação fora do estado — caso raro. Se
quisermos cobrir todos os TCEs nacionalmente, virar um projeto
dedicado (5-10 pipelines de scraping, cada um com schema próprio).

Lista: `tce_sp`, `tce_pe`, `tce_rj`, `tce_rs`, `tce_mg`, `tce_ba`,
`tce_ce`, `tce_pr`, `tce_sc`, `tce_es`, `tce_mt`, `tce_ms`, `tce_am`,
`tce_pa`, `tce_ro`, `tce_rr`, `tce_ap`, `tce_to`, `tce_ma`, `tce_pi`,
`tce_rn`, `tce_pb`, `tce_al`, `tce_se`.

### Outras

| Source | Entrega | Motivo |
|---|---|---|
| `interpol_red_notices` | Alertas vermelhos Interpol (requer key) | Baixa probabilidade de match com políticos GO; custo de credencial |

---

## Tier PX — blocker conhecido (débito documentado)

Fontes que NÃO devem virar sprint antes do blocker sair. Cross-ref com
débitos em `todo-list-prompts/`. Tabela reescrita em 2026-04-22 pra
refletir apenas fontes **ainda em aberto** — o que virou `loaded` saiu
da lista.

| Source | Blocker | Débito ativo em | Ação |
|---|---|---|---|
| `caged` | `.7z` + form-wall; dep `py7zr` não aprovada | `very_high_priority/script-download-conversions/medium-tier/caged.md` | Esperar aprovação de dep ou mirror CSV oficial |
| `rais` | Multi-GB PDET behind login; basedosdados exige creds GCP | `very_high_priority/script-download-conversions/medium-tier/rais.md` | Creds GCP já liberadas (secretAccessor concedido 2026-04-22); resta dependência login PDET |
| `datajud` | Credenciais CNJ não operacionais em prod | `blocked_external` no registry | Retomar quando credencial prod sair |
| `tce_go` (irregulares + fiscalizações) | Qlik Sense WebSocket scrape, fragil + volumoso | `high_priority/debitos/tce-go-qlik-scraper.md` | Decisões já `loaded` via `iago-search-api`; completar Qlik quando ROI justificar |
| `tcmgo_sancoes` (impedidos de licitar) | `robots.txt` do subdomínio `tcmgo.tc.br` tem `Disallow: /` | `high_priority/debitos/tcmgo-impedidos-jsf-scraper.md` | Scraper entregue mas bloqueado por robots; fallback LAI |
| `ssp_go` (granularidade municipal) | SSP-GO só publica totais estaduais em PDF; RAI municipal só via LAI | `high_priority/debitos/ssp-go-granularidade-municipio.md` | Estadual `loaded`; municipal depende de LAI/SINESP/FBSP |
| `camara_goiania` | Portal Plone retorna stubs; dados reais só via HTML+PDF scraping | `medium_priority/debitos/camara-goiania-scraping.md` | Camada 1 (regex `ato_vereador` em `querido_diario_go`) entregue em 2026-04-22; camada 2 (scraper CMG completo) em aberto — rebaixada pra medium |
| `pncp` nacional | Volume (completo, não só UF=GO); janela dedicada | `high_priority/debitos/rodar-pipelines-pesados.md` | Pipeline existe; rodar em janela planejada |
| `comprasnet` | Volume ~6.4 GB; agendamento dedicado | `high_priority/debitos/rodar-pipelines-pesados.md` | OOM fix já aplicado em `0d407d5`, falta rodar |
| `pgfn` (full history) | ~1.2 GB, transform `iterrows()` ~10M rows (>1h) | `high_priority/debitos/rodar-pipelines-pesados.md` | Idem — janela dedicada |
| ~~Aura prod operações~~ | — | — | Todos os 3 débitos (source_id, ano DOOU, CEAP) resolvidos em 2026-04-22 |
| ~~`archival` root ownership~~ | — | — | `chown` aplicado + commit `7c1b872` fixou compose; falta só rerun fotos (ver memo `project_fotos_politicos_pendente`) |
| Pipelines TSE que exigem BigQuery | GCP creds | `medium_priority/reexecutar/01-tse_filiados.md` | IAM secretAccessor já concedido 2026-04-22 — destravado |

**Saíram da tabela desde 2026-04-19** (todos viraram `loaded` ou
`script_download` no registry + contract + data no grafo):
`bndes`, `transferegov`, `inep`, `icij`, `cpgf`, `holdings`, `mides`,
`siconfi` (parcial), `alego`, `tcmgo_sancoes` (REST/1.4k rows),
`ssp_go` (nível estadual), `tce_go` (decisões). O `querido_diario`
federal foi `deprecated` em `7208381` (substituído por `querido_diario_go`).
Débito `repopular-ceap-aura.md` resolvido em 2026-04-22: pipeline
`camara_politicos_go` já havia rodado (17 FedLeg GO + 12.080
`:LegislativeExpense` em prod), mas rels `:INCURRED` estavam sem props
(run anterior ao fix `21cc860`). Backfill Cypher preencheu
`tipo/ano/mes/valor_liquido`; API `/politico/<id_camara>` agora retorna
`despesas_gabinete` populado. Débito colateral
`meta-stats-legislative-expense-count.md` fechado em 2026-04-24: cypher
(`meta_stats.cypher:59`) e router (`meta.py:100-102`) já expunham o
campo antes desta auditoria — o md é que estava desatualizado.

Débito `backfill-ano-doou-rels.md` resolvido em 2026-04-22: 34.164/46.449
rels `:DOOU` em prod tinham `ano IS NULL` (todas com `year` preenchido —
pipelines TSE legados). Backfill `SET r.ano = r.year` via
`apoc.periodic.iterate` cobriu 100%. API `/politico/<eid>` agora devolve
doadores PJ/PF (validado: Jovair Arantes, CPF `040.359.761-72` → 4
doadores pessoa + 2 empresa, R$ 605k em 2022). Follow-up
`medium_priority/debitos/tightening-filtro-ano-doou.md` fechado em
2026-04-24 (commit `0c28ce5`): filtro agora descarta `ano IS NULL`
ativamente — falha rápido se pipeline futuro esquecer de carimbar.

Réplica do mesmo débito no Neo4j **local** resolvida em 2026-04-30 (TODO
`tse-doou-rels-sem-ano-backfill.md`): 1.654.900 rels `:DOOU` com
`ano IS NULL` (year=2022: 536.568; year=2024: 1.118.332) — vinham de runs
do `tse.py` antes do fix `7aca93b`. Mesmo backfill `SET r.ano = r.year`
via `apoc.periodic.iterate` (34 batches × 50k, 0 falhas). Distribuição
final no local: ano=2024 → 1.169.638; ano=2022 → 548.583; ano=2020 →
63.692; NULL → 0. Smoke test: Vanderlan Vieira Cardoso (Prefeito 2024)
agora soma R$ 6.906.500 em 6 rels DOOU 2024 — antes do backfill, filtro
`ano_doacao=2024` descartava todas. Card "Confere com TSE" do PWA passa
a funcionar pra 2020/2024 além de 2022.

Débito `09-perfil-sancoes-tce-embargos-cards.md` resolvido em 2026-05-01:
4 queries Cypher (`perfil_sancoes`, `perfil_tce_go_irregulares`,
`perfil_tcm_impedidos`, `perfil_embargos`), service unificado
`irregularidades_service.py` (4 funções cluster-aware que retornam lista
vazia silenciosamente quando sem registros), 4 models
(`SancaoCard/TceGoIrregularCard/TcmGoImpedidoCard/EmbargoCard` em
`api/src/bracc/models/perfil.py:516-606`), 4 campos novos no
`PerfilPolitico`, plumbagem em `perfil_service.py:761-787,983-986`, e
card único "Histórico de irregularidades" no PWA
(`pwa/index.html:2795-2918`) com 4 sub-blocos colapsáveis. **Decisões
de design vs. TODO original**: (1) o nó relevante pra "Decisões TCE-GO"
é `:TceGoIrregularAccount` via `:IMPEDIDO_TCE_GO`, não `:TceGoDecision`
(esse último são 10k decisões órfãs, sem rel com Person — provável
backlog futuro); (2) `:TcmGoImpedido` casa por nome (case-insensitive,
normalizado) porque `imp.document` vem mascarado pela fonte upstream
(`76***.***-**`) — risco de falso positivo em homônimos, card expõe
`fonte_url` pra verificação; (3) caminho 2-hop sugerido no TODO
(`Person→SOCIO_DE→Company→EMBARGADA→Embargo`) retorna 0 no local —
ficou só o caminho direto `(:Person)-[:EMBARGADA]->(:Embargo)` que tem
84k rels; (4) simplificação dos alertas agregados (sanção/impedido) NÃO
foi feita — alertas atuais têm escopo "qualquer conexão sancionada"
enquanto os cards cobrem só o cluster do próprio político; semânticas
não são equivalentes, simplificar regrediria informação. Smoke test
2026-05-01 (3 políticos): Mauro de Souza Junior → 1 sanção CEIS;
Alcides Rodrigues Filho → 2 embargos IBAMA Fazenda Santo Antônio (MT);
Adelina da Cunha Araujo → 2 contas julgadas irregulares TCE-GO
(processos 2005-2014); Adailton Vidal dos Santos → 1 impedimento
TCM-GO (Balancete Semestral, processo 2414/23-2). 27 testes unit
`test_perfil_service.py` passando. Validação UI no browser fica pendente
da usuária. Container `fiscal-bracc-api` precisou ser rebuilt+rerun
manualmente (memory `project_ceap_federal_ingerido` confirma que é
built image, não bind-mount); criado na rede `br-acc_default` porque o
`fiscal-neo4j` é do compose `br-acc` (legado da renomeação do
projeto).

Débito `08-perfil-historico-eleitoral.md` resolvido em 2026-04-30 (commit
`66f0076` "v157 - Dados politicos"): query
`perfil_historico_eleitoral.cypher` (cluster-walk via :CanonicalPerson →
:CANDIDATO_EM → :Election), service `historico_eleitoral_service.py`
(monta `CarreiraPolitica` com num_candidaturas, primeira/última eleição,
anos_carreira, cargos_distintos, lista de candidaturas e resumo
narrativo), models `CandidaturaTSE` + `CarreiraPolitica`
(`api/src/bracc/models/perfil.py:476-512`) e card "Carreira política
(TSE)" no PWA (`pwa/index.html:2746-2793`). **Divergência intencional vs.
TODO original**: não exibe "Eleito ✓ / Não eleito" porque 443k de 517k
rels `:CANDIDATO_EM` têm `r.situacao=NULL` no grafo local — afirmar
resultado seria errado pra maioria. Tooltip do card explica: "TSE
registra candidatura, não mandato exercido — eleito ou não eleito requer
cruzamento separado." Smoke test 2026-05-01: Vanderlan Vieira Cardoso
retorna 2 candidaturas (Prefeito Goiânia/GO 2024 e 2020), 4 anos de
carreira. Validação UI no browser fica pendente da usuária.

Débito `aura-prod-source-id-migracao.md` resolvido em 2026-04-22: dry-run
mostrou zero resíduos de `portal_transparencia` / `tribunal_superior_eleitoral`
no Aura prod (IngestionRun, nodes e rels). Slugs canônicos
(`transparencia`, `tse`) já populados. Migração aparentemente aplicada
antes ou o Aura prod foi populado direto com a versão pós-`d23baee`.

Débito `custo-mandato-municipal.md` MVP entregue em 2026-04-22: pipeline
novo `custo_mandato_municipal_go.py` (cargos `prefeito_goiania` +
`vereador_goiania`), 18 testes unit + paridade com `custo_mandato_br`,
integração em service/router/runner/registry. Vereador = 75% dep
estadual GO = R$ 26.080,98/mês × 35 cadeiras = R$ 10,95 mi/ano.
Prefeito sem valor (Lei Orgânica sem API — padrão do governador_go).
Rodado localmente (Docker Neo4j) + validado via API local. **Não rodado
em prod**: Aura Free atingiu 200.000 nodes — ver débito novo
`aura-free-quota-estourada.md`. Expansão pros 245 municípios GO
restantes migrou pra `medium_priority/debitos/custo-mandato-municipal-expansao.md`.

**Novo débito aberto 2026-04-22**: `aura-free-quota-estourada.md` —
qualquer ingestão futura em prod está bloqueada até cleanup (20k+ nós
descartáveis candidatos) OU upgrade pro Aura Professional.

---

## Distribuição final

- **P1**: 20 fontes (prioridade máxima, custo baixo) — entregam baseline de sanções administrativas multi-jurisdição + decisões STJ/STF + emendas Tesouro
- **P2**: 18 fontes (alto impacto, custo médio/alto) — exigem novo pipeline ou são sprint dedicado
- **P3**: 36 fontes (reguladoras setoriais + portais/TCEs de UFs fora-escopo) — adiar
- **PX**: 17 fontes (blocker/débito já documentado) — esperar blocker

Soma: 91. Restantes 3 do total 94 são casos-limite contados num tier
apenas (ex.: `pncp` aparece só em PX, `stj_dados_abertos` só em P1, etc.).

---

## Decisões pendentes (requer input editorial humano)

1. **Portais estaduais de UFs fronteiriças com GO** — MG, MT, MS, DF
   (ausente do registry), BA, TO. Estas UFs compartilham fronteira +
   fluxo econômico com GO. Política atual: P3 por "fora-escopo GO".
   Promover MG/MT/MS pra P2 porque políticos GO frequentemente têm
   negócios transfronteira? **Decisão de Fernando.**

2. **TCEs fronteiriços** (`tce_mg`, `tce_mt`, `tce_ms`, `tce_to`,
   `tce_ba`) — mesmo argumento. Promover pra P2?

3. **`state_portal_rj`** — Rio concentra bolsa de valores + várias
   sedes regulatórias federais. Políticos GO com cargo federal podem
   ter CNPJs/pessoas ligadas a RJ. Promover pra P1 por volume de
   cross-conexão esperado? **Decisão editorial.**

4. **`bolsa_familia_bpc`** — alto volume + CPF masked na fonte. Política
   LGPD atual mascara CPF na API (`mascarar_cpf` só últimos 2 dígitos).
   A fonte já vem mascarada (primeiros 3 e últimos 2 zerados). Carregar
   como `BenefitPayment` agregado por município + período OU individual
   com CPF masked de ponta a ponta? **Decisão de arquitetura + LGPD.**

5. **`sicar_rural_registry`** — muito alto impacto para Goiás (cerrado +
   disputas fundiárias + monitoramento de sobreposições), mas muito alto volume (GB por
   estado × 27 estados). Rodar **só UF=GO** (plano municipal) ou
   nacional? **Decisão de escopo.**

6. **`camara_votes_bills` + `senado_votes_bills`** — cadência diária +
   volume alto (~milhares de votações por ano × N deputados).
   Entregam comportamento legislativo (quem votou como). Carregar só
   deputados/senadores GO, ou todos? **Decisão de escopo GO.**

7. **`pesquisar`**: `receita_dirbi` (declarações de benefícios fiscais)
   — o schema público disponibiliza CNPJ do beneficiário? Confirmar
   antes de classificar como P2 firme.

---

## Como usar este documento

Futuros sprints (numerados 08+ em `todo-list-prompts/medium_priority/
more_data/`) devem puxar fontes deste tier em ordem P1 → P2, criando
prompts específicos por fonte (ou grupo afim, ex.: todas as sanções
internacionais em um só). Este documento NÃO cria esses prompts — é
só a matriz de priorização pra evitar decisão ad-hoc a cada sprint.

Re-rodar a consulta de "não-carregadas" trimestralmente: lista muda
conforme pipelines rodam e novas fontes entram no registry.

# Priorização das fontes não-carregadas

## Contexto

Snapshot 2026-04-19 do registry `docs/source_registry_br_v1.csv` (127
entradas `in_universe_v1=true`) cruzado com:

- `MATCH (r:IngestionRun) RETURN DISTINCT r.source_id` no Neo4j local
  (27 source_ids com execuções registradas)
- `ls data/` (24 pastas com dados baixados)

Resultado: **94 fontes catalogadas que nunca rodaram nem baixaram**. Este
documento as agrupa em tiers pra nortear os próximos sprints.

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

## Tier PX — blocker conhecido (débito documentado) ou débito externo preexistente

Fontes que NÃO devem virar sprint antes do blocker sair. Cross-ref com
débitos em `todo-list-prompts/`.

| Source | Blocker | Débito em | Ação |
|---|---|---|---|
| `caged` | `.7z` + form-wall, `load_state=partial` | `todo-list-prompts/very_high_priority/script-download-conversions/medium-tier/caged.md` | Esperar deps/creds; pipeline já implementado em modo agregado |
| `rais` | BigQuery via basedosdados, `load_state=loaded` já | — (já loaded via BigQuery agg) | Revalidar que está atualizado; se não, `medium-tier/rais.md` |
| `pncp` | Agendamento de janela longa | `todo-list-prompts/high_priority/debitos/rodar-pipelines-pesados.md` (discute janelas), + `very_high_priority/.../medium-tier/pncp.md` | Esperar janela; pipeline existe |
| `datajud` | Credenciais CNJ não totalmente operacionais em prod | `load_state=not_loaded` no registry, `blocked_external` | Retomar quando credencial prod sair |
| `tce_go` | CSV export schema indefinido | `todo-list-prompts/high_priority/variados/01-tce_go.md` + `very_high_priority/.../hard-tier/tce_go.md` | Seguir o prompt dedicado |
| `tcmgo_sancoes` | Export CSV pendente no portal TCMGO | `todo-list-prompts/high_priority/variados/03-tcmgo_sancoes.md` | Seguir prompt dedicado |
| `ssp_go` | Export machine-readable pendente | `todo-list-prompts/high_priority/variados/04-ssp_go.md` | Seguir prompt dedicado |
| `alego` | Downloader existe mas IngestionRun nunca rodou | `todo-list-prompts/high_priority/variados/02-alego.md` + `todo-list-prompts/high_priority/variados/06-verba-indenizatoria-estadual-go.md` | Seguir prompts dedicados |
| `camara_goiania` | Plone endpoints devolvem stubs; dados só via HTML+PDF scraping | `todo-list-prompts/high_priority/debitos/camara-goiania-scraping.md` + `variados/13-cota-vereadores-goiania.md` | Seguir débito |
| `inep` | `load_state=loaded` já no registry, `data/inep/` ausente local? | `todo-list-prompts/very_high_priority/script-download-conversions/easy-recovery/inep.md` (conversão para `script_download`) | Seguir débito de conversão |
| `transferegov` | Idem — script_download conversion | `easy-recovery/transferegov.md` | Seguir débito |
| `cpgf` | Idem — conversion | `easy-recovery/cpgf.md` | Seguir débito |
| `holdings` | Conversion para script_download | `medium-tier/holdings.md` | Seguir débito (após P1 rodar a carga inicial) |
| `icij` | Conversion | `medium-tier/icij.md` | Seguir débito (após P1) |
| `bndes` | Conversion | `medium-tier/bndes.md` | Seguir débito (após P1) |
| `mides` | Conversion; BigQuery já funciona | `medium-tier/mides.md` | Já carregado; conversão é débito secundário |
| `siconfi` | Conversion (API ORDS) | `medium-tier/siconfi.md` | Seguir débito; P2 acima é a carga em si |

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

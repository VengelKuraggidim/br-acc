# Roadmap: cross-check de perspectivas TSE (doador vs candidato vs partido)

## Contexto

Sessão 2026-04-19. Conversa com Fernando sobre "o TSE poderia registrar
caixa 2 sem saber?". Conclusão: o ecossistema TSE já tem **múltiplas
perspectivas** da mesma transação, e divergências entre elas são
achados auditáveis. Hoje nosso grafo ingere só a **perspectiva do
candidato** (`receitas_candidatos.csv`) — logo, divergências
TSE-internas ficam invisíveis.

Este débito é **roadmap**, não fix pontual. Lista o que seria necessário
pra surfacear divergências internas TSE honestamente.

## Perspectivas que existem no ecossistema TSE

| Perspectiva | Fonte | Status no grafo |
|---|---|---|
| **Candidato**: "recebi R$ X de Fulano" | `receitas_candidatos_{ano}.csv` | ✅ ingerido (`tse_prestacao_contas_go`) |
| **Partido**: "repassei R$ X pro candidato Y (fundo partidário/FEFC)" | `receitas_partidos_{ano}.csv` + arquivo de transferências | ❌ não ingerido |
| **Doador**: "doei R$ X pro candidato Y" (via SPCE/Pix/cartão ou reporte direto de empresa) | CSVs de doadores publicados pelo TSE (conferir disponibilidade) | ❌ não ingerido |
| **Bancária**: extratos da conta de campanha (Res. TSE 23.607/2019) | Não publicado no portal — obtido só via Lei de Acesso | ❌ impossível sem LAI |
| **Despesas**: "paguei R$ X pra fornecedor Z" | `despesas_pagas_{ano}.csv` + `despesas_contratadas_{ano}.csv` | ✅ ingerido (mesmo pipeline) |

## Casos de divergência TSE-interna detectáveis

1. **Fundo partidário**: partido declara ter repassado R$ 100k pro
   candidato X; candidato X declara ter recebido R$ 50k do partido.
   Divergência visível cruzando `receitas_partidos` × `receitas_candidatos`.

2. **Doador vs candidato**: empresa Y declara ter doado R$ 200k pro
   candidato X; candidato X declara ter recebido R$ 150k de Y.
   Divergência visível cruzando `doadores` × `receitas_candidatos`.

3. **Receitas vs despesas**: candidato X declara R$ 500k em receitas e
   R$ 700k em despesas pagas (sem empréstimo). Divergência visível no
   próprio CSV `despesas_pagas` vs `receitas_candidatos` (já ingerimos
   os dois, não surfaceamos no perfil).

## Plano (multi-sessão)

### Fase 1: caso 3 (mais barato — dados já estão no grafo)

- Adicionar card "Contas batem?" no perfil /politico comparando
  `total_tse_{ano}` (receitas) vs `total_despesas_tse_{ano}`.
- Se despesas > receitas sem empréstimo declarado → surfacear com link
  pra CSV de ambos no portal TSE.
- Tests + mensagem neutra ("verificar fontes").

### Fase 2: caso 1 (fundo partidário)

- Pipeline novo: ingerir `receitas_partidos_{ano}.csv` + arquivo de
  transferências partido→candidato.
- Nodes: `:Party`, relações `:REPASSOU {ano, valor}` para `:Person`.
- Serviço que compara sum de `:REPASSOU` (lado partido) com sum de
  `:DOOU` onde doador é partido (lado candidato).

### Fase 3: caso 2 (doador vs candidato)

- Investigar se TSE publica CSV de doadores com perspectiva própria
  (ou se só há a perspectiva do candidato).
- Se sim: pipeline + cross-check análogo à fase 2.
- Se não: este caso só é detectável via LAI (fora do escopo do projeto
  por ora).

## Guardrails

- **Neutralidade**: qualquer card novo usa linguagem "divergência
  visível entre duas declarações oficiais", com link pras duas fontes.
  Nunca rotular causa (erro de digitação, omissão, etc).
- **Proveniência**: toda rel `:REPASSOU` precisa de `source_id`,
  `source_url`, `ingested_at`, `run_id`, `source_snapshot_uri`
  (archival obrigatório).
- **LGPD**: CPFs continuam mascarados via `mascarar_cpf`.

## Prioridade

**Média-alta** — não bloqueia MVP, mas é o que diferencia o Fiscal
Cidadão de outros portais de transparência. Surfacear divergências
TSE-internas com link pras duas fontes é exatamente o caso de uso de
"proveniência rastreável" que a missão do projeto promete.

## Referências

- `etl/src/bracc_etl/pipelines/tse_prestacao_contas_go.py` (pipeline de
  receitas do candidato — base pra replicar pros outros CSVs)
- `api/src/bracc/services/validacao_tse_service.py` (pattern de
  cross-check que serviria de base pros novos cards)
- `docs/provenance.md`, `docs/archival.md` (contratos obrigatórios)

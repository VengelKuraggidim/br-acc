# Perfil — cards próprios pra Sanções, TCE-GO, Embargos, TCM-impedidos

## Contexto
O `gerar_alertas_completos` consome `Sanction`/`TcmGoImpedido` mas só vira **uma linha de alerta** ("⚠ Tem 3 sanções"). O cidadão não vê **quais** sanções, **qual** órgão sancionador, **quando**, **por quanto tempo**. E `TceGoDecision` (10k decisões), `Embargo` (101k embargos ambientais via empresas conectadas) e `Expulsion`/`BCBPenalty`/`InternationalSanction` não são surfaceados em lugar nenhum.

## Forma esperada na UI
Quatro cards novos no `renderPerfil`, agrupados sob heading "Histórico de irregularidades", abaixo dos alertas:

1. **Sanções administrativas** (`:Sanction` — CGU CEIS/CNEP) — lista com órgão sancionador, tipo (inidôneo, impedido, multa), data, vigência, fonte.
2. **Decisões TCE-GO** (`:TceGoDecision`) — número do processo, ano, ementa curta, link.
3. **TCM-GO impedidos** (`:TcmGoImpedido`) — motivo, ano, link.
4. **Embargos ambientais via empresas conectadas** (`(:Person)-[:SOCIO_DE|DOOU_PARA_COMITE_DE]->(:Company)-[:EMBARGADA]-(:Embargo)`) — quem é a empresa, qual o embargo (IBAMA/SEMAD), data.

Cada card só renderiza se `len > 0`. Severidade visual igual à dos alertas (vermelho/amarelo).

## Onde mexer
- `api/src/bracc/queries/` — 4 queries novas: `perfil_sancoes.cypher`, `perfil_tce_go_decisoes.cypher`, `perfil_tcm_impedido.cypher`, `perfil_embargos_via_empresas.cypher`.
- `api/src/bracc/services/perfil_service.py` — gather paralelo (já tem padrão), agregar 4 listas novas.
- `api/src/bracc/models/perfil.py` — 4 models novos (`SancaoCard`, `TceGoCard`, `TcmImpedidoCard`, `EmbargoConectadoCard`) + 4 campos no `PerfilPolitico`.
- `pwa/index.html` — bloco novo no `renderPerfil` agrupando os 4 cards (com colapsável `<details>` por card).
- `alertas_service.py` — quando o card detalhado existir, **simplificar** o alerta agregado pra "Ver detalhes abaixo" em vez de repetir o conteúdo.

## Não confundir com
- `Embargo` direto em `:Person` é raro; o ataque comum é via empresa do político (sócio ou doadora). Query precisa fazer 2-hop.
- `:InternationalSanction` (39k) é OFAC/UN — só relevante pra empresas multinacionais; deixar pra fase futura, fora deste card.

## Esforço
Médio. 4 queries + 4 models + ~150 linhas no PWA. Sem pipeline novo — todos os dados já estão ingeridos.

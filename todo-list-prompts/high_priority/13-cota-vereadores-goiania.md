# 13 — Cota dos vereadores de Goiânia (Câmara Municipal)

**Origem**: fase follow-up do 06 (verba indenizatória estadual GO).

A fase 06 cobriu deputados estaduais GO (ALEGO) — `PerfilService` roteia
`StateLegislator` GO para `obter_verba_indenizatoria_alego`. Faltam os
**vereadores** de Goiânia: hoje `CityCouncilor` / `GoVereador` não têm
seção "Quanto gasta com a política" mesmo se as conexões do perfil
trouxerem o político.

## Escopo

- **Goiânia apenas** (capital). Outros municípios GO ficam fase futura
  — se o portal da Câmara de Goiânia publicar e se o volume compensar.
- **Schema previsto**:
  `(:GoVereador)-[:GASTOU_COTA_GOIANIA]->(:GoCouncilExpense)`
  (nomes já reservados em `init.cypher`; pipeline a criar).

## Descoberta

- Descobrir endpoint/dataset do portal da Câmara Municipal de Goiânia
  (https://www.goiania.go.leg.br/ e/ou transparência municipal). Os
  pipelines existentes `camara_goiania_*` (se houver) são boa pista.
- Se só tiver PDF de prestação de contas (inescrutável) → PARAR e
  documentar bloqueio (similar ao 09 TSE SPA).
- Se tiver API/CSV → criar pipeline `camara_goiania_vereadores_cota`
  seguindo o padrão `alego.py` (archival + attach_provenance por linha).

## Entregáveis

1. Pipeline ETL com archival completo.
2. Função `obter_cota_vereador_goiania(driver, vereador_id, anos)` em
   `despesas_service.py` — shape idêntico a CEAP/ALEGO
   (`list[DespesaGabinete]`).
3. Roteamento em `perfil_service.obter_perfil`:
   - Se `CityCouncilor` / `GoVereador` com `municipality='Goiania'` →
     chama `obter_cota_vereador_goiania`.
   - Demais casos permanecem como hoje (despesas vazias + aviso de
     "ainda não temos dados dessa casa legislativa").
4. `aviso_despesas` ganha 4º ramo: "Verba / cota da Câmara Municipal
   de Goiânia (CMG)".

## Constraints

- `StateLegislator` uf=GO **continua** usando ALEGO (fase 06 já feita)
  — não regredir roteamento.
- Provenance obrigatória (source_url, source_record_id, etc).
- LGPD: se houver CPF de prestador, aplicar `mask_cpf` antes de gravar.

## Tests esperados

- Pipeline: TestArchivalRetrofit + fixture com 2 vereadores + ≥5 despesas cada.
- Service: driver mockado → list[DespesaGabinete] correto.
- PerfilService: rota vereador Goiânia → função específica é chamada,
  CEAP federal e ALEGO não são chamados.
- `aviso_despesas` dinâmico: 4 casos cobertos.

## Commits esperados (3 atômicos)

1. `feat(etl): pipeline camara_goiania_vereadores_cota (arquival + prov)`
2. `feat(api): obter_cota_vereador_goiania em DespesasService`
3. `feat(api): PerfilService roteia vereador Goiania → cota municipal`

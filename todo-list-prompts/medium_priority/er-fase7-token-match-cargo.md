# ER — Fase 7 automática: token-match StateLeg sem CPF + cargo TSE

> Proposta criada 2026-05-02. Generaliza o batch manual de 2026-05-02
> (`project_er_alego_batch_2026_05_02.md`) pra evitar lookup futuro.

## Contexto

ALEGO (e provavelmente futuras ingestões de ALCs estaduais) não publica
CPF. O resolver atual em
`etl/src/bracc_etl/pipelines/entity_resolution_politicos_go.py` cobre 6
fases (CPF, nome+partido, nome+município, shadow exato, shadow prefix,
manual). Nenhuma cobre o caso "StateLeg sem CPF, nome ALEGO truncado/
fuzzy do nome eleitoral, cargo TSE confirma Dep.Estadual".

Em 2026-05-02 isso foi feito a mão: 16 pareamentos com REPRESENTS
manual + tag `source='er_alego_reconcile_2026_05_02'`.

## Proposta — Fase 7 `name_first_last_cargo_estadual`

Casa shadow `:StateLegislator {uf:'GO', cpf:''}` com `:Person {uf:'GO'}`
quando:

1. Person tem `cargo_tse_<ano>` contendo "DEPUTADO ESTADUAL" (qualquer
   ano TSE ingerido).
2. Token-match: todos os tokens core do StateLeg (após remover
   prefixo profissional `DELEGADA|DR|MAJOR|CORONEL|PASTOR|...` e
   stopwords `DE|DA|DO`) batem fuzzy (edit ≤ 1) com algum token do
   nome do Person.
3. Resultado é único — se múltiplos Persons batem, abort e marca pro
   manual.

Confidence: **0.75** (abaixo de shadow_prefix_match 0.70 só se quiser
ser conservador; senão 0.78 — entre prefix e cpf_suffix).

## Implementação

- Adicionar nova fase em
  `entity_resolution_politicos_go.py` após `shadow_prefix_match`.
- Reusar helpers do batch ad-hoc (em `/tmp/reconcile_alego.py`):
  normalize, tokens, fuzzy edit-distance ≤ 1.
- Excluir StateLeg que já tenham canonical com sibling Person
  (idempotência).
- Marcar com `r.method = 'token_fuzzy_cargo_estadual'`.

## Pendência colateral

3 StateLeg do batch (BIA DE LIMA, DELEGADA FERNANDA, MAJOR ARAUJO) não
casaram porque:
- BIA → não tem candidato (Person 'BEATRIZ' ou similar pode estar
  ingerido sem `cargo_tse_*` = Dep.Estadual; relaxar filtro pra qualquer
  cargo TSE 2022 GO ajudaria).
- FERNANDA/ARAUJO → ambíguos por sobrenome comum.

A Fase 7 deve abort em ambíguo (não inventar match), e o tooling expor
um relatório com o subset pendente pra resolver via `manual_override.csv`.

## Validação

- Rodar Fase 7 contra o snapshot atual e verificar que recria os 16
  pareamentos do batch ad-hoc.
- Conferir que nenhuma `:Person` ganha 2 canonicals.

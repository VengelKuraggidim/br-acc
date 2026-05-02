# Emendas estaduais GO — pipeline LOA

> Aberto em 2026-05-02. Detectado quando o perfil de
> deputado estadual mostrava 0 emendas mesmo após batch ER ALEGO
> (memo `project_er_alego_batch_2026_05_02.md`).

## Estado atual

`etl/.../emendas_parlamentares_go.py` e `transparencia.py` ingerem
**emendas federais** com filtro `uf='GO'` (memo
`project_emendas_transparencia_sem_filtro_uf.md`). Cobrem deputado
federal e senador da bancada GO.

**Não há pipeline de emendas estaduais (LOA Goiás)**. Resultado:
`/politico/<state_legislator>` retorna `emendas: []` por design,
não por bug.

## Fontes candidatas

1. **Portal da Transparência GO** — emendas individuais à LOA estadual,
   se publicado. Verificar
   <https://transparencia.goias.gov.br/>.
2. **ALEGO Transparência** — apresenta projetos de lei e emendas
   apresentadas; pode não ter execução orçamentária.
3. **SIOFI/SICONV** federal não cobre LOA estadual.
4. **TCE-GO** — pode ter relatórios de execução com emendas.

## Definir antes de scoping

- Granularidade: emenda por deputado estadual + valor empenhado/pago +
  destinatário (município/órgão)?
- Refresh: anual (LOA é orçamento anual) ou mensal?
- Fallback: se a LOA não publica emenda individual, vale só ingerir
  totais agregados ou abandonar?

## Notas

Não bloqueia nada hoje — perfil mostra `emendas=0` e o aviso fala
sobre verba indenizatória, não emendas. Se priorizar, scoping novo
no padrão dos pipelines existentes (`etl/src/bracc_etl/pipelines/`).

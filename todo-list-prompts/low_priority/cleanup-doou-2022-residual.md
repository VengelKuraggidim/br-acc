# Cleanup residual de duplicados :DOOU 2022 (cross-product duvidoso)

> Em 2026-05-02 o cleanup confirmado de 147 pares 1:1 foi rodado
> (DELETE em rels :Person/:Company → :Person legacy sem `donation_id`
> que tinham contraparte 1:1 em :CampaignDonor → :Person novo, mesmo
> target+valor). 23 pares restantes são **cross-product duvidoso** —
> não foram deletados porque o critério não dá confiança alta.

## Estado em 2026-05-02 (pós-cleanup dos 147)

| Métrica | Valor |
|---|---|
| :DOOU 2022 total | 543.656 |
| sem `donation_id` (legacy `tse.py`) | 531.641 |
| com `donation_id` (`tse_prestacao_contas_go`) | 12.015 |
| pares 1:1 já deletados (cleanup 2026-05-02) | 147 |
| **pares cross-product remanescentes** | ~23 |

## O que sobra e por que é duvidoso

Cenários onde **mesmo target + mesmo valor** matcha em ambos pipelines
mas com >1 rel de pelo menos um lado:

| target | valor | n_legacy | n_new | risco |
|---|---|---|---|---|
| EDWARD MADUREIRA BRASIL | R$ 1.000 | 1 | 27 | falso positivo: 27 doadores distintos doaram R$ 1k pra mesma pessoa |
| ISSY QUINAN JUNIOR | R$ 600 | 1 | 9 | mesma coisa |
| MARIA EUZEBIA DE LIMA | R$ 500 | 1 | 7 | mesma coisa |
| ... | ... | ... | ... | |

Como o pipeline novo usa `doador_id` mascarado (`***.***.*71-60`) e o
legacy usa CPF pleno, **não dá pra parear pessoa-a-pessoa**. O critério
só de target+valor é frágil pra valores pequenos/comuns (R$ 50, R$ 100,
R$ 500, R$ 1.000) — pode ser doação real.

## Critérios mais seguros (em ordem de confiança)

1. **Adicionar `data` ao TSE-novo loader** — hoje `r.data` é NULL nos
   dois lados (campo `dt_recebimento` pulado em ambos). Se pelo menos
   o pipeline novo gravar a data, pareamento `target+valor+data` fica
   distintivo. Ver `tse_prestacao_contas_go.py` linha que constrói o
   dict de DOOU.
2. **Backfill de doador_id no legacy** — re-rodar `tse.py` 2022 com
   o mesmo masker do `tse_prestacao_contas_go` (gera `donor_id`
   determinístico via hash do CPF). Aí pareamento por `donor_id` vira
   exato. Mais trabalho, mais conservador.
3. **Mover por mão** — só atacar perfis de políticos GO 2022 onde a
   discrepância for visível na UI (ex: Amilton já está OK pós-fix de
   service, então não há urgência).

## Por que está em low_priority

- O fix de service em `conexoes_service.py` já dedupa em runtime
  (commit `4e7bf0a`), então a UI mostra o número certo
- ~23 rels é insignificante perante os 543k totais — impacto em
  agregações é < 0.005%
- Risco de falso positivo é alto demais pra rodar DELETE cego

Reabre pra medium se aparecer caso onde a UI mostra valor inflado
(provavelmente em queries fora do conexões_service que não passem
pelo dedup runtime).

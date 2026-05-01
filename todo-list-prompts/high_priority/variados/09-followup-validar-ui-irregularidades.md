# Validar UI do card "Histórico de irregularidades" no PWA

> Follow-up do TODO 09 (resolvido em 2026-05-01, ver
> `medium_priority/more_data/07-priorizacao-tier.md`). Backend + PWA
> code estão prontos, mas a validação visual no browser não foi feita
> nesta sessão (sem browser).

## O que validar

Abrir o PWA no browser (`http://localhost`) e checar 4 perfis com
diferentes irregularidades:

| Caso              | entity_id (eid)                                            | Esperado                                                                |
|-------------------|------------------------------------------------------------|-------------------------------------------------------------------------|
| Sanção CEIS       | `4:da0ec56f-cb5d-454a-b730-78a989eacdb6:7931` (Mauro)      | Card "Histórico de irregularidades" com 1 sanção CEIS, motivo longo (Lei 8429) |
| Embargo IBAMA     | `4:da0ec56f-cb5d-454a-b730-78a989eacdb6:7897` (Alcides)    | 2 embargos Fazenda Santo Antônio em Araguaiana/MT                       |
| Conta TCE-GO      | `4:da0ec56f-cb5d-454a-b730-78a989eacdb6:4342262` (Adelina) | 2 contas julgadas irregulares (proc. 27761401 + outro), link pro PDF do TCE |
| Impedimento TCM   | `4:da0ec56f-cb5d-454a-b730-78a989eacdb6:413920` (Adailton) | 1 impedimento (Balancete Semestral, processo 2414/23-2)                 |

## O que olhar especificamente

1. **Bloco renderiza** — `<div class="section">` com border-left vermelho e
   header "⚠️ Histórico de irregularidades", badge mostrando total.
2. **Sub-cards independentes** — cada um dos 4 tipos só aparece se tiver
   dado (`<details>` colapsável). Quem tem só sanção, só vê sanção.
3. **Truncar texto longo** — motivo do CEIS tem ~1.5k char (Lei 8429
   citada na íntegra); helper `trunc(s, 280)` deve cortar em 280 e
   colocar "…".
4. **Formato de data** — `fmtData()` converte ISO `2024-05-06` →
   `06/05/2024`. Embargo às vezes vem com `data=""` — não renderiza
   linha de data nesse caso.
5. **Link do TCE-GO PDF** — clicar no "Ver decisão" deve abrir o PDF
   no portal `paineis.tce.go.gov.br/single/...`.
6. **Disclaimer do TCM** — `summary` tem `(match por nome — verificar
   fonte)` em itálico/cinza, alertando do match heurístico.

## Possíveis ajustes pós-validação

- Espaçamento/alinhamento dos cards aninhados.
- Cor do badge (`badge-danger` é vermelho intenso — pode ficar
  agressivo demais se um perfil tem 50+ embargos).
- Texto introdutório do bloco — está conservador hoje, pode pedir
  ajuste de tom (ex.: deixar claro que "ligação ao cluster canônico"
  inclui o nó canonical e siblings TSE).

## Onde mexer se for ajustar

- Card: `pwa/index.html:2795-2918` (bloco que começa em
  `// Histórico de irregularidades`).
- Truncamento: helper `trunc()` na linha ~2825.
- Formato de data: helper `fmtData()` na linha ~2818.

## Esforço

Pequeno (15-30min). Validar visualmente, aplicar 1-2 ajustes de CSS/copy
se necessário, commitar.

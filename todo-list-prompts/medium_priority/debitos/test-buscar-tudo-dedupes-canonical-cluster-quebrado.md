# Test `test_buscar_tudo_dedupes_by_canonical_cluster` quebrado

> Descoberto 2026-05-02 enquanto rodava `pytest tests/unit/test_pwa_parity.py`
> antes do commit `88c4343` (reranking fuzzy). O teste falha **desde o
> HEAD anterior** ao meu commit — confirmado via `git stash && pytest`.
> Não é regressão da mudança de reranking.

## Sintoma

```
FAILED tests/unit/test_pwa_parity.py::test_buscar_tudo_dedupes_by_canonical_cluster[asyncio]

>       assert "fed:flavia" in ids
E       AssertionError: assert 'fed:flavia' in ['person:tse', 'person:solo']
```

## Cenário do teste

3 nós no mesmo `canonical_id="canon_camara_160598"`:

| ID | Label | Score Lucene | document_id | Esperado |
|---|---|---|---|---|
| `person:camara` | Person | 12.0 | `4:x:1` | dropado (cluster mate) |
| `fed:flavia` | FederalLegislator | 11.5 | `4:x:2` | **representante** (maior oficialidade) |
| `person:tse` | Person | 10.0 | `12345678900` (CPF) | dropado (cluster mate) |

Esperado: o `_merge_group` do cluster canônico elege `fed:flavia` por
ter maior `_cluster_rank` (FederalLegislator > Person). Pessoa solta
`person:solo` passa intacta. Final: 2 resultados.

## Realidade

Final: 2 resultados, mas é `[person:tse, person:solo]` — `fed:flavia` foi
dropado e `person:tse` ficou no lugar.

## Hipóteses

- `_rank` (em `pwa_parity.py:570-586`) ordena por `(-_latest_cargo_year,
  has_cpf, _cluster_rank, -score, id)`. Como nenhum dos 3 tem
  `latest_cargo_year`, o desempate cai em `has_cpf` — e `person:tse` é
  o único com CPF (`document_id="12345678900"`). `has_cpf=0` ganha sobre
  `has_cpf=1` (False ordena antes). Aí `person:tse` vira `rows[0]` em
  vez de `fed:flavia`.
- Bug aparente: a regra "maior oficialidade ganha o cluster" não está
  refletida na ordem do `_rank` — `_cluster_rank` aparece **depois**
  de `has_cpf`. Se a regra de negócio é "FederalLegislator > Person
  com CPF", `_cluster_rank` deveria vir antes.

## O que fazer

1. Confirmar a regra de negócio com a usuária (qual é o tiebreaker
   correto entre "tem CPF" e "é parlamentar formal"?).
2. Se a regra é "oficialidade ganha", ajustar `_rank` em
   `api/src/bracc/routers/pwa_parity.py:574` pra colocar
   `_cluster_rank` antes de `has_cpf`. Validar que o teste passa
   sem regredir os outros 8.
3. Investigar produção: rodar `Flavia Morais` no PWA e ver se
   aparece o nó certo. Se sim, o teste é que está errado (atualizar
   asserts). Se não, o bug é real.

## Não-bloqueante

PWA aparentemente não está apresentando o sintoma reportado pela
usuária — ela não levantou caso de "cluster com perfil errado
ganhando". Tratar como débito técnico, não bug crítico.

# Perfil — seção "Histórico eleitoral"

## Contexto
O grafo tem 17.637 `:Election` e 517.656 relações `(:Person)-[:CANDIDATO_EM]->(:Election)`. Hoje o `PerfilPolitico` ignora 100% disso — o cidadão não vê quantas vezes a pessoa disputou eleição, em qual cargo, ou se ganhou/perdeu. É um dos contextos mais leigos-friendly que dá pra mostrar.

## Forma esperada na UI
Card "Histórico eleitoral" no `pwa/index.html::renderPerfil`, abaixo do header. Lista cronológica reversa:

```
2024 — Vereador (Goiânia/GO) — PT — Eleito ✓
2020 — Vereador (Goiânia/GO) — PT — Não eleito
2018 — Deputado Estadual (GO) — PT — Não eleito
```

Quando há ≥3 candidaturas, mostra também: "Disputou X eleições nos últimos Y anos. Eleito em N delas."

## Onde mexer
- `api/src/bracc/queries/` — nova query `perfil_historico_eleitoral.cypher` (input: `entity_id`, output: lista `{ano, cargo, uf, municipio, partido, situacao}` ordenada).
- `api/src/bracc/services/perfil_service.py` — chamada paralela com `obter_ceap_deputado` etc., agregar no `PerfilPolitico`.
- `api/src/bracc/models/perfil.py` — adicionar `class CandidaturaHistorica` + campo `historico_eleitoral: list[CandidaturaHistorica] = []`.
- `pwa/index.html::renderPerfil` — card novo após o header (antes de "Score de red flags").

## Campos disponíveis no `:Election`
`year`, `cargo`, `uf`, `municipio`, `election_id`. Status (eleito/não-eleito) precisa vir do rel `CANDIDATO_EM` ou de prop no nó — confirmar com `MATCH (p)-[r:CANDIDATO_EM]->(e:Election) RETURN keys(r), keys(e) LIMIT 1`. Se não tiver, fica só "candidatou-se".

## Esforço
Pequeno. ~1 query + ~1 model + ~30 linhas no PWA. Não precisa de pipeline novo.

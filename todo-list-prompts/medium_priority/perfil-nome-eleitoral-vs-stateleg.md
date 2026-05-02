# Perfil — nome eleitoral vs nome do StateLeg

> Aberto em 2026-05-02 após batch ER ALEGO. Cosmético, não bloqueia
> dado nenhum.

## Sintoma

Buscar `Henrique César Pereira` → resultado mostra
`HENRIQUE CEZAR PEREIRA — Deputado Estadual`. Ao clicar, o cabeçalho do
perfil exibe `HENRIQUE CESAR` (nome curto do `:StateLegislator` ALEGO),
não o nome eleitoral completo.

Mesma coisa pode acontecer com qualquer um dos 16 batch-pareados
(LEONNARDO PORTILHO → "LEO PORTILHO", LINCOLN GRAZIANI... → "LINCOLN
TEJOTA", etc.).

## Causa

Pós-batch ER, o cluster CanonicalPerson tem 2 siblings: `:Person` (TSE,
nome completo) + `:StateLegislator` (ALEGO, nome curto/apelido). A query
`api/src/bracc/queries/perfil_politico_connections.cypher` escolhe o
focal pelo `_CLUSTER_RANK` (StateLegislator=2 < Person=4) — preferência
correta pra disparar `is_estadual_go` e ALEGO. Mas o `nome` do header do
perfil sai do focal eleito → fica curto.

## Fix proposto

Em `perfil_service.py::_build_politico_resumo` (ou na query
`perfil_politico_connections.cypher`): preferir o `name` mais longo
entre os siblings do cluster, mantendo todos os outros campos do focal
(labels, legislator_id, foto). Heurística: maior `size(split(name, ' '))`,
empate vai pro focal.

Alternativa: campo `nome_oficial` separado no resumo, e o frontend
mostra "HENRIQUE CEZAR PEREIRA (HENRIQUE CESAR)" — preserva ambos sem
escolher.

## Validação

Pós-fix, abrir `/politico/...:7952` (Henrique Cezar) deve retornar
`politico.nome = "HENRIQUE CEZAR PEREIRA"` (ou variante com ambos),
mantendo `cargo`, despesas e demais campos intactos.

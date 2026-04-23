// CEAPS de um senador federal (leitura do grafo — nao live-call).
//
// Escopo: label :Senator (pipeline senado_senadores_foto cria o no;
// pipeline senado cria as :Expense linkadas via (:Person)-[:GASTOU]).
//
// Como o pipeline senado.py liga Expense a :Person (match por CPF ou nome),
// e o :Senator tem o mesmo ``name`` do CSV do portal do Senado (normalizado
// em ambos os lados via transforms.normalize_name), a bridge acontece em:
//
//     :Senator {id_senado}
//       └── match por ``s.name = p.name`` ───→ :Person
//               └── [:GASTOU] ──→ :Expense {source:"senado"}
//
// Alternativa via cluster canonico (:CanonicalPerson): nao assumida aqui
// porque nem todo senador tem cluster canonico gerado, mas o name match
// e robusto porque o mesmo pipeline grava ambos os lados (Senator.name e
// Person.name passaram pelo mesmo normalize_name).
//
// Props do no de despesa (shape atual do senado pipeline):
//   - type         (ex: "Passagens aereas, aquaticas e terrestres ...")
//   - value        (BRL float)
//   - date         (ISO string "YYYY-MM-DD")
//   - description  (detalhamento do CSV)
//   - source       ("senado")
//
// Filtro por ano: o pipeline nao grava ``ano``/``mes`` separados, so ``date``.
// Extraimos o ano com substring pra o contrato com _aggregate_despesas.
MATCH (s:Senator {id_senado: $id_senado})
MATCH (p:Person {name: s.name})
MATCH (p)-[:GASTOU]->(e:Expense)
WHERE e.source = "senado"
  AND e.date IS NOT NULL
  AND toInteger(substring(e.date, 0, 4)) IN $anos
// `WITH DISTINCT e` defende contra múltiplos :Person homônimos (mesmo
// name, CPFs diferentes) que causariam soma inflada no Python. O pipeline
// também pega 1 Person por nome, mas mantemos o DISTINCT como 2ª camada
// pra que runs antigos ou outros bridges (cluster canônico) não quebrem.
WITH DISTINCT e
RETURN e.type AS tipo_raw,
       e.value AS valor,
       toInteger(substring(e.date, 0, 4)) AS ano

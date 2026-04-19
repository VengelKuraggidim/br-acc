"""Build a reduced demo copy of the Fiscal Cidadão Neo4j graph.

Lê do grafo fonte (default `bolt://localhost:7687`) e escreve num grafo
destino (default `bolt://localhost:7688`, container `fiscal-neo4j-demo`).
Nunca modifica o grafo fonte.

Estratégia: janela temporal deslizante.
  - Seed atemporal: políticos GO (Senator/FederalLegislator/StateLegislator
    + CanonicalPerson{uf='GO'}) + municípios GO.
  - Eventos datados (Amendment, LegislativeExpense, Contract, Sanction,
    TceGoDecision, GoProcurement, GoGazetteAct, Election ...) entram se
    `ano/date/published_at >= cutoff`.
  - Entidades atemporais ligadas (Company, Partner) entram só se tocadas
    por evento admitido.
  - Cutoff começa em `--start-year` (default 2025) e recua ano-a-ano até
    `--min-year` (default 2018) ou até bater 80% dos limites Aura Free
    (160k nodes / 320k rels).

Uso:
  # Dry-run apenas (não mexe no destino):
  python scripts/build_demo_graph.py --dry-run-only

  # Build completo (wipe + copy):
  python scripts/build_demo_graph.py --wipe-target

Requer o driver `neo4j` (já presente em `etl/pyproject.toml`). Rodar de
dentro do venv do ETL:
  cd etl && uv run python ../scripts/build_demo_graph.py ...
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field

from neo4j import Driver, GraphDatabase, Session

logger = logging.getLogger("build_demo_graph")

# ----------------------------------------------------------------------
# Orçamento + defaults Aura Free
# ----------------------------------------------------------------------
AURA_FREE_NODE_LIMIT = 200_000
AURA_FREE_REL_LIMIT = 400_000
DEFAULT_NODE_BUDGET = int(AURA_FREE_NODE_LIMIT * 0.80)  # 160_000
DEFAULT_REL_BUDGET = int(AURA_FREE_REL_LIMIT * 0.80)  # 320_000

BATCH = 2_000  # nodes/rels por transação de cópia


# ----------------------------------------------------------------------
# Cypher queries de seleção
# ----------------------------------------------------------------------
# Seed = políticos GO (qualquer label que carregue uf='GO' em cargo público)
# + CanonicalPerson GO (camada de entity resolution).
SEED_QUERY = """
MATCH (p)
WHERE (p:FederalLegislator OR p:StateLegislator OR p:Senator
       OR p:CanonicalPerson)
  AND p.uf = 'GO'
RETURN collect(DISTINCT elementId(p)) AS ids
"""

# REPRESENTS liga CanonicalPerson aos nodes-fonte (Person/Senator/...).
# Incluir o outro lado do REPRESENTS no seed pra garantir identidade.
REPRESENTS_EXPAND = """
MATCH (p)-[:REPRESENTS]-(other)
WHERE elementId(p) IN $ids
RETURN collect(DISTINCT elementId(other)) AS ids
"""

# GoMunicipality — fixo, pouco volume (~246)
GO_MUNICIPALITY_QUERY = """
MATCH (m:GoMunicipality)
RETURN collect(elementId(m)) AS ids
"""

# --- Eventos datados (filtrados por cutoff) ---

# Amendment proposta por político GO, ano >= cutoff
AMENDMENT_QUERY = """
MATCH (p)-[:PROPOS]->(a:Amendment)
WHERE elementId(p) IN $seed_ids
  AND coalesce(toInteger(a.ano), 0) >= $year
RETURN collect(DISTINCT elementId(a)) AS ids
"""

# LegislativeExpense (CEAP + ALEGO) do político GO, ano >= cutoff
LEGISLATIVE_EXPENSE_QUERY = """
MATCH (p)-[r]->(e:LegislativeExpense)
WHERE elementId(p) IN $seed_ids
  AND type(r) IN ['INCURRED', 'GASTOU_COTA_GO']
  AND coalesce(toInteger(e.ano), 0) >= $year
RETURN collect(DISTINCT elementId(e)) AS ids
"""

# Election datado por ano
ELECTION_QUERY = """
MATCH (e:Election)
WHERE coalesce(toInteger(e.year), 0) >= $year
RETURN collect(DISTINCT elementId(e)) AS ids
"""

# Finance de doações ao/pelo político GO.
# DOOU carrega `year` na relação (confirmado em tse.py).
FINANCE_DOOU_QUERY = """
MATCH (src)-[r:DOOU]->(f:Finance)
WHERE (elementId(src) IN $seed_ids OR elementId(f) IN $seed_ids)
  AND coalesce(toInteger(r.year), 0) >= $year
RETURN collect(DISTINCT elementId(f)) AS ids
"""

# Sanction — date_start >= cutoff_iso
SANCTION_QUERY = """
MATCH (s:Sanction)
WHERE coalesce(s.date_start, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(s)) AS ids
"""

# Embargo — date >= cutoff_iso
EMBARGO_QUERY = """
MATCH (e:Embargo)
WHERE coalesce(e.date, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(e)) AS ids
"""

# TCE-GO decisões >= cutoff
TCE_GO_QUERY = """
MATCH (d:TceGoDecision)
WHERE coalesce(d.published_at, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(d)) AS ids
"""

# TCE-GO contas irregulares (todas — volume baixo, sem data confiável)
TCE_GO_IRREG_QUERY = """
MATCH (i:TceGoIrregularAccount)
RETURN collect(DISTINCT elementId(i)) AS ids
"""

# TCM-GO impedidos (data_inicio disponível em impedidos)
TCM_GO_IMPEDIDO_QUERY = """
MATCH (t:TcmGoImpedido)
WHERE coalesce(t.data_inicio, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(t)) AS ids
"""

# TCM-GO contas rejeitadas (todas — sem data confiável no schema)
TCM_GO_REJECTED_QUERY = """
MATCH (t:TcmGoRejectedAccount)
RETURN collect(DISTINCT elementId(t)) AS ids
"""

# Atos do diário oficial GO
GAZETTE_QUERY = """
MATCH (g:GoGazetteAct)
WHERE coalesce(g.published_at, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(g)) AS ids
"""

# Licitações municipais GO
GO_PROCUREMENT_QUERY = """
MATCH (g:GoProcurement)
WHERE coalesce(g.published_at, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(g)) AS ids
"""

# Contratos federais — só os que a empresa aparece como beneficiada por
# emenda já admitida. Filtrar por cutoff.
CONTRACT_FROM_COMPANIES_QUERY = """
MATCH (c:Company)-[:VENCEU]->(ct:Contract)
WHERE elementId(c) IN $company_ids
  AND coalesce(ct.date, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(ct)) AS ids
"""

# --- Entidades ligadas (2-hop), atemporais ---

# Empresas beneficiadas por Amendments já admitidas
COMPANY_FROM_AMENDMENT_QUERY = """
MATCH (a:Amendment)-[:BENEFICIOU]->(c:Company)
WHERE elementId(a) IN $amendment_ids
RETURN collect(DISTINCT elementId(c)) AS ids
"""

# Sócios (Partner) das empresas admitidas. Snapshot_date na rel — usar
# o mais recente como proxy. Pra economia, pegar TODOS os sócios das
# empresas-alvo (não filtra por data).
PARTNER_QUERY = """
MATCH (c:Company)-[:SOCIO_DE_SNAPSHOT]-(p:Partner)
WHERE elementId(c) IN $company_ids
RETURN collect(DISTINCT elementId(p)) AS ids
"""

# Person sócio das empresas admitidas
PERSON_PARTNER_QUERY = """
MATCH (c:Company)-[:SOCIO_DE_SNAPSHOT]-(p:Person)
WHERE elementId(c) IN $company_ids
RETURN collect(DISTINCT elementId(p)) AS ids
"""

# --- Contagem de relações entre nodes admitidos ---
REL_COUNT_QUERY = """
MATCH (a)-[r]-(b)
WHERE elementId(a) IN $ids AND elementId(b) IN $ids
  AND elementId(a) < elementId(b)
RETURN count(r) AS n
"""


# ----------------------------------------------------------------------
# Estruturas
# ----------------------------------------------------------------------
@dataclass
class TierResult:
    """Contagem acumulada de IDs por tier num dado cutoff."""

    cutoff_year: int
    node_ids: set[str] = field(default_factory=set)
    node_count: int = 0
    rel_count: int = 0
    tier_breakdown: dict[str, int] = field(default_factory=dict)

    @property
    def fits(self) -> bool:
        return (
            self.node_count <= ARGS.node_budget
            and self.rel_count <= ARGS.rel_budget
        )


# preenchido pelo main — usado em dataclasses sem acoplamento direto
class _Args:
    node_budget: int = DEFAULT_NODE_BUDGET
    rel_budget: int = DEFAULT_REL_BUDGET


ARGS = _Args()


# ----------------------------------------------------------------------
# Helpers de query
# ----------------------------------------------------------------------
def run_ids(session: Session, query: str, **params) -> set[str]:
    """Executa query que retorna `ids` (lista) e devolve como set."""
    result = session.run(query, **params).single()
    if result is None:
        return set()
    ids = result.get("ids") or []
    return set(ids)


def count_rels_between(session: Session, ids: set[str]) -> int:
    if not ids:
        return 0
    result = session.run(REL_COUNT_QUERY, ids=list(ids)).single()
    return int(result["n"]) if result else 0


def build_for_cutoff(session: Session, cutoff_year: int) -> TierResult:
    """Monta conjunto de IDs incluídos se janela começar em `cutoff_year`."""
    cutoff_iso = f"{cutoff_year}-01-01"
    result = TierResult(cutoff_year=cutoff_year)
    accumulated: set[str] = set()

    # --- Seed (invariante) ---
    seed = run_ids(session, SEED_QUERY)
    represents = run_ids(session, REPRESENTS_EXPAND, ids=list(seed))
    municipalities = run_ids(session, GO_MUNICIPALITY_QUERY)

    tier1 = seed | represents | municipalities
    accumulated |= tier1
    result.tier_breakdown["seed+represents+municipios"] = len(tier1)

    # --- Eventos diretos do seed (datados) ---
    amendments = run_ids(
        session, AMENDMENT_QUERY, seed_ids=list(seed), year=cutoff_year
    )
    expenses = run_ids(
        session,
        LEGISLATIVE_EXPENSE_QUERY,
        seed_ids=list(seed),
        year=cutoff_year,
    )
    elections = run_ids(session, ELECTION_QUERY, year=cutoff_year)
    donations = run_ids(
        session, FINANCE_DOOU_QUERY, seed_ids=list(seed), year=cutoff_year
    )

    tier2 = amendments | expenses | elections | donations
    accumulated |= tier2
    result.tier_breakdown["eventos_diretos"] = len(tier2)

    # --- 2-hop: empresas beneficiadas por emendas admitidas ---
    companies = run_ids(
        session, COMPANY_FROM_AMENDMENT_QUERY, amendment_ids=list(amendments)
    )
    accumulated |= companies
    result.tier_breakdown["empresas_beneficiadas"] = len(companies)

    # --- 2-hop: contratos dessas empresas no período ---
    contracts = run_ids(
        session,
        CONTRACT_FROM_COMPANIES_QUERY,
        company_ids=list(companies),
        cutoff_iso=cutoff_iso,
    )
    accumulated |= contracts
    result.tier_breakdown["contratos"] = len(contracts)

    # --- 2-hop: sócios das empresas admitidas ---
    partners = run_ids(session, PARTNER_QUERY, company_ids=list(companies))
    person_partners = run_ids(
        session, PERSON_PARTNER_QUERY, company_ids=list(companies)
    )
    tier_partners = partners | person_partners
    accumulated |= tier_partners
    result.tier_breakdown["socios"] = len(tier_partners)

    # --- Sanções, embargos (por período) ---
    sanctions = run_ids(session, SANCTION_QUERY, cutoff_iso=cutoff_iso)
    embargoes = run_ids(session, EMBARGO_QUERY, cutoff_iso=cutoff_iso)
    tier_sancoes = sanctions | embargoes
    accumulated |= tier_sancoes
    result.tier_breakdown["sancoes_embargos"] = len(tier_sancoes)

    # --- Contexto fiscal GO ---
    tce_decisions = run_ids(session, TCE_GO_QUERY, cutoff_iso=cutoff_iso)
    tce_irreg = run_ids(session, TCE_GO_IRREG_QUERY)
    tcm_impedidos = run_ids(
        session, TCM_GO_IMPEDIDO_QUERY, cutoff_iso=cutoff_iso
    )
    tcm_rejected = run_ids(session, TCM_GO_REJECTED_QUERY)
    gazette = run_ids(session, GAZETTE_QUERY, cutoff_iso=cutoff_iso)
    go_procurement = run_ids(
        session, GO_PROCUREMENT_QUERY, cutoff_iso=cutoff_iso
    )

    tier_ctx = (
        tce_decisions
        | tce_irreg
        | tcm_impedidos
        | tcm_rejected
        | gazette
        | go_procurement
    )
    accumulated |= tier_ctx
    result.tier_breakdown["contexto_fiscal_go"] = len(tier_ctx)

    # --- Consolidação final ---
    result.node_ids = accumulated
    result.node_count = len(accumulated)
    result.rel_count = count_rels_between(session, accumulated)
    return result


# ----------------------------------------------------------------------
# Discovery + dry-run
# ----------------------------------------------------------------------
def discover(session: Session) -> None:
    """Imprime contagens de sanidade antes de iniciar o loop."""
    logger.info("=== Descoberta inicial (fonte) ===")
    totals = session.run(
        "MATCH (n) RETURN count(n) AS n"
    ).single()
    rels = session.run(
        "MATCH ()-[r]->() RETURN count(r) AS n"
    ).single()
    logger.info(
        "Total: %s nodes / %s rels",
        f"{totals['n']:,}",
        f"{rels['n']:,}",
    )

    seed_count = len(run_ids(session, SEED_QUERY))
    logger.info("Seed políticos GO: %s", seed_count)
    if seed_count == 0:
        logger.error(
            "Seed vazio. O grafo fonte não tem CanonicalPerson/Senator/"
            "FederalLegislator/StateLegislator com uf='GO'. Abortando."
        )
        sys.exit(2)


def find_best_cutoff(
    session: Session, start_year: int, min_year: int
) -> TierResult | None:
    """Recua ano-a-ano. Retorna último cutoff que coube no orçamento."""
    logger.info("=== Dry-run por cutoff ===")
    logger.info(
        "%-8s %-12s %-12s %-10s %-10s %s",
        "cutoff",
        "nodes",
        "rels",
        "nodes %",
        "rels %",
        "status",
    )

    best: TierResult | None = None
    for year in range(start_year, min_year - 1, -1):
        r = build_for_cutoff(session, year)
        node_pct = 100 * r.node_count / ARGS.node_budget
        rel_pct = 100 * r.rel_count / ARGS.rel_budget
        status = "fits" if r.fits else "EXCEEDS"
        logger.info(
            "%-8d %-12s %-12s %-10s %-10s %s",
            year,
            f"{r.node_count:,}",
            f"{r.rel_count:,}",
            f"{node_pct:5.1f}%",
            f"{rel_pct:5.1f}%",
            status,
        )
        if r.fits:
            best = r
        else:
            break  # monotônico: se 2022 estourou, 2021 também estoura
    return best


# ----------------------------------------------------------------------
# Cópia subgrafo fonte → destino
# ----------------------------------------------------------------------
def wipe_target(session: Session) -> None:
    logger.info("Wipe target (DETACH DELETE em batches)…")
    while True:
        summary = session.run(
            "MATCH (n) WITH n LIMIT 5000 DETACH DELETE n RETURN count(n) AS n"
        ).single()
        deleted = summary["n"] if summary else 0
        if deleted == 0:
            break
        logger.info("  apagados %s", deleted)


def fetch_nodes(
    session: Session, ids: list[str]
) -> list[dict]:
    """Lê nodes do source em batches."""
    rows: list[dict] = []
    for i in range(0, len(ids), BATCH):
        chunk = ids[i : i + BATCH]
        result = session.run(
            """
            UNWIND $ids AS id
            MATCH (n) WHERE elementId(n) = id
            RETURN elementId(n) AS id, labels(n) AS labels,
                   properties(n) AS props
            """,
            ids=chunk,
        )
        rows.extend(dict(r) for r in result)
    return rows


def write_nodes(session: Session, nodes: list[dict]) -> None:
    """Cria nodes no target agrupados por combo de labels.

    Cada node recebe:
      - label auxiliar `_Imported` (permite index único sobre _src_id)
      - prop `_src_id` (elementId do source)
    Ambos removidos no final.
    """
    by_labels: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    for n in nodes:
        key = tuple(sorted(n["labels"]))
        by_labels[key].append({"id": n["id"], "props": n["props"]})

    for labels, items in by_labels.items():
        # Adiciona `_Imported` como label aux
        all_labels = ("_Imported",) + labels
        label_expr = ":".join(f"`{l}`" for l in all_labels)
        cypher = (
            f"UNWIND $items AS it "
            f"CREATE (x:{label_expr}) "
            f"SET x = it.props, x._src_id = it.id"
        )
        for i in range(0, len(items), BATCH):
            session.run(cypher, items=items[i : i + BATCH])


def fetch_rels(session: Session, ids: list[str]) -> list[dict]:
    """Lê rels onde ambos endpoints estão no conjunto admitido."""
    rows: list[dict] = []
    # Particionar por endpoint-source pra evitar explosão em single query.
    for i in range(0, len(ids), BATCH):
        chunk = ids[i : i + BATCH]
        result = session.run(
            """
            MATCH (a)-[r]->(b)
            WHERE elementId(a) IN $chunk AND elementId(b) IN $all_ids
            RETURN elementId(a) AS src_id, elementId(b) AS tgt_id,
                   type(r) AS type, properties(r) AS props
            """,
            chunk=chunk,
            all_ids=ids,
        )
        rows.extend(dict(r) for r in result)
    return rows


def write_rels(session: Session, rels: list[dict]) -> None:
    """Cria rels no target agrupadas por tipo."""
    by_type: dict[str, list[dict]] = defaultdict(list)
    for r in rels:
        by_type[r["type"]].append(r)

    for rtype, items in by_type.items():
        cypher = (
            f"UNWIND $items AS it "
            f"MATCH (a {{_src_id: it.src_id}}), (b {{_src_id: it.tgt_id}}) "
            f"CREATE (a)-[x:`{rtype}`]->(b) "
            f"SET x = it.props"
        )
        for i in range(0, len(items), BATCH):
            session.run(cypher, items=items[i : i + BATCH])


def finalize_target(session: Session) -> None:
    """Remove prop auxiliar `_src_id` do destino."""
    session.run("MATCH (n) WHERE n._src_id IS NOT NULL REMOVE n._src_id")


def copy_subgraph(
    src_driver: Driver,
    tgt_driver: Driver,
    node_ids: set[str],
    src_db: str,
    tgt_db: str,
) -> None:
    ids = list(node_ids)
    logger.info("=== Cópia: %s nodes ===", f"{len(ids):,}")

    with src_driver.session(database=src_db) as src, tgt_driver.session(
        database=tgt_db
    ) as tgt:
        # 1. Lê nodes do source
        logger.info("Lendo nodes do source…")
        nodes = fetch_nodes(src, ids)
        logger.info("  %s lidos", len(nodes))

        # 2. Escreve nodes no target
        logger.info("Escrevendo nodes no target…")
        write_nodes(tgt, nodes)

        # 3. Cria index no target sobre `_src_id` (pra match dos rels)
        logger.info("Indexando _src_id no target…")
        tgt.run(
            "CREATE INDEX demo_src_id IF NOT EXISTS FOR (n:`_Node`) ON "
            "(n._src_id)"
        ).consume()
        # fallback: index genérico via every label não funciona — usar
        # botequim: scan + index por label é custo alto; pra <200k o
        # MATCH(_src_id) faz full scan uma vez, aceitável.
        # Remover index fake (a sintaxe acima falha silenciosamente em
        # algumas versões). Criamos um index "universal" via constraint:
        tgt.run(
            "CREATE INDEX IF NOT EXISTS FOR (n:_DemoLookup) ON (n._src_id)"
        ).consume()

        # 4. Lê rels do source
        logger.info("Lendo rels do source…")
        rels = fetch_rels(src, ids)
        logger.info("  %s lidos", len(rels))

        # 5. Escreve rels no target
        logger.info("Escrevendo rels no target…")
        write_rels(tgt, rels)

        # 6. Limpeza
        logger.info("Removendo _src_id…")
        finalize_target(tgt)

        # 7. Validação
        nc = tgt.run("MATCH (n) RETURN count(n) AS n").single()["n"]
        rc = tgt.run("MATCH ()-[r]->() RETURN count(r) AS n").single()["n"]
        logger.info("=== Destino final: %s nodes / %s rels ===", f"{nc:,}", f"{rc:,}")


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--source-uri",
        default=os.environ.get("DEMO_SOURCE_URI", "bolt://localhost:7687"),
    )
    p.add_argument("--source-user", default="neo4j")
    p.add_argument(
        "--source-password",
        default=os.environ.get("DEMO_SOURCE_PASSWORD")
        or os.environ.get("NEO4J_PASSWORD"),
    )
    p.add_argument("--source-database", default="neo4j")

    p.add_argument(
        "--target-uri",
        default=os.environ.get("DEMO_TARGET_URI", "bolt://localhost:7688"),
    )
    p.add_argument("--target-user", default="neo4j")
    p.add_argument(
        "--target-password",
        default=os.environ.get("DEMO_TARGET_PASSWORD"),
    )
    p.add_argument("--target-database", default="neo4j")

    p.add_argument(
        "--node-budget", type=int, default=DEFAULT_NODE_BUDGET,
        help=f"Orçamento de nodes no destino (default {DEFAULT_NODE_BUDGET:,})",
    )
    p.add_argument(
        "--rel-budget", type=int, default=DEFAULT_REL_BUDGET,
        help=f"Orçamento de rels no destino (default {DEFAULT_REL_BUDGET:,})",
    )
    p.add_argument("--start-year", type=int, default=2025)
    p.add_argument("--min-year", type=int, default=2018)
    p.add_argument(
        "--dry-run-only",
        action="store_true",
        help="Só mede. Não conecta no destino nem copia.",
    )
    p.add_argument(
        "--wipe-target",
        action="store_true",
        help="Necessário pra sobrescrever destino não vazio.",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    ARGS.node_budget = args.node_budget
    ARGS.rel_budget = args.rel_budget

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.source_password:
        logger.error(
            "Senha do source não informada. Use --source-password ou "
            "env NEO4J_PASSWORD."
        )
        return 2

    logger.info("Source: %s (db=%s)", args.source_uri, args.source_database)
    src_driver = GraphDatabase.driver(
        args.source_uri, auth=(args.source_user, args.source_password)
    )

    with src_driver.session(database=args.source_database) as session:
        discover(session)
        best = find_best_cutoff(
            session, args.start_year, args.min_year
        )

    if best is None:
        logger.error(
            "Nem mesmo cutoff=%s cabe no orçamento. "
            "Reduza start-year ou aumente budget.",
            args.start_year,
        )
        src_driver.close()
        return 3

    logger.info("")
    logger.info("=== Melhor cutoff: %d ===", best.cutoff_year)
    logger.info("Breakdown por tier:")
    for tier, count in best.tier_breakdown.items():
        logger.info("  %-30s %s", tier, f"{count:,}")
    logger.info(
        "TOTAL: %s nodes / %s rels",
        f"{best.node_count:,}",
        f"{best.rel_count:,}",
    )

    if args.dry_run_only:
        logger.info("Dry-run only — encerrando sem tocar no destino.")
        src_driver.close()
        return 0

    if not args.target_password:
        logger.error(
            "Senha do target não informada. Use --target-password."
        )
        src_driver.close()
        return 2

    logger.info("Target: %s (db=%s)", args.target_uri, args.target_database)
    tgt_driver = GraphDatabase.driver(
        args.target_uri, auth=(args.target_user, args.target_password)
    )

    try:
        with tgt_driver.session(database=args.target_database) as tgt:
            count = tgt.run("MATCH (n) RETURN count(n) AS n").single()["n"]
            if count > 0:
                if not args.wipe_target:
                    logger.error(
                        "Target tem %s nodes. Use --wipe-target pra confirmar.",
                        count,
                    )
                    return 4
                wipe_target(tgt)

        copy_subgraph(
            src_driver,
            tgt_driver,
            best.node_ids,
            args.source_database,
            args.target_database,
        )
    finally:
        src_driver.close()
        tgt_driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())

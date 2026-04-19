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

# --- Eventos diretos do seed expandido (sem filtro temporal, pq dates
#     no grafo são esparsos/vazios) ---

# Amendments (via PROPOS ou AUTOR_EMENDA) do seed expandido
AMENDMENT_QUERY = """
MATCH (p)-[r]->(a:Amendment)
WHERE elementId(p) IN $seed_ids
  AND type(r) IN ['PROPOS', 'AUTOR_EMENDA']
RETURN collect(DISTINCT elementId(a)) AS ids
"""

# LegislativeExpense (CEAP + ALEGO) — mantém filtro por ano (prop sólida)
LEGISLATIVE_EXPENSE_QUERY = """
MATCH (p)-[r]->(e:LegislativeExpense)
WHERE elementId(p) IN $seed_ids
  AND type(r) IN ['INCURRED', 'GASTOU_COTA_GO', 'GASTOU']
  AND coalesce(toInteger(e.ano), 0) >= $year
RETURN collect(DISTINCT elementId(e)) AS ids
"""

# Declared assets (bens declarados pelos políticos)
DECLARED_ASSET_QUERY = """
MATCH (p)-[:DECLAROU_BEM]->(a:DeclaredAsset)
WHERE elementId(p) IN $seed_ids
RETURN collect(DISTINCT elementId(a)) AS ids
"""

# Candidaturas do seed
CANDIDATO_EM_QUERY = """
MATCH (p)-[:CANDIDATO_EM]->(e:Election)
WHERE elementId(p) IN $seed_ids
RETURN collect(DISTINCT elementId(e)) AS ids
"""

# Filiações partidárias
PARTY_MEMBERSHIP_QUERY = """
MATCH (p)-[:FILIADO_A]->(pm:PartyMembership)
WHERE elementId(p) IN $seed_ids
RETURN collect(DISTINCT elementId(pm)) AS ids
"""

# Doações: tudo que DOOU pro político OU que político DOOU
# (Company/CampaignDonor/Person como doador; Person/Company como alvo)
DONATIONS_QUERY = """
MATCH (src)-[:DOOU]->(tgt)
WHERE elementId(src) IN $seed_ids OR elementId(tgt) IN $seed_ids
RETURN collect(DISTINCT elementId(src)) + collect(DISTINCT elementId(tgt)) AS ids
"""

# --- Eventos datados (contexto GO mais amplo, filtrado por cutoff) ---

# Sanction — date_start >= cutoff_iso
SANCTION_QUERY = """
MATCH (s:Sanction)
WHERE coalesce(s.date_start, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(s)) AS ids
"""

# Atos do diário oficial GO (volume alto - 10k)
GAZETTE_QUERY = """
MATCH (g:GoGazetteAct)
WHERE coalesce(g.date, '') >= $cutoff_iso
   OR coalesce(g.published_at, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(g)) AS ids
"""

# Licitações municipais GO
GO_PROCUREMENT_QUERY = """
MATCH (g:GoProcurement)
WHERE coalesce(g.published_at, '') >= $cutoff_iso
   OR coalesce(g.date, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(g)) AS ids
"""

# Eleições (Election, 54 total — incluir todas)
ELECTION_QUERY = """
MATCH (e:Election)
RETURN collect(DISTINCT elementId(e)) AS ids
"""

# --- Dados eleitorais GO (Campaign*) — label inteiro é GO-scoped ---

# Gastos de campanha GO por ano
CAMPAIGN_EXPENSE_QUERY = """
MATCH (c:CampaignExpense)
WHERE c.uf = 'GO' AND coalesce(toInteger(c.ano), 0) >= $year
RETURN collect(DISTINCT elementId(c)) AS ids
"""

# Doações eleitorais GO por ano
CAMPAIGN_DONATION_QUERY = """
MATCH (c:CampaignDonation)
WHERE c.uf = 'GO' AND coalesce(toInteger(c.ano), 0) >= $year
RETURN collect(DISTINCT elementId(c)) AS ids
"""

# Doadores canônicos (sem uf — pegar todos que receberam DOOU de Person GO)
CAMPAIGN_DONOR_QUERY = """
MATCH (p:Person)-[:DOOU]->(cd:CampaignDonor)
WHERE p.uf = 'GO'
RETURN collect(DISTINCT elementId(cd)) AS ids
"""

# Bens declarados de todos Person GO (não só seed)
DECLARED_ASSET_GO_QUERY = """
MATCH (p:Person)-[:DECLAROU_BEM]->(a:DeclaredAsset)
WHERE p.uf = 'GO'
RETURN collect(DISTINCT elementId(a)) AS ids
"""

# Person GO (candidatos históricos + atuais). Entra como nodes
# standalone — NÃO usamos esses IDs como seed pra queries downstream
# (pra evitar explosão via DOOU).
PERSONS_GO_QUERY = """
MATCH (p:Person)
WHERE p.uf = 'GO'
RETURN collect(DISTINCT elementId(p)) AS ids
"""

# Empresas que doaram pra qualquer Person GO (não só seed). Amplia o
# tier empresas_doadoras sem expandir o seed.
COMPANIES_ANY_GO_PERSON_QUERY = """
MATCH (c:Company)-[:DOOU]->(p:Person)
WHERE p.uf = 'GO'
RETURN collect(DISTINCT elementId(c)) AS ids
"""

# StateEmployee comissionado (volume baixo — todos 695 incluídos)
STATE_EMPLOYEE_COMMISSIONED_QUERY = """
MATCH (s:StateEmployee)
WHERE s.is_commissioned = true
RETURN collect(DISTINCT elementId(s)) AS ids
"""

# StateAgency (44 nodes — todas, pra as rels LOTADO_EM fazerem sentido)
STATE_AGENCY_QUERY = """
MATCH (a:StateAgency)
RETURN collect(DISTINCT elementId(a)) AS ids
"""

# Gastos municipais GO (MunicipalExpenditure → GoMunicipality via GASTOU)
MUNICIPAL_EXPENDITURE_QUERY = """
MATCH (m:MunicipalExpenditure)-[:GASTOU]->(gm:GoMunicipality)
RETURN collect(DISTINCT elementId(m)) AS ids
"""

# Receitas municipais GO
MUNICIPAL_REVENUE_QUERY = """
MATCH (m:MunicipalRevenue)
RETURN collect(DISTINCT elementId(m)) AS ids
"""

# Finance (dívidas fiscais) de empresas admitidas — filtro por data
FINANCE_FROM_COMPANIES_QUERY = """
MATCH (c:Company)-[:DEVE]->(f:Finance)
WHERE elementId(c) IN $company_ids
  AND coalesce(f.date, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(f)) AS ids
"""

# --- 2-hop: atores financeiros ligados via doações/licitações ---

# Empresas que doaram aos políticos OU que venceram GoProcurement admitido
COMPANIES_FROM_DONATIONS_QUERY = """
MATCH (c:Company)-[:DOOU]->(tgt)
WHERE elementId(tgt) IN $seed_ids
RETURN collect(DISTINCT elementId(c)) AS ids
"""

COMPANIES_FROM_GO_PROCUREMENT_QUERY = """
MATCH (c:Company)-[:CONTRATOU_GO]->(g:GoProcurement)
WHERE elementId(g) IN $procurement_ids
RETURN collect(DISTINCT elementId(c)) AS ids
"""

# Contratos federais dessas empresas no período
CONTRACT_FROM_COMPANIES_QUERY = """
MATCH (c:Company)-[:VENCEU]->(ct:Contract)
WHERE elementId(c) IN $company_ids
  AND coalesce(ct.date, '') >= $cutoff_iso
RETURN collect(DISTINCT elementId(ct)) AS ids
"""

# Sócios das empresas admitidas (Person é a única label de sócio nesse grafo)
SOCIOS_QUERY = """
MATCH (c:Company)-[:SOCIO_DE_SNAPSHOT|SOCIO_DE]-(p:Person)
WHERE elementId(c) IN $company_ids
RETURN collect(DISTINCT elementId(p)) AS ids
"""

# --- Contagem de relações entre nodes admitidos ---
# Direção fixa (->): cada rel aparece uma vez, sem precisar de dedup.
REL_COUNT_QUERY = """
MATCH (a)-[r]->(b)
WHERE elementId(a) IN $ids AND elementId(b) IN $ids
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
    seed_expanded = seed | represents
    municipalities = run_ids(session, GO_MUNICIPALITY_QUERY)

    tier1 = seed_expanded | municipalities
    accumulated |= tier1
    result.tier_breakdown["seed+represents+municipios"] = len(tier1)

    # --- Eventos diretos do seed expandido ---
    seed_list = list(seed_expanded)
    amendments = run_ids(session, AMENDMENT_QUERY, seed_ids=seed_list)
    expenses = run_ids(
        session, LEGISLATIVE_EXPENSE_QUERY, seed_ids=seed_list, year=cutoff_year
    )
    assets = run_ids(session, DECLARED_ASSET_QUERY, seed_ids=seed_list)
    candidaturas = run_ids(session, CANDIDATO_EM_QUERY, seed_ids=seed_list)
    filiacoes = run_ids(session, PARTY_MEMBERSHIP_QUERY, seed_ids=seed_list)
    donation_nodes = run_ids(session, DONATIONS_QUERY, seed_ids=seed_list)
    elections = run_ids(session, ELECTION_QUERY)

    tier2 = (
        amendments | expenses | assets | candidaturas | filiacoes
        | donation_nodes | elections
    )
    accumulated |= tier2
    result.tier_breakdown["eventos_diretos"] = len(tier2)

    # --- 2-hop: empresas doadoras (seed) + ampla (qualquer Person GO) ---
    companies_donors = run_ids(
        session, COMPANIES_FROM_DONATIONS_QUERY, seed_ids=seed_list
    )
    companies_any_go = run_ids(session, COMPANIES_ANY_GO_PERSON_QUERY)
    companies_all_donors = companies_donors | companies_any_go
    accumulated |= companies_all_donors
    result.tier_breakdown["empresas_doadoras"] = len(companies_all_donors)

    # --- Contexto GO amplo (datado) ---
    gazette = run_ids(session, GAZETTE_QUERY, cutoff_iso=cutoff_iso)
    go_procurement = run_ids(
        session, GO_PROCUREMENT_QUERY, cutoff_iso=cutoff_iso
    )
    tier_go = gazette | go_procurement
    accumulated |= tier_go
    result.tier_breakdown["contexto_go"] = len(tier_go)

    # --- 2-hop: empresas em licitações GO admitidas ---
    companies_go = run_ids(
        session,
        COMPANIES_FROM_GO_PROCUREMENT_QUERY,
        procurement_ids=list(go_procurement),
    )
    accumulated |= companies_go
    result.tier_breakdown["empresas_licitacao_go"] = len(companies_go)

    # --- 2-hop: contratos federais das empresas doadoras+licitantes ---
    all_companies = companies_all_donors | companies_go
    contracts = run_ids(
        session,
        CONTRACT_FROM_COMPANIES_QUERY,
        company_ids=list(all_companies),
        cutoff_iso=cutoff_iso,
    )
    accumulated |= contracts
    result.tier_breakdown["contratos"] = len(contracts)

    # --- 2-hop: sócios dessas empresas ---
    socios = run_ids(session, SOCIOS_QUERY, company_ids=list(all_companies))
    accumulated |= socios
    result.tier_breakdown["socios"] = len(socios)

    # --- Sanções no período ---
    sanctions = run_ids(session, SANCTION_QUERY, cutoff_iso=cutoff_iso)
    accumulated |= sanctions
    result.tier_breakdown["sancoes"] = len(sanctions)

    # --- Dados eleitorais GO (Campaign*) ---
    campaign_expense = run_ids(
        session, CAMPAIGN_EXPENSE_QUERY, year=cutoff_year
    )
    campaign_donation = run_ids(
        session, CAMPAIGN_DONATION_QUERY, year=cutoff_year
    )
    campaign_donors = run_ids(session, CAMPAIGN_DONOR_QUERY)
    tier_campaign = campaign_expense | campaign_donation | campaign_donors
    accumulated |= tier_campaign
    result.tier_breakdown["campaign"] = len(tier_campaign)

    # --- Bens declarados de todos Person GO ---
    assets_go = run_ids(session, DECLARED_ASSET_GO_QUERY)
    accumulated |= assets_go
    result.tier_breakdown["declared_assets_go"] = len(assets_go)

    # --- Finanças municipais GO ---
    municipal_exp = run_ids(session, MUNICIPAL_EXPENDITURE_QUERY)
    municipal_rev = run_ids(session, MUNICIPAL_REVENUE_QUERY)
    tier_muni = municipal_exp | municipal_rev
    accumulated |= tier_muni
    result.tier_breakdown["financas_municipais"] = len(tier_muni)

    # --- Finance (dívidas fiscais) das empresas admitidas ---
    finances = run_ids(
        session,
        FINANCE_FROM_COMPANIES_QUERY,
        company_ids=list(all_companies),
        cutoff_iso=cutoff_iso,
    )
    accumulated |= finances
    result.tier_breakdown["finance_empresas"] = len(finances)

    # --- Persons GO (candidatos históricos + atuais, standalone) ---
    persons_go = run_ids(session, PERSONS_GO_QUERY)
    accumulated |= persons_go
    result.tier_breakdown["persons_go"] = len(persons_go)

    # --- StateEmployee comissionado + agências ---
    employees = run_ids(session, STATE_EMPLOYEE_COMMISSIONED_QUERY)
    agencies = run_ids(session, STATE_AGENCY_QUERY)
    tier_state = employees | agencies
    accumulated |= tier_state
    result.tier_breakdown["comissionados+agencias"] = len(tier_state)

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
            # Primeiro estouro — loga breakdown pra diagnóstico
            logger.info("  (breakdown excedente):")
            for tier, count in r.tier_breakdown.items():
                logger.info("    %-30s %s", tier, f"{count:,}")
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
    """Lê rels saindo do conjunto admitido. Filtra endpoints em Python."""
    id_set = set(ids)
    rows: list[dict] = []
    # Particionar pela ponta `a` (source). Cada rel aparece uma vez.
    for i in range(0, len(ids), BATCH):
        chunk = ids[i : i + BATCH]
        result = session.run(
            """
            MATCH (a)-[r]->(b)
            WHERE elementId(a) IN $chunk
            RETURN elementId(a) AS src_id, elementId(b) AS tgt_id,
                   type(r) AS type, properties(r) AS props
            """,
            chunk=chunk,
        )
        for r in result:
            # Só mantém rels cujo target também está no conjunto
            if r["tgt_id"] in id_set:
                rows.append(dict(r))
    return rows


def write_rels(session: Session, rels: list[dict]) -> int:
    """Cria rels no target agrupadas por tipo. Retorna total criado."""
    by_type: dict[str, list[dict]] = defaultdict(list)
    for r in rels:
        by_type[r["type"]].append(r)

    total_created = 0
    for rtype, items in by_type.items():
        cypher = (
            f"UNWIND $items AS it "
            f"MATCH (a:_Imported {{_src_id: it.src_id}}) "
            f"MATCH (b:_Imported {{_src_id: it.tgt_id}}) "
            f"CREATE (a)-[x:`{rtype}`]->(b) "
            f"SET x = it.props "
            f"RETURN count(x) AS n"
        )
        for i in range(0, len(items), BATCH):
            r = session.run(cypher, items=items[i : i + BATCH]).single()
            total_created += int(r["n"]) if r else 0
    return total_created


def finalize_target(session: Session) -> None:
    """Remove prop auxiliar `_src_id` e label `_Imported` do destino."""
    # Em batches pra não estourar memória de transação
    while True:
        r = session.run(
            "MATCH (n:_Imported) WITH n LIMIT 5000 "
            "REMOVE n._src_id, n:_Imported "
            "RETURN count(n) AS n"
        ).single()
        n = int(r["n"]) if r else 0
        if n == 0:
            break


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

        # 2. Index sobre _src_id no label aux _Imported (pra match rápido)
        logger.info("Criando index _Imported(_src_id) no target…")
        tgt.run(
            "CREATE INDEX demo_imported_src_id IF NOT EXISTS "
            "FOR (n:_Imported) ON (n._src_id)"
        ).consume()

        # 3. Escreve nodes no target
        logger.info("Escrevendo nodes no target…")
        write_nodes(tgt, nodes)

        # 4. Lê rels do source
        logger.info("Lendo rels do source…")
        rels = fetch_rels(src, ids)
        logger.info("  %s lidos", len(rels))

        # 5. Escreve rels no target
        logger.info("Escrevendo rels no target…")
        created = write_rels(tgt, rels)
        if created < len(rels):
            logger.warning(
                "Apenas %s/%s rels criadas (gap pode indicar endpoints "
                "fora do conjunto).",
                created,
                len(rels),
            )

        # 6. Limpeza — remove label aux + prop
        logger.info("Removendo _src_id + label _Imported…")
        finalize_target(tgt)
        tgt.run("DROP INDEX demo_imported_src_id IF EXISTS").consume()

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

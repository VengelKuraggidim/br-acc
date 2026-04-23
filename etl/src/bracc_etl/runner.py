import inspect
import logging
import os
from pathlib import Path
from typing import Any

import click
from neo4j import GraphDatabase

from bracc_etl.linking_hooks import run_post_load_hooks
from bracc_etl.pipelines.alego import AlegoPipeline
from bracc_etl.pipelines.alego_deputados_foto import AlegoDeputadosFotoPipeline
from bracc_etl.pipelines.bcb import BcbPipeline
from bracc_etl.pipelines.bndes import BndesPipeline
from bracc_etl.pipelines.brasilapi_cnpj_status import (
    BrasilapiCnpjStatusPipeline,
)
from bracc_etl.pipelines.caged import CagedPipeline
from bracc_etl.pipelines.camara import CamaraPipeline
from bracc_etl.pipelines.camara_goiania import CamaraGoianiaPipeline
from bracc_etl.pipelines.camara_inquiries import CamaraInquiriesPipeline
from bracc_etl.pipelines.camara_politicos_go import CamaraPoliticosGoPipeline
from bracc_etl.pipelines.ceaf import CeafPipeline
from bracc_etl.pipelines.cepim import CepimPipeline
from bracc_etl.pipelines.cnpj import CNPJPipeline
from bracc_etl.pipelines.comprasnet import ComprasnetPipeline
from bracc_etl.pipelines.cpgf import CpgfPipeline
from bracc_etl.pipelines.custo_mandato_br import CustoMandatoBrPipeline
from bracc_etl.pipelines.custo_mandato_municipal_go import (
    CustoMandatoMunicipalGoPipeline,
)
from bracc_etl.pipelines.cvm import CvmPipeline
from bracc_etl.pipelines.cvm_funds import CvmFundsPipeline
from bracc_etl.pipelines.datajud import DatajudPipeline
from bracc_etl.pipelines.datasus import DatasusPipeline
from bracc_etl.pipelines.dou import DouPipeline
from bracc_etl.pipelines.emendas_parlamentares_go import (
    EmendasParlamentaresGoPipeline,
)
from bracc_etl.pipelines.entity_resolution_politicos_go import (
    EntityResolutionPoliticosGoPipeline,
)
from bracc_etl.pipelines.eu_sanctions import EuSanctionsPipeline
from bracc_etl.pipelines.folha_go import FolhaGoPipeline
from bracc_etl.pipelines.holdings import HoldingsPipeline
from bracc_etl.pipelines.ibama import IbamaPipeline
from bracc_etl.pipelines.icij import ICIJPipeline
from bracc_etl.pipelines.inep import InepPipeline
from bracc_etl.pipelines.leniency import LeniencyPipeline
from bracc_etl.pipelines.mides import MidesPipeline
from bracc_etl.pipelines.ofac import OfacPipeline
from bracc_etl.pipelines.opensanctions import OpenSanctionsPipeline
from bracc_etl.pipelines.pep_cgu import PepCguPipeline
from bracc_etl.pipelines.pgfn import PgfnPipeline
from bracc_etl.pipelines.pncp import PncpPipeline
from bracc_etl.pipelines.pncp_go import PncpGoPipeline
from bracc_etl.pipelines.propagacao_fotos_person import (
    PropagacaoFotosPersonPipeline,
)
from bracc_etl.pipelines.querido_diario_go import QueridoDiarioGoPipeline
from bracc_etl.pipelines.rais import RaisPipeline
from bracc_etl.pipelines.renuncias import RenunciasPipeline
from bracc_etl.pipelines.sanctions import SanctionsPipeline
from bracc_etl.pipelines.senado import SenadoPipeline
from bracc_etl.pipelines.senado_cpis import SenadoCpisPipeline
from bracc_etl.pipelines.senado_senadores_foto import (
    SenadoSenadoresFotoPipeline,
)
from bracc_etl.pipelines.siconfi import SiconfiPipeline
from bracc_etl.pipelines.siop import SiopPipeline
from bracc_etl.pipelines.ssp_go import SspGoPipeline
from bracc_etl.pipelines.state_portal_go import StatePortalGoPipeline
from bracc_etl.pipelines.stf import StfPipeline
from bracc_etl.pipelines.stj_dados_abertos import StjPipeline
from bracc_etl.pipelines.tce_go import TceGoPipeline
from bracc_etl.pipelines.tcm_go import TcmGoPipeline
from bracc_etl.pipelines.tcmgo_sancoes import TcmgoSancoesPipeline
from bracc_etl.pipelines.tcu import TcuPipeline
from bracc_etl.pipelines.tesouro_emendas import TesouroEmendasPipeline
from bracc_etl.pipelines.transferegov import TransferegovPipeline
from bracc_etl.pipelines.transparencia import TransparenciaPipeline
from bracc_etl.pipelines.tse import TSEPipeline
from bracc_etl.pipelines.tse_bens import TseBensPipeline
from bracc_etl.pipelines.tse_candidatos_foto import TseCandidatosFotoPipeline
from bracc_etl.pipelines.tse_filiados import TseFiliadosPipeline
from bracc_etl.pipelines.tse_prestacao_contas_go import TsePrestacaoContasGoPipeline
from bracc_etl.pipelines.un_sanctions import UnSanctionsPipeline
from bracc_etl.pipelines.viagens import ViagensPipeline
from bracc_etl.pipelines.wikidata_politicos_foto import (
    WikidataPoliticosFotoPipeline,
)
from bracc_etl.pipelines.world_bank import WorldBankPipeline

PIPELINES: dict[str, type] = {
    "cnpj": CNPJPipeline,
    "tse": TSEPipeline,
    "transparencia": TransparenciaPipeline,
    "sanctions": SanctionsPipeline,
    "pep_cgu": PepCguPipeline,
    "bndes": BndesPipeline,
    "pgfn": PgfnPipeline,
    "ibama": IbamaPipeline,
    "comprasnet": ComprasnetPipeline,
    "tcu": TcuPipeline,
    "transferegov": TransferegovPipeline,
    "rais": RaisPipeline,
    "inep": InepPipeline,
    "dou": DouPipeline,
    "datasus": DatasusPipeline,
    "icij": ICIJPipeline,
    "opensanctions": OpenSanctionsPipeline,
    "cvm": CvmPipeline,
    "cvm_funds": CvmFundsPipeline,
    "camara": CamaraPipeline,
    "camara_inquiries": CamaraInquiriesPipeline,
    "senado": SenadoPipeline,
    "ceaf": CeafPipeline,
    "cepim": CepimPipeline,
    "cpgf": CpgfPipeline,
    "leniency": LeniencyPipeline,
    "ofac": OfacPipeline,
    "holdings": HoldingsPipeline,
    "viagens": ViagensPipeline,
    "siop": SiopPipeline,
    "pncp": PncpPipeline,
    "renuncias": RenunciasPipeline,
    "siconfi": SiconfiPipeline,
    "tse_bens": TseBensPipeline,
    "tse_filiados": TseFiliadosPipeline,
    "bcb": BcbPipeline,
    "stf": StfPipeline,
    "caged": CagedPipeline,
    "eu_sanctions": EuSanctionsPipeline,
    "un_sanctions": UnSanctionsPipeline,
    "world_bank": WorldBankPipeline,
    "senado_cpis": SenadoCpisPipeline,
    "mides": MidesPipeline,
    "datajud": DatajudPipeline,
    "tesouro_emendas": TesouroEmendasPipeline,
    "stj_dados_abertos": StjPipeline,
    # Goiás state/municipal pipelines
    "folha_go": FolhaGoPipeline,
    "tcm_go": TcmGoPipeline,
    "pncp_go": PncpGoPipeline,
    "querido_diario_go": QueridoDiarioGoPipeline,
    "camara_goiania": CamaraGoianiaPipeline,
    "state_portal_go": StatePortalGoPipeline,
    "tce_go": TceGoPipeline,
    "alego": AlegoPipeline,
    "alego_deputados_foto": AlegoDeputadosFotoPipeline,
    "tcmgo_sancoes": TcmgoSancoesPipeline,
    "ssp_go": SspGoPipeline,
    "camara_politicos_go": CamaraPoliticosGoPipeline,
    "senado_senadores_foto": SenadoSenadoresFotoPipeline,
    "tse_prestacao_contas_go": TsePrestacaoContasGoPipeline,
    "emendas_parlamentares_go": EmendasParlamentaresGoPipeline,
    "brasilapi_cnpj_status": BrasilapiCnpjStatusPipeline,
    "wikidata_politicos_foto": WikidataPoliticosFotoPipeline,
    "tse_candidatos_foto": TseCandidatosFotoPipeline,
    "propagacao_fotos_person": PropagacaoFotosPersonPipeline,
    "entity_resolution_politicos_go": EntityResolutionPoliticosGoPipeline,
    "custo_mandato_br": CustoMandatoBrPipeline,
    "custo_mandato_municipal_go": CustoMandatoMunicipalGoPipeline,
}


def _pipeline_init_params(pipeline_cls: type) -> set[str]:
    """Set de nomes de parametros explicitos do ``__init__`` do pipeline.

    Usado pra decidir se um flag CLI opcional (ex.: ``--batch-size``)
    e passavel pro ctor sem quebrar pipelines que nao o aceitam.
    Retorna set vazio se a introspeccao falhar — comportamento conservador.
    """
    try:
        sig = inspect.signature(pipeline_cls)
    except (TypeError, ValueError):
        return set()
    return {name for name in sig.parameters if name != "self"}


def _pipeline_accepts_kwarg(pipeline_cls: type, name: str) -> bool:
    """Whether ``pipeline_cls.__init__`` accepts *name* as a kwarg.

    Duas formas suportadas:

    1. *name* e parametro keyword explicito de ``__init__``.
    2. ``__init__`` declara ``**kwargs`` **e** consome *name* via
       ``kwargs.pop("name"...)`` — padrao usado por CamaraPoliticosGo
       pra optional kwargs. Verificar so a presenca de ``**kwargs`` nao
       basta: pipelines como brasilapi_cnpj_status declaram **kwargs
       mas encaminham pro ``Pipeline.__init__`` base (que nao aceita
       kwargs arbitrarios) — passar algo nao-consumido da TypeError.

    Retorna False se a introspeccao falhar — comportamento conservador.
    """
    try:
        sig = inspect.signature(pipeline_cls)
    except (TypeError, ValueError):
        return False
    has_var_kw = False
    for pname, param in sig.parameters.items():
        if pname == name:
            return True
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            has_var_kw = True
    if not has_var_kw:
        return False
    try:
        src = inspect.getsource(pipeline_cls.__init__)
    except (OSError, TypeError):
        return False
    return f'kwargs.pop("{name}"' in src or f"kwargs.pop('{name}'" in src


def _load_dotenv_if_present() -> None:
    """Carrega ``.env`` do repo root em ``os.environ`` (sem sobrescrever).

    ``uv run`` não carrega ``.env`` automaticamente. Pipelines CLI
    (``bracc-etl run ...``) precisam de ``GCP_PROJECT_ID`` pro
    ``load_secret`` funcionar. Procuramos o ``.env`` no cwd e até 3
    pais — cobre rodar do repo root ou de ``etl/``.

    Parse minimalista (``KEY=VALUE``, ignora ``#`` e linhas vazias,
    strip de aspas). Evita dep nova só pra isso.
    """
    for directory in [Path.cwd(), *Path.cwd().parents[:3]]:
        env_file = directory / ".env"
        if not env_file.is_file():
            continue
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
        return  # Primeiro .env encontrado vence.


@click.group()
def cli() -> None:
    """BR-ACC ETL — Data ingestion pipelines for Brazilian public data."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    _load_dotenv_if_present()


@cli.command()
@click.option("--source", required=True, help="Pipeline name (see 'sources' command)")
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j", help="Neo4j user")
@click.option(
    "--neo4j-password",
    default=None,
    help="Neo4j password (default: busca via GCP Secret Manager fiscal-cidadao-neo4j-password)",
)
@click.option("--neo4j-database", default="neo4j", help="Neo4j database")
@click.option("--data-dir", default="./data", help="Directory for downloaded data")
@click.option("--limit", type=int, default=None, help="Limit rows processed")
@click.option("--chunk-size", type=int, default=50_000, help="Chunk size for batch processing")
@click.option(
    "--linking-tier",
    type=click.Choice(["community", "full"]),
    default=os.getenv("LINKING_TIER", "full"),
    show_default=True,
    help="Post-load linking strategy tier",
)
@click.option("--streaming/--no-streaming", default=False, help="Streaming mode")
@click.option("--start-phase", type=int, default=1, help="Skip to phase N")
@click.option("--history/--no-history", default=False, help="Enable history mode when supported")
@click.option(
    "--batch-size",
    type=int,
    default=None,
    help=(
        "Per-run batch size (only honored by pipelines that accept it, "
        "e.g. brasilapi_cnpj_status)."
    ),
)
@click.option(
    "--start-year",
    type=int,
    default=None,
    help=(
        "Earliest year to ingest (only honored by pipelines that accept "
        "it, e.g. camara_politicos_go / camara_deputados_ceap). Util pra "
        "limitar volume em Aura Free tier."
    ),
)
def run(
    source: str,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str | None,
    neo4j_database: str,
    data_dir: str,
    limit: int | None,
    chunk_size: int,
    linking_tier: str,
    streaming: bool,
    start_phase: int,
    history: bool,
    batch_size: int | None,
    start_year: int | None,
) -> None:
    """Run an ETL pipeline."""
    os.environ["NEO4J_DATABASE"] = neo4j_database

    if source not in PIPELINES:
        available = ", ".join(PIPELINES.keys())
        raise click.ClickException(f"Unknown source: {source}. Available: {available}")

    if not neo4j_password:
        from bracc_etl.secrets import SecretNotFoundError, load_secret

        try:
            neo4j_password = load_secret("neo4j-password")
        except SecretNotFoundError as exc:
            raise click.ClickException(
                f"--neo4j-password ausente: passe via CLI ou configure "
                f"GCP_PROJECT_ID + secret 'fiscal-cidadao-neo4j-password'. "
                f"Detalhe: {exc}"
            ) from None

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        pipeline_cls = PIPELINES[source]
        extra_kwargs: dict[str, Any] = {}
        # batch_size e opt-in por pipeline: so passa quando o __init__
        # aceita (evita TypeError nos pipelines que nao mexem com isso).
        if batch_size is not None and "batch_size" in _pipeline_init_params(
            pipeline_cls,
        ):
            extra_kwargs["batch_size"] = batch_size
        # start_year e consumido via kwargs.pop nos pipelines que suportam
        # (ex.: CamaraPoliticosGoPipeline), entao precisa do guard que olha
        # pra **kwargs, nao so params explicitos.
        if start_year is not None and _pipeline_accepts_kwarg(
            pipeline_cls, "start_year",
        ):
            extra_kwargs["start_year"] = start_year
        pipeline = pipeline_cls(
            driver=driver,
            data_dir=data_dir,
            limit=limit,
            chunk_size=chunk_size,
            history=history,
            **extra_kwargs,
        )

        if streaming and hasattr(pipeline, "run_streaming"):
            pipeline.run_streaming(start_phase=start_phase)
        else:
            pipeline.run()

        run_post_load_hooks(
            driver=driver,
            source=source,
            neo4j_database=neo4j_database,
            linking_tier=linking_tier,
        )
    finally:
        driver.close()


def _resolve_rf_release_inline(year_month: str | None = None) -> str:
    """Resolve Receita Federal CNPJ release URL.

    Tries the current arquivos.receitafederal.gov.br monthly archive path first,
    then Nextcloud shares, then older dadosabertos fallbacks.
    """
    from datetime import UTC, datetime

    import httpx

    now = datetime.now(UTC)
    if year_month is not None:
        candidates = [year_month]
    else:
        candidates = []
        cursor = now.replace(day=1)
        for _ in range(12):
            candidates.append(f"{cursor.year:04d}-{cursor.month:02d}")
            if cursor.month == 1:
                cursor = cursor.replace(year=cursor.year - 1, month=12)
            else:
                cursor = cursor.replace(month=cursor.month - 1)

    # --- Current archive path (authoritative monthly releases) ---
    archive_base = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ym}/"
    for ym in candidates:
        url = archive_base.format(ym=ym)
        try:
            resp = httpx.head(url, follow_redirects=True, timeout=30)
            if resp.status_code < 400:
                return url
        except httpx.HTTPError:
            pass

    # --- Nextcloud (legacy interim path) ---
    nextcloud_dl = "https://arquivos.receitafederal.gov.br/s/{token}/download?path=%2F&files="
    tokens: list[str] = []
    env_token = os.environ.get("CNPJ_SHARE_TOKEN")
    if env_token:
        tokens.append(env_token)
    tokens.extend(["gn672Ad4CF8N6TK", "YggdBLfdninEJX9"])

    for token in tokens:
        share_url = f"https://arquivos.receitafederal.gov.br/s/{token}"
        try:
            resp = httpx.head(share_url, follow_redirects=True, timeout=30)
            if resp.status_code < 400:
                return nextcloud_dl.format(token=token)
        except httpx.HTTPError:
            pass

    # --- Legacy dadosabertos (fallback) ---
    new_base = "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/{ym}/"
    legacy_url = "https://dadosabertos.rfb.gov.br/CNPJ/"

    for ym in candidates:
        url = new_base.format(ym=ym)
        try:
            resp = httpx.head(url, follow_redirects=True, timeout=30)
            if resp.status_code < 400:
                return url
        except httpx.HTTPError:
            pass

    try:
        resp = httpx.head(legacy_url, follow_redirects=True, timeout=30)
        if resp.status_code < 400:
            return legacy_url
    except httpx.HTTPError:
        pass

    tried = ", ".join(candidates)
    msg = f"Could not resolve CNPJ release. Tried Nextcloud tokens, months [{tried}], and legacy."
    raise RuntimeError(msg)


@cli.command()
@click.option("--output-dir", default="./data/cnpj", help="Output directory")
@click.option("--files", type=int, default=10, help="Number of files per type (0-9)")
@click.option("--skip-existing/--no-skip-existing", default=True)
@click.option("--release", default=None, help="Pin to specific monthly release (YYYY-MM)")
def download(output_dir: str, files: int, skip_existing: bool, release: str | None) -> None:
    """Download CNPJ data from Receita Federal."""
    import zipfile
    from pathlib import Path

    import httpx

    logger = logging.getLogger(__name__)

    base_url = _resolve_rf_release_inline(release)
    logger.info("Using CNPJ release URL: %s", base_url)
    file_types = ["Empresas", "Socios", "Estabelecimentos"]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    for file_type in file_types:
        for i in range(min(files, 10)):
            filename = f"{file_type}{i}.zip"
            url = f"{base_url}{filename}"
            dest = out / filename
            try:
                if skip_existing and dest.exists():
                    logger.info("Skipping (exists): %s", dest.name)
                    continue

                logger.info("Downloading %s...", url)
                with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
                    response.raise_for_status()
                    with open(dest, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)
                logger.info("Downloaded: %s", dest.name)

                logger.info("Extracting %s...", dest.name)
                with zipfile.ZipFile(dest, "r") as zf:
                    # Path traversal guard
                    out_resolved = out.resolve()
                    safe = True
                    for info in zf.infolist():
                        target = (out / info.filename).resolve()
                        if not target.is_relative_to(out_resolved):
                            logger.warning(
                                "Path traversal in %s: %s — skipping archive",
                                dest.name,
                                info.filename,
                            )
                            safe = False
                            break
                    if not safe:
                        continue
                    # Zip bomb guard (50 GB limit for CNPJ data)
                    total = sum(i.file_size for i in zf.infolist())
                    if total > 50 * 1024**3:
                        logger.warning(
                            "Uncompressed size too large: %s (%.1f GB) — skipping",
                            dest.name,
                            total / 1e9,
                        )
                        continue
                    zf.extractall(out)
            except httpx.HTTPError:
                logger.warning("Failed to download %s (may not exist)", filename)


@cli.command()
@click.option("--status", "show_status", is_flag=True, help="Show ingestion status from Neo4j")
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j")
@click.option("--neo4j-password", default=None)
def sources(show_status: bool, neo4j_uri: str, neo4j_user: str, neo4j_password: str | None) -> None:
    """List available data sources."""
    if not show_status:
        click.echo("Available pipelines:")
        for name in sorted(PIPELINES):
            click.echo(f"  {name}")
        return

    if not neo4j_password:
        from bracc_etl.secrets import SecretNotFoundError, load_secret

        try:
            neo4j_password = load_secret("neo4j-password")
        except SecretNotFoundError as exc:
            raise click.ClickException(
                f"--neo4j-password ausente: passe via CLI ou configure "
                f"GCP_PROJECT_ID + secret 'fiscal-cidadao-neo4j-password'. "
                f"Detalhe: {exc}"
            ) from None

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as session:
            result = session.run(
                "MATCH (r:IngestionRun) "
                "WITH r ORDER BY r.started_at DESC "
                "WITH r.source_id AS sid, collect(r)[0] AS latest "
                "RETURN latest ORDER BY sid"
            )
            runs = {r["latest"]["source_id"]: dict(r["latest"]) for r in result}

        click.echo(
            f"{'Source':<20} {'Status':<15} {'Rows In':>10} {'Loaded':>10} "
            f"{'Started':<20} {'Finished':<20}"
        )
        click.echo("-" * 100)

        for name in sorted(PIPELINES):
            run = runs.get(name, {})
            click.echo(
                f"{name:<20} "
                f"{run.get('status', '-'):<15} "
                f"{run.get('rows_in', 0):>10,} "
                f"{run.get('rows_loaded', 0):>10,} "
                f"{str(run.get('started_at', '-')):<20} "
                f"{str(run.get('finished_at', '-')):<20}"
            )
    finally:
        driver.close()


if __name__ == "__main__":
    cli()

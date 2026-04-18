from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.tcm_go import TcmGoPipeline
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def pipeline() -> TcmGoPipeline:
    driver = MagicMock()
    return TcmGoPipeline(driver=driver, data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self, pipeline: TcmGoPipeline) -> None:
        assert pipeline.name == "tcm_go"

    def test_source_id(self, pipeline: TcmGoPipeline) -> None:
        assert pipeline.source_id == "tcm_go"


class TestExtract:
    def test_extract_reads_csv(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        assert len(pipeline._municipalities) == 3
        assert len(pipeline._raw_fiscal) == 4

    def test_extract_with_limit(self) -> None:
        driver = MagicMock()
        p = TcmGoPipeline(driver=driver, data_dir=str(FIXTURES), limit=2)
        p.extract()
        assert len(p._raw_fiscal) == 2

    @patch("bracc_etl.pipelines.tcm_go.httpx.Client")
    def test_extract_empty_dir(
        self,
        mock_client_cls: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Redirect archival pra tmp_path pra não poluir o cwd com o arquivo
        # gravado pelo archive_fetch (extract online sempre arquiva).
        monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(tmp_path / "archival"))
        (tmp_path / "tcm_go").mkdir()
        # Mock API to return empty results
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"items": []}
        # ``content``/``headers`` existem pra archival poder rodar — payload
        # real é substituído pelo MockTransport em TestArchivalRetrofit; aqui
        # só queremos um fluxo sem explodir.
        mock_resp.content = b'{"items": []}'
        mock_resp.headers = {"content-type": "application/json"}
        mock_client.get.return_value = mock_resp
        driver = MagicMock()
        p = TcmGoPipeline(driver=driver, data_dir=str(tmp_path))
        p.extract()
        assert len(p._municipalities) == 0
        assert len(p._raw_fiscal) == 0

    def test_extract_filters_goias_only(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        for m in pipeline._municipalities:
            assert str(m["cod_ibge"]).startswith("52")


class TestTransform:
    def test_transform_produces_municipalities(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.municipalities) == 3

    def test_transform_municipality_fields(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for m in pipeline.municipalities:
            assert m["uf"] == "GO"
            assert m["source"] == "tcm_go"
            assert m["municipality_id"].startswith("52")
            assert m["name"]  # not empty

    def test_transform_separates_revenues_and_expenditures(
        self, pipeline: TcmGoPipeline
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.revenues) == 2
        assert len(pipeline.expenditures) == 2

    def test_transform_revenue_fields(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.revenues:
            assert "revenue_id" in r
            assert isinstance(r["amount"], float)
            assert r["amount"] > 0
            assert r["source"] == "tcm_go"
            assert r["municipality_id"].startswith("52")

    def test_transform_expenditure_fields(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        for e in pipeline.expenditures:
            assert "expenditure_id" in e
            assert isinstance(e["amount"], float)
            assert e["amount"] > 0
            assert e["source"] == "tcm_go"

    def test_transform_generates_unique_ids(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        rev_ids = {r["revenue_id"] for r in pipeline.revenues}
        exp_ids = {e["expenditure_id"] for e in pipeline.expenditures}
        assert len(rev_ids) == len(pipeline.revenues)
        assert len(exp_ids) == len(pipeline.expenditures)

    def test_transform_creates_rels(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.revenue_rels) == 2
        assert len(pipeline.expenditure_rels) == 2
        for rel in pipeline.revenue_rels:
            assert "source_key" in rel
            assert "target_key" in rel

    def test_transform_empty_input(self, pipeline: TcmGoPipeline) -> None:
        pipeline._municipalities = []
        pipeline._raw_fiscal = []
        pipeline.transform()
        assert len(pipeline.municipalities) == 0
        assert len(pipeline.revenues) == 0
        assert len(pipeline.expenditures) == 0

    def test_transform_skips_null_valor(self, pipeline: TcmGoPipeline) -> None:
        pipeline._municipalities = [
            {"cod_ibge": "5208707", "ente": "Goiania", "populacao": "1555626"}
        ]
        pipeline._raw_fiscal = [
            {
                "cod_ibge": "5208707",
                "exercicio": "2023",
                "conta": "Receita Corrente",
                "coluna": "Valor",
                "valor": None,
            }
        ]
        pipeline.transform()
        assert len(pipeline.revenues) == 0
        assert len(pipeline.expenditures) == 0

    def test_transform_skips_non_goias(self, pipeline: TcmGoPipeline) -> None:
        pipeline._municipalities = [
            {"cod_ibge": "3550308", "ente": "Sao Paulo", "populacao": "12345678"}
        ]
        pipeline._raw_fiscal = [
            {
                "cod_ibge": "3550308",
                "exercicio": "2023",
                "conta": "Receita Corrente",
                "coluna": "Valor",
                "valor": "1000000",
            }
        ]
        pipeline.transform()
        assert len(pipeline.municipalities) == 0
        assert len(pipeline.revenues) == 0

    def test_provenance_stamped_on_municipalities(
        self, pipeline: TcmGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert pipeline.municipalities
        for m in pipeline.municipalities:
            assert m["source_id"] == "tcm_go"
            # record_id is the natural IBGE code.
            assert m["source_record_id"] == m["municipality_id"]
            assert m["source_url"].startswith("http")
            assert m["ingested_at"].startswith("20")
            assert m["run_id"].startswith("tcm_go_")

    def test_provenance_stamped_on_revenues_and_rels(
        self, pipeline: TcmGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert pipeline.revenues
        for r in pipeline.revenues:
            assert r["source_id"] == "tcm_go"
            # Composite raw cod_ibge|exercicio|conta|coluna.
            assert r["source_record_id"].count("|") == 3
            assert r["source_url"].startswith("http")
        for rel in pipeline.revenue_rels:
            assert rel["source_id"] == "tcm_go"
            assert "|" in rel["source_record_id"]
            assert rel["run_id"].startswith("tcm_go_")

    def test_provenance_stamped_on_expenditures(
        self, pipeline: TcmGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert pipeline.expenditures
        for e in pipeline.expenditures:
            assert e["source_id"] == "tcm_go"
            assert e["source_record_id"].count("|") == 3
            assert e["source_url"].startswith("http")

    def test_is_revenue_classification(self) -> None:
        assert TcmGoPipeline._is_revenue("Receita Corrente Liquida") is True
        assert TcmGoPipeline._is_revenue("Receita Tributaria") is True
        assert TcmGoPipeline._is_revenue("Despesa Total com Pessoal") is False
        assert TcmGoPipeline._is_revenue("Despesa de Capital") is False


class TestLoad:
    def test_load_creates_nodes_and_rels(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.called

    def test_load_empty_data(self, pipeline: TcmGoPipeline) -> None:
        pipeline.municipalities = []
        pipeline.revenues = []
        pipeline.expenditures = []
        pipeline.revenue_rels = []
        pipeline.expenditure_rels = []
        pipeline.load()
        # No errors on empty data

    def test_load_calls_loader(self, pipeline: TcmGoPipeline) -> None:
        pipeline.municipalities = [
            pipeline.attach_provenance(
                {
                    "municipality_id": "5208707",
                    "name": "GOIANIA",
                    "uf": "GO",
                    "population": "1555626",
                    "source": "tcm_go",
                },
                record_id="5208707",
            )
        ]
        pipeline.revenues = [
            pipeline.attach_provenance(
                {
                    "revenue_id": "abc123",
                    "municipality_id": "5208707",
                    "year": "2023",
                    "account": "Receita Corrente Liquida",
                    "description": "Valor",
                    "amount": 8923456000.50,
                    "source": "tcm_go",
                },
                record_id="5208707|2023|Receita Corrente Liquida|Valor",
            )
        ]
        pipeline.expenditures = []
        pipeline.revenue_rels = [
            pipeline.attach_provenance(
                {"source_key": "5208707", "target_key": "abc123"},
                record_id="5208707|2023|Receita Corrente Liquida|Valor",
            ),
        ]
        pipeline.expenditure_rels = []
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.called

    def test_load_sets_rows_loaded(self, pipeline: TcmGoPipeline) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        assert pipeline.rows_loaded == len(pipeline.revenues) + len(pipeline.expenditures)


# ---------------------------------------------------------------------------
# Archival — snapshot do JSON bruto do SICONFI no momento do fetch
# (retrofit #6 do plano em high_priority/11-archival-retrofit-go.md, último
# pipeline GO legado a integrar com bracc_etl.archival).
#
# Estratégia: ``data_dir`` vazio -> ``extract`` cai no fallback HTTP pro
# SICONFI. Mockamos ``httpx.Client`` no módulo ``tcm_go`` com um
# ``MockTransport`` que emula dois endpoints: ``/entes`` (JSON com os entes
# de Goias) e ``/rreo`` (JSON com as linhas de receita/despesa por muni+ano).
# Conferimos:
#   * snapshot do payload de /entes gravado em ``BRACC_ARCHIVAL_ROOT/tcm_go/
#     YYYY-MM/*.json`` e carimbado em cada nó municipality;
#   * snapshot do payload de /rreo gravado idem e carimbado em cada
#     revenue/expenditure + rels via ``attach_provenance``;
#   * ``restore_snapshot`` devolve os bytes originais dos mocks (round-trip).
# O path offline (fixture CSV local) NÃO deve popular o campo — rodado em
# paralelo pra garantir que o retrofit continua opt-in.
# ---------------------------------------------------------------------------


_MOCK_ENTES_PAYLOAD: dict[str, Any] = {
    "items": [
        {"cod_ibge": "5208707", "ente": "Goiania", "populacao": "1555626"},
        {"cod_ibge": "5201108", "ente": "Anapolis", "populacao": "391772"},
    ],
}

# /rreo payload replicates real SICONFI shape: ~summary accounts with the
# ``Até o bimestre (c)`` column for revenue, ``Despesas liquidadas até o
# bimestre (h)`` for expenditure. Archival é content-addressed, então duas
# chamadas idênticas deduplicam — variamos valor por muni pra ficar realista.
_MOCK_RREO_BY_MUNI: dict[str, dict[str, Any]] = {
    "5208707": {
        "items": [
            {
                "cod_ibge": "5208707",
                "conta": "RECEITA CORRENTE LIQUIDA",
                "coluna": "Até o bimestre (c)",
                "valor": "1000000.00",
            },
            {
                "cod_ibge": "5208707",
                "conta": "DESPESAS CORRENTES",
                "coluna": "Despesas liquidadas até o bimestre (h)",
                "valor": "900000.00",
            },
        ],
    },
    "5201108": {
        "items": [
            {
                "cod_ibge": "5201108",
                "conta": "RECEITA TRIBUTÁRIA",
                "coluna": "Até o bimestre (c)",
                "valor": "400000.00",
            },
            {
                "cod_ibge": "5201108",
                "conta": "DESPESA TOTAL COM PESSOAL",
                "coluna": "Despesas liquidadas até o bimestre (h)",
                "valor": "350000.00",
            },
        ],
    },
}


def _tcm_go_handler() -> httpx.MockTransport:
    """MockTransport que emula apidatalake.tesouro.gov.br/siconfi."""

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {"content-type": "application/json; charset=utf-8"}
        path = request.url.path
        if path.endswith("/entes"):
            return httpx.Response(
                200,
                content=json.dumps(_MOCK_ENTES_PAYLOAD).encode("utf-8"),
                headers=headers,
            )
        if path.endswith("/rreo"):
            cod_ibge = request.url.params.get("id_ente", "")
            payload = _MOCK_RREO_BY_MUNI.get(cod_ibge, {"items": []})
            return httpx.Response(
                200,
                content=json.dumps(payload).encode("utf-8"),
                headers=headers,
            )
        return httpx.Response(
            404,
            content=b'{"error": "unhandled"}',
            headers=headers,
        )

    return httpx.MockTransport(handler)


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


@pytest.fixture()
def online_pipeline(
    tmp_path: Path,
    archival_root: Path,  # noqa: ARG001 — apenas ativa o env var
    monkeypatch: pytest.MonkeyPatch,
) -> TcmGoPipeline:
    """Pipeline com data_dir vazio (força fallback SICONFI) + transport mockado."""
    empty_data = tmp_path / "data_empty"
    (empty_data / "tcm_go").mkdir(parents=True)

    transport = _tcm_go_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.tcm_go.httpx.Client",
        _client_factory,
    )
    # Neutraliza o sleep de rate-limit pra não arrastar a suíte.
    monkeypatch.setattr(
        "bracc_etl.pipelines.tcm_go.time.sleep",
        lambda _s: None,
    )
    pipeline = TcmGoPipeline(driver=MagicMock(), data_dir=str(empty_data))
    # run_id canônico → bucket 2024-03 no archival.
    pipeline.run_id = "tcm_go_20240315120000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: tcm_go agora grava snapshots dos payloads /entes e /rreo."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: TcmGoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # /entes: uma única URI compartilhada por todos os municipality nodes.
        assert online_pipeline._entes_snapshot_uri is not None
        entes_uri = online_pipeline._entes_snapshot_uri
        parts = entes_uri.split("/")
        assert parts[0] == "tcm_go"
        assert parts[1] == "2024-03"
        assert parts[2].endswith(".json")

        # municipalities: todos carimbados com a URI do /entes.
        assert online_pipeline.municipalities
        for m in online_pipeline.municipalities:
            assert m["source_snapshot_uri"] == entes_uri

        # revenues/expenditures + rels: URI do /rreo daquele (muni, ano).
        # RREO_YEARS = 2021..2024 → 4 chamadas por muni, mas mock devolve
        # o mesmo payload pra cada ano, então archival deduplica e apenas
        # uma URI existe por muni (content-addressing).
        assert online_pipeline.revenues
        for r in online_pipeline.revenues:
            uri = r.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            assert uri.startswith("tcm_go/2024-03/")
            assert uri.endswith(".json")
            # URI do /rreo tem que ser diferente da URI do /entes
            # (payloads distintos → hashes distintos).
            assert uri != entes_uri

        assert online_pipeline.expenditures
        for e in online_pipeline.expenditures:
            uri = e.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            assert uri.startswith("tcm_go/2024-03/")

        # rels também herdam a URI correspondente à row fiscal.
        assert online_pipeline.revenue_rels
        for rel in online_pipeline.revenue_rels:
            assert rel.get("source_snapshot_uri", "").startswith(
                "tcm_go/2024-03/",
            )
        assert online_pipeline.expenditure_rels
        for rel in online_pipeline.expenditure_rels:
            assert rel.get("source_snapshot_uri", "").startswith(
                "tcm_go/2024-03/",
            )

        # Storage: arquivos fisicamente presentes sob o root configurado.
        assert (archival_root / entes_uri).exists()
        rreo_uris = {
            r["source_snapshot_uri"] for r in online_pipeline.revenues
        }
        for uri in rreo_uris:
            assert (archival_root / uri).exists(), f"snapshot ausente: {uri}"

        # Round-trip: restore_snapshot devolve os bytes originais dos mocks.
        restored_entes = restore_snapshot(entes_uri)
        assert b"Goiania" in restored_entes
        any_rreo = next(iter(rreo_uris))
        restored_rreo = restore_snapshot(any_rreo)
        assert b"RECEITA" in restored_rreo or b"DESPESA" in restored_rreo

    def test_offline_path_nao_popula_snapshot_uri(
        self, pipeline: TcmGoPipeline,
    ) -> None:
        """Fixture local (sem HTTP) mantém o campo ausente — opt-in preservado."""
        pipeline.extract()
        pipeline.transform()

        # URI do /entes fica ``None`` — CSV local não hit-a a API.
        assert pipeline._entes_snapshot_uri is None

        # E nenhuma row ganha ``source_snapshot_uri`` (attach_provenance
        # só injeta a chave quando ``snapshot_uri`` não é ``None``).
        assert pipeline.municipalities
        for m in pipeline.municipalities:
            assert "source_snapshot_uri" not in m
        assert pipeline.revenues
        for r in pipeline.revenues:
            assert "source_snapshot_uri" not in r
        assert pipeline.expenditures
        for e in pipeline.expenditures:
            assert "source_snapshot_uri" not in e
        for rel in pipeline.revenue_rels:
            assert "source_snapshot_uri" not in rel
        for rel in pipeline.expenditure_rels:
            assert "source_snapshot_uri" not in rel

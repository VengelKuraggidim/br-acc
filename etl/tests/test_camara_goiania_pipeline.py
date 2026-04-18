from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.camara_goiania import CamaraGoianiaPipeline

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CamaraGoianiaPipeline:
    return CamaraGoianiaPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert CamaraGoianiaPipeline.name == "camara_goiania"

    def test_source_id(self) -> None:
        assert CamaraGoianiaPipeline.source_id == "camara_goiania"


class TestTransform:
    def test_extract_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()

        assert len(pipeline._raw_vereadores) == 2
        assert len(pipeline._raw_expenses) == 2
        assert len(pipeline._raw_proposicoes) == 2

    def test_transform_vereadores(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.vereadores) == 2
        names = {v["name"] for v in pipeline.vereadores}
        assert "JOAO DA SILVA" in names
        assert "MARIA OLIVEIRA" in names

        for v in pipeline.vereadores:
            assert v["uf"] == "GO"
            assert v["municipality"] == "Goiania"
            assert v["municipality_code"] == "5208707"
            assert v["source"] == "camara_goiania"
            assert v["vereador_id"]

    def test_transform_expenses(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.expenses) == 2
        amounts = {e["vereador_name"]: e["amount"] for e in pipeline.expenses}
        assert amounts["JOAO DA SILVA"] == 1250.0
        assert amounts["MARIA OLIVEIRA"] == 800.5

    def test_transform_proposals(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.proposals) == 2
        types = {p["number"]: p["type"] for p in pipeline.proposals}
        assert types["1234"] == "PL"
        assert types["1235"] == "Resolucao"

    def test_autor_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.autor_rels) == 2

    def test_despesa_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.despesa_rels) == 2

    def test_provenance_stamped_on_nodes(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for group in (pipeline.vereadores, pipeline.expenses, pipeline.proposals):
            for item in group:
                assert item["source_id"] == "camara_goiania"
                assert item["source_record_id"]
                assert item["source_url"].startswith("http")
                assert item["ingested_at"].startswith("20")
                assert item["run_id"].startswith("camara_goiania_")

    def test_provenance_stamped_on_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for rel in (*pipeline.autor_rels, *pipeline.despesa_rels):
            assert rel["source_id"] == "camara_goiania"
            assert rel["source_record_id"]
            assert rel["source_url"].startswith("http")
            assert rel["run_id"].startswith("camara_goiania_")
            assert rel["source_key"]
            assert rel["target_key"]

    def test_stable_ids_are_deterministic(self) -> None:
        p1 = _make_pipeline()
        p1.extract()
        p1.transform()

        p2 = _make_pipeline()
        p2.extract()
        p2.transform()

        ids1 = {v["vereador_id"] for v in p1.vereadores}
        ids2 = {v["vereador_id"] for v in p2.vereadores}
        assert ids1 == ids2


class TestLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

    def test_load_empty_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.load()


# ---------------------------------------------------------------------------
# Archival — snapshot dos payloads JSON do portal da Camara Municipal de
# Goiania no momento do fetch (retrofit #8 do plano em
# todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estrategia: ``data_dir`` vazio forca o fallback online em ``extract`` ->
# ``_fetch_json`` -> ``archive_fetch`` pra cada um dos 3 endpoints Plone
# (``@@portalmodelo-json`` / ``@@transparency-json`` / ``@@pl-json``).
# Mockamos ``httpx.Client`` no modulo camara_goiania via ``MockTransport``
# pra servir bytes deterministicos e verificamos que:
#   * arquivos de snapshot existem sob ``BRACC_ARCHIVAL_ROOT``;
#   * todas as rows (vereadores / expenses / proposals) e rels (autor_rels
#     / despesa_rels) ganharam ``source_snapshot_uri``;
#   * ``restore_snapshot`` devolve os bytes originais (round-trip);
#   * URIs diferem entre as 3 familias de row (3 endpoints = 3 hashes
#     distintos porque o payload e diferente).
# O caminho offline (fixtures JSON) NAO popula o campo — contrato opt-in
# preservado; confirmado no teste complementar.
# ---------------------------------------------------------------------------


_VEREADORES_PAYLOAD: list[dict[str, Any]] = [
    {"nome": "DEPUTADA MOCKADA", "partido": "PARTIDO X", "legislatura": "2025-2028"},
]
_TRANSPARENCY_PAYLOAD: list[dict[str, Any]] = [
    {
        "vereador": "DEPUTADA MOCKADA",
        "tipo": "MATERIAL DE ESCRITORIO",
        "descricao": "CANETAS",
        "valor": "125,50",
        "data": "2025-02-10",
        "ano": "2025",
    },
]
_PROPOSICOES_PAYLOAD: list[dict[str, Any]] = [
    {
        "numero": "42",
        "ano": "2025",
        "tipo": "PL",
        "ementa": "DISPOE SOBRE ALGO IMPORTANTE",
        "autor": "DEPUTADA MOCKADA",
        "situacao": "EM TRAMITACAO",
        "data": "2025-03-01",
    },
]


def _camara_handler() -> httpx.MockTransport:
    """MockTransport que emula www.goiania.go.leg.br (3 endpoints Plone)."""

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {"content-type": "application/json; charset=utf-8"}
        path = request.url.path
        if path.endswith("@@portalmodelo-json"):
            body: Any = _VEREADORES_PAYLOAD
        elif path.endswith("@@transparency-json"):
            body = _TRANSPARENCY_PAYLOAD
        elif path.endswith("@@pl-json"):
            body = _PROPOSICOES_PAYLOAD
        else:
            return httpx.Response(
                404,
                content=b'{"error": "unhandled"}',
                headers=headers,
            )
        return httpx.Response(
            200,
            content=json.dumps(body).encode("utf-8"),
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
    archival_root: Path,  # noqa: ARG001 — just activates the env var
    monkeypatch: pytest.MonkeyPatch,
) -> CamaraGoianiaPipeline:
    """Pipeline com data_dir vazio (forca API fallback) + HTTP mockado."""
    empty_data = tmp_path / "data_empty"
    (empty_data / "camara_goiania").mkdir(parents=True)

    transport = _camara_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.camara_goiania.httpx.Client",
        _client_factory,
    )
    pipeline = CamaraGoianiaPipeline(driver=MagicMock(), data_dir=str(empty_data))
    # run_id canonico pra conferir o bucket mensal (``YYYY-MM``) no path.
    pipeline.run_id = "camara_goiania_20250315120000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: camara_goiania agora grava snapshots dos payloads Plone."""

    def test_carimba_source_snapshot_uri_em_rows_e_rels(
        self,
        online_pipeline: CamaraGoianiaPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Rows: todas as 3 familias ganharam URI.
        assert online_pipeline.vereadores
        assert online_pipeline.expenses
        assert online_pipeline.proposals
        for group in (
            online_pipeline.vereadores,
            online_pipeline.expenses,
            online_pipeline.proposals,
        ):
            for item in group:
                uri = item.get("source_snapshot_uri")
                assert isinstance(uri, str) and uri
                parts = uri.split("/")
                assert parts[0] == "camara_goiania"
                assert parts[1] == "2025-03"
                assert parts[2].endswith(".json")

        # Rels: autor_rels e despesa_rels carregam URI do row que originou.
        assert online_pipeline.autor_rels
        assert online_pipeline.despesa_rels
        for rel in (*online_pipeline.autor_rels, *online_pipeline.despesa_rels):
            assert rel.get("source_snapshot_uri")

        # 3 endpoints com payload distinto → 3 URIs distintas (archival
        # e content-addressed: payloads iguais cairiam na mesma URI).
        ver_uri = online_pipeline.vereadores[0]["source_snapshot_uri"]
        exp_uri = online_pipeline.expenses[0]["source_snapshot_uri"]
        prop_uri = online_pipeline.proposals[0]["source_snapshot_uri"]
        assert ver_uri != exp_uri
        assert ver_uri != prop_uri
        assert exp_uri != prop_uri

        # despesa_rel carimba URI do expense (fonte do par); autor_rel
        # carimba URI da proposta — nao da listagem de vereadores.
        assert online_pipeline.despesa_rels[0]["source_snapshot_uri"] == exp_uri
        assert online_pipeline.autor_rels[0]["source_snapshot_uri"] == prop_uri

        # Storage: arquivos fisicamente presentes sob o root configurado.
        for uri in (ver_uri, exp_uri, prop_uri):
            absolute = archival_root / uri
            assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored_exp = restore_snapshot(exp_uri)
        assert b"MATERIAL DE ESCRITORIO" in restored_exp
        restored_prop = restore_snapshot(prop_uri)
        assert b"DISPOE SOBRE ALGO IMPORTANTE" in restored_prop

        # A chave privada de propagacao NAO deve vazar pro loader — ela
        # vive nos raw records, mas attach_provenance constroi dicts novos
        # sem copiar chaves ``__*``. Conferir explicitamente.
        for item in (
            *online_pipeline.vereadores,
            *online_pipeline.expenses,
            *online_pipeline.proposals,
        ):
            assert "__snapshot_uri" not in item

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Fixture local (sem HTTP) mantem o campo ausente — opt-in."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.vereadores
        assert pipeline.expenses
        assert pipeline.proposals
        # Ausencia do campo == opt-in nao ativado (contrato do
        # attach_provenance: so injeta a chave quando snapshot_uri nao
        # e None).
        for group in (pipeline.vereadores, pipeline.expenses, pipeline.proposals):
            for item in group:
                assert "source_snapshot_uri" not in item
        for rel in (*pipeline.autor_rels, *pipeline.despesa_rels):
            assert "source_snapshot_uri" not in rel

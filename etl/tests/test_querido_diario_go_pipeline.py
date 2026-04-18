from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.querido_diario_go import (  # type: ignore[attr-defined]
    QueridoDiarioGoPipeline,
    _classify_act,
    _extract_appointments,
    _extract_cnpjs,
    _stable_id,
)
from tests._mock_helpers import mock_driver

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> QueridoDiarioGoPipeline:
    # ``archive=False``: fixtures offline não têm MockTransport pro fetch
    # dos PDFs, então desativamos archival pra não hit network. O caminho
    # online (``archive=True`` + ``MockTransport``) é coberto em
    # ``TestArchivalRetrofit`` abaixo.
    return QueridoDiarioGoPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
        archive=False,
    )


class TestMetadata:
    def test_name(self) -> None:
        assert QueridoDiarioGoPipeline.name == "querido_diario_go"

    def test_source_id(self) -> None:
        assert QueridoDiarioGoPipeline.source_id == "querido_diario_go"


class TestHelpers:
    def test_stable_id_deterministic(self) -> None:
        a = _stable_id("a", "b", "c")
        b = _stable_id("a", "b", "c")
        assert a == b
        assert len(a) == 24

    def test_stable_id_different_inputs(self) -> None:
        a = _stable_id("a", "b")
        b = _stable_id("x", "y")
        assert a != b

    def test_classify_act_nomeacao(self) -> None:
        assert _classify_act("resolve nomear FULANO") == "nomeacao"

    def test_classify_act_exoneracao(self) -> None:
        assert _classify_act("resolve exonerar FULANO") == "exoneracao"

    def test_classify_act_contrato(self) -> None:
        assert _classify_act("extrato de contrato celebrado") == "contrato"

    def test_classify_act_outro(self) -> None:
        assert _classify_act("publicação genérica sem palavras-chave") == "outro"

    def test_extract_cnpjs(self) -> None:
        text = "Empresa 12.345.678/0001-95 contratada."
        results = _extract_cnpjs(text)
        assert len(results) == 1
        assert results[0][0] == "12.345.678/0001-95"

    def test_extract_cnpjs_dedup(self) -> None:
        text = "CNPJ 12.345.678/0001-95 e novamente 12.345.678/0001-95."
        results = _extract_cnpjs(text)
        assert len(results) == 1

    def test_extract_appointments(self) -> None:
        text = "nomear MARIA DA SILVA SANTOS para o cargo de Diretora do Departamento."
        results = _extract_appointments(text)
        assert len(results) == 1
        assert "MARIA" in results[0]["person_name"].upper()
        assert "Diretora" in results[0]["role"]

class TestTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.acts) == 2

    def test_act_fields(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        act = pipeline.acts[0]
        assert act["uf"] == "GO"
        assert act["source"] == "querido_diario_go"
        assert "act_id" in act
        assert "territory_id" in act
        assert "act_type" in act
        assert "excerpt" in act

    def test_extracts_cnpj_mentions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.company_mentions) >= 1
        cnpjs = [m["cnpj"] for m in pipeline.company_mentions]
        assert "12.345.678/0001-95" in cnpjs

    def test_extracts_appointments(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.appointments) >= 1
        appt = pipeline.appointments[0]
        assert appt["uf"] == "GO"
        assert appt["appointment_type"] in ("nomeacao", "exoneracao")
        assert "person_name" in appt
        assert "role" in appt
        assert "act_id" in appt

    def test_act_types_classified(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        types = {a["act_id"]: a["act_type"] for a in pipeline.acts}
        type_values = list(types.values())
        # First fixture has "nomear" -> nomeacao (or contrato since both match)
        assert any(t in ("nomeacao", "contrato") for t in type_values)

    def test_excerpt_max_length(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for act in pipeline.acts:
            assert len(act["excerpt"]) <= 500

    def test_provenance_stamped_on_acts_and_appointments(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for group in (pipeline.acts, pipeline.appointments):
            for item in group:
                assert item["source_id"] == "querido_diario_go"
                assert item["source_record_id"]
                assert item["source_url"].startswith("http")
                assert item["run_id"].startswith("querido_diario_go_")

    def test_provenance_stamped_on_company_mentions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for m in pipeline.company_mentions:
            assert m["source_id"] == "querido_diario_go"
            assert m["source_record_id"]
            assert m["source_url"].startswith("http")
            assert m["source_key"] == m["cnpj"]
            assert m["target_key"]


class TestLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

    def test_load_calls_driver(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

        assert mock_driver(pipeline).session.called


# ---------------------------------------------------------------------------
# Archival — snapshot dos PDFs dos diários no momento do fetch (retrofit #9
# do plano em todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estratégia: fixture (``gazettes.json`` com 2 edições, Goiânia 2026-03-10
# e Anápolis 2026-03-12) fornece as rows; mockamos ``httpx.Client`` no
# módulo ``querido_diario_go`` com um ``MockTransport`` que devolve um PDF
# fake por URL de diário. Daí:
#  * snapshot gravado em ``BRACC_ARCHIVAL_ROOT/querido_diario_go/YYYY-MM/*.pdf``
#    (um por edição, content-addressed);
#  * todas as rows (acts, mentions, appointments) ganham ``source_snapshot_uri``
#    pela chave natural ``territory_id|date|edition``;
#  * ``restore_snapshot`` devolve os bytes originais do PDF (round-trip).
# O path offline (``archive=False``) NÃO deve popular o campo — rodado em
# paralelo pra garantir que o retrofit continua opt-in.
# ---------------------------------------------------------------------------


_FAKE_PDF_GOIANIA = (
    b"%PDF-1.4\n%qd_go fake bulletin goiania 2026-03-10\n%%EOF"
)
_FAKE_PDF_ANAPOLIS = (
    b"%PDF-1.4\n%qd_go fake bulletin anapolis 2026-03-12\n%%EOF"
)
_GAZETTE_URL_GOIANIA = (
    "https://queridodiario.ok.org.br/api/gazettes/5208707/2026-03-10"
)
_GAZETTE_URL_ANAPOLIS = (
    "https://queridodiario.ok.org.br/api/gazettes/5201108/2026-03-12"
)


def _qd_handler() -> httpx.MockTransport:
    """MockTransport que devolve um PDF fake por URL de diário."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == _GAZETTE_URL_GOIANIA:
            return httpx.Response(
                200,
                content=_FAKE_PDF_GOIANIA,
                headers={"content-type": "application/pdf"},
            )
        if url == _GAZETTE_URL_ANAPOLIS:
            return httpx.Response(
                200,
                content=_FAKE_PDF_ANAPOLIS,
                headers={"content-type": "application/pdf"},
            )
        return httpx.Response(
            404,
            content=b"not found",
            headers={"content-type": "text/plain"},
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
    archival_root: Path,  # noqa: ARG001 — just activates the env var
    monkeypatch: pytest.MonkeyPatch,
) -> QueridoDiarioGoPipeline:
    """Pipeline com HTTP mockado, ``archive=True`` e fixtures locais.

    ``data_dir`` reusa ``tests/fixtures/querido_diario_go/gazettes.json``
    (Goiânia 2026-03-10 + Anápolis 2026-03-12) pra transform produzir
    rows, enquanto o mock devolve um PDF fake por URL de diário pra
    popular o mapa de snapshot URIs.
    """
    transport = _qd_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.querido_diario_go.httpx.Client",
        _client_factory,
    )
    pipeline = QueridoDiarioGoPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
        archive=True,
    )
    # run_id canônico (``{source}_YYYYMMDDHHMMSS``) cai no bucket 2026-03,
    # só pra facilitar conferência visual do path no assert.
    pipeline.run_id = "querido_diario_go_20260310000000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: querido_diario_go agora grava snapshots dos PDFs dos diários."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: QueridoDiarioGoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Ambas as edições do fixture têm PDF fake no mock — logo todas
        # as rows geradas (acts, mentions, appointments) ganham
        # ``source_snapshot_uri``.
        assert online_pipeline.acts
        for act in online_pipeline.acts:
            uri = act.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            # Shape: ``querido_diario_go/YYYY-MM/hash12.pdf``
            parts = uri.split("/")
            assert parts[0] == "querido_diario_go"
            assert parts[1] == "2026-03"
            assert parts[2].endswith(".pdf")

        # Granularidade por diário: os 2 PDFs fake geram URIs distintas.
        uris = {act["source_snapshot_uri"] for act in online_pipeline.acts}
        assert len(uris) == 2

        # Mentions e appointments extraídos do mesmo diário herdam a URI.
        for m in online_pipeline.company_mentions:
            assert isinstance(m.get("source_snapshot_uri"), str)
        for appt in online_pipeline.appointments:
            assert isinstance(appt.get("source_snapshot_uri"), str)

        # Storage: arquivo fisicamente presente sob o root configurado.
        sample_uri = online_pipeline.acts[0]["source_snapshot_uri"]
        absolute = archival_root / sample_uri
        assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored = restore_snapshot(sample_uri)
        assert restored in (_FAKE_PDF_GOIANIA, _FAKE_PDF_ANAPOLIS)

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Pipeline com ``archive=False`` deixa o campo fora (opt-in)."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.acts
        for act in pipeline.acts:
            # Ausência do campo == opt-in não ativado (contrato do
            # ``attach_provenance``: só injeta a chave quando snapshot_uri
            # não é None).
            assert "source_snapshot_uri" not in act
        for m in pipeline.company_mentions:
            assert "source_snapshot_uri" not in m
        for appt in pipeline.appointments:
            assert "source_snapshot_uri" not in appt

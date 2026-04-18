from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.folha_go import (
    FolhaGoPipeline,
    _is_commissioned,
)
from bracc_etl.transforms import mask_cpf

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> FolhaGoPipeline:
    return FolhaGoPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMetadata:
    def test_name(self) -> None:
        assert FolhaGoPipeline.name == "folha_go"

    def test_source_id(self) -> None:
        assert FolhaGoPipeline.source_id == "folha_go"


class TestHelpers:
    def test_mask_cpf_valid(self) -> None:
        result = mask_cpf("12345678901")
        assert result == "***.***.*89-01"
        # Only last 4 digits visible
        assert "1234" not in result

    def test_mask_cpf_invalid(self) -> None:
        assert mask_cpf("123") == "***.***.***-**"
        assert mask_cpf("") == "***.***.***-**"

    def test_is_commissioned_das(self) -> None:
        assert _is_commissioned("ASSESSOR DAS-3 COMISSIONADO") is True

    def test_is_commissioned_regular(self) -> None:
        assert _is_commissioned("ANALISTA DE SISTEMAS") is False

    def test_is_commissioned_cc(self) -> None:
        assert _is_commissioned("CC-2 COORDENADOR") is True

    def test_is_commissioned_cds(self) -> None:
        assert _is_commissioned("DIRETOR CDS-4") is True

    def test_is_commissioned_dai(self) -> None:
        assert _is_commissioned("CHEFE DAI-1") is True

    def test_is_commissioned_fcpe(self) -> None:
        assert _is_commissioned("ASSESSOR FCPE 101.4") is True


class TestTransform:
    def test_transform_employee_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.employees) == 3

    def test_transform_agency_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 3 employees across 3 distinct agencies
        assert len(pipeline.agencies) == 3

    def test_commissioned_flag(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        commissioned = [e for e in pipeline.employees if e["is_commissioned"]]
        regular = [e for e in pipeline.employees if not e["is_commissioned"]]
        assert len(commissioned) == 1
        assert len(regular) == 2

    def test_cpf_masked(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for emp in pipeline.employees:
            # CPF must not contain full digits
            assert "12345678901" not in emp["cpf"]
            assert "98765432100" not in emp["cpf"]
            assert "***" in emp["cpf"]

    def test_uf_always_go(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for emp in pipeline.employees:
            assert emp["uf"] == "GO"

    def test_employee_agency_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.employee_agency_rels) == 3

    def test_provenance_stamped_on_employees(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for emp in pipeline.employees:
            assert emp["source_id"] == "folha_go"
            assert emp["source_record_id"]
            assert emp["source_url"].startswith("http")
            assert emp["ingested_at"].startswith("20")
            assert emp["run_id"].startswith("folha_go_")

    def test_provenance_stamped_on_agencies(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for agency in pipeline.agencies:
            assert agency["source_id"] == "folha_go"
            assert agency["source_record_id"] == agency["name"]
            assert agency["source_url"].startswith("http")

    def test_provenance_stamped_on_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        for rel in pipeline.employee_agency_rels:
            assert rel["source_id"] == "folha_go"
            assert rel["source_record_id"]
            assert rel["source_url"].startswith("http")
            assert rel["run_id"].startswith("folha_go_")

    def test_salary_values(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        gross_values = sorted([e["salary_gross"] for e in pipeline.employees])
        assert 4500.0 in gross_values
        assert 8500.0 in gross_values
        assert 12000.0 in gross_values

    def test_stable_ids_are_deterministic(self) -> None:
        p1 = _make_pipeline()
        p1.extract()
        p1.transform()

        p2 = _make_pipeline()
        p2.extract()
        p2.transform()

        ids1 = sorted([e["employee_id"] for e in p1.employees])
        ids2 = sorted([e["employee_id"] for e in p2.employees])
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
# Archival — snapshot do payload CKAN no momento do fetch (retrofit #1 do
# plano em todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estratégia: ``data_dir`` vazio -> extract cai no fallback CKAN; mockamos
# ``httpx.Client`` no módulo ``folha_go`` com um ``MockTransport`` que
# devolve bytes determinísticos, daí conferimos:
#  * snapshot file gravado em ``BRACC_ARCHIVAL_ROOT/folha_go/YYYY-MM/*.json``;
#  * todas as rows transformadas têm ``source_snapshot_uri`` populado;
#  * ``restore_snapshot`` devolve os bytes originais (round-trip).
# O path offline (fixtures locais) NÃO deve popular o campo — rodado em
# paralelo pra garantir que o retrofit continua opt-in.
# ---------------------------------------------------------------------------


_CKAN_RESOURCE_ID = "deadbeef-1111-2222-3333-444455556666"
_CKAN_PACKAGE_RESPONSE: dict[str, Any] = {
    "result": {
        "resources": [
            {
                "id": _CKAN_RESOURCE_ID,
                "datastore_active": True,
                "format": "CSV",
                "name": "Folha de Pagamento - Marco/2024",
            },
        ],
    },
}
_CKAN_DATASTORE_RECORDS: list[dict[str, Any]] = [
    {
        "nomeServidor": "ANA CARVALHO",
        "cpf": "11122233344",
        "nomeCargo": "ANALISTA TRIBUTARIO",
        "orgao": "SECRETARIA DA ECONOMIA",
        "valorProvento": "9000.00",
        "valorLiquido": "7500.00",
        "municipio": "GOIANIA",
    },
    {
        "nomeServidor": "BRUNO MELO",
        "cpf": "55566677788",
        "nomeCargo": "AUDITOR DAS-3 COMISSIONADO",
        "orgao": "SECRETARIA DA ECONOMIA",
        "valorProvento": "14000.00",
        "valorLiquido": "10500.00",
        "municipio": "GOIANIA",
    },
]


def _ckan_handler() -> httpx.MockTransport:
    """MockTransport que emula o CKAN de dadosabertos.go.gov.br."""

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {"content-type": "application/json; charset=utf-8"}
        path = request.url.path
        if path.endswith("/package_show"):
            return httpx.Response(
                200,
                content=json.dumps(_CKAN_PACKAGE_RESPONSE).encode("utf-8"),
                headers=headers,
            )
        if path.endswith("/datastore_search"):
            offset = int(request.url.params.get("offset", "0"))
            # Uma página só — segunda chamada retorna vazio pra fechar o loop.
            if offset == 0:
                body = {
                    "result": {"records": _CKAN_DATASTORE_RECORDS},
                }
            else:
                body = {"result": {"records": []}}
            return httpx.Response(
                200,
                content=json.dumps(body).encode("utf-8"),
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
    archival_root: Path,  # noqa: ARG001 — just activates the env var
    monkeypatch: pytest.MonkeyPatch,
) -> FolhaGoPipeline:
    """Pipeline com data_dir vazio (força CKAN fallback) + HTTP mockado."""
    empty_data = tmp_path / "data_empty"
    (empty_data / "folha_go").mkdir(parents=True)

    transport = _ckan_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.folha_go.httpx.Client",
        _client_factory,
    )
    # run_id canônico (``{source}_YYYYMMDDHHMMSS``) cai no bucket 2024-03,
    # alinhando com o nome do CKAN resource pro teste ficar mais óbvio.
    pipeline = FolhaGoPipeline(driver=MagicMock(), data_dir=str(empty_data))
    pipeline.run_id = "folha_go_20240315120000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: folha_go agora grava snapshots dos payloads CKAN."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: FolhaGoPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Proveniência: todas as rows ganham source_snapshot_uri.
        assert online_pipeline.employees
        for emp in online_pipeline.employees:
            uri = emp.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            # Shape: ``folha_go/YYYY-MM/hash12.ext``
            parts = uri.split("/")
            assert parts[0] == "folha_go"
            assert parts[1] == "2024-03"
            assert parts[2].endswith(".json")

        for agency in online_pipeline.agencies:
            assert agency.get("source_snapshot_uri")
        for rel in online_pipeline.employee_agency_rels:
            assert rel.get("source_snapshot_uri")

        # Storage: arquivo fisicamente presente sob o root configurado.
        sample_uri = online_pipeline.employees[0]["source_snapshot_uri"]
        absolute = archival_root / sample_uri
        assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored = restore_snapshot(sample_uri)
        assert b'"records"' in restored
        assert b"ANA CARVALHO" in restored

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Fixture local (sem HTTP) mantém o campo ``None`` — opt-in preservado."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.employees
        for emp in pipeline.employees:
            # Ausência do campo == opt-in não ativado (contrato do
            # attach_provenance: só injeta a chave quando snapshot_uri
            # não é None).
            assert "source_snapshot_uri" not in emp

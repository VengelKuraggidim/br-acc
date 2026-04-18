"""Tests for ``brasilapi_cnpj_status`` pipeline.

Cobre:

* happy path — 3 CNPJs mockados (ATIVA, BAIXADA, timeout) virando SET no
  grafo com proveniência + archival;
* archival — cada resposta gera snapshot content-addressed sob o root;
* cache TTL — CNPJ com ``situacao_verified_at`` recente não volta ao
  batch (verificado via query parameters do driver mockado);
* opt-in ``archive_online=False`` — não popula ``source_snapshot_uri``;
* failure silencioso — 404 + timeout não derrubam o run, só pulam o CNPJ.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.brasilapi_cnpj_status import (
    _SOURCE_ID,
    BrasilapiCnpjStatusPipeline,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures / factories
# ---------------------------------------------------------------------------


_CNPJ_ATIVA = "12345678000190"
_CNPJ_BAIXADA = "22333444000155"
_CNPJ_TIMEOUT = "99887766000100"
_CNPJ_CACHED_RECENT = "11122233000144"  # Nunca deve ser consultado.


def _payload_ativa() -> dict[str, Any]:
    return {
        "cnpj": _CNPJ_ATIVA,
        "razao_social": "EMPRESA ATIVA LTDA",
        "situacao_cadastral": "ATIVA",
        "data_situacao_cadastral": "2010-01-15",
        "cnae_fiscal": 4711301,
        "cnae_fiscal_descricao": "Comercio varejista de mercadorias",
        "porte": "MEDIO",
        "capital_social": 100_000.00,
        "municipio": "GOIANIA",
        "uf": "GO",
        "data_inicio_atividade": "2010-01-10",
    }


def _payload_baixada() -> dict[str, Any]:
    return {
        "cnpj": _CNPJ_BAIXADA,
        "razao_social": "EMPRESA BAIXADA LTDA",
        "situacao_cadastral": "BAIXADA",
        "data_situacao_cadastral": "2023-05-20",
        "cnae_fiscal": 4399103,
        "cnae_fiscal_descricao": "Obras de alvenaria",
        "porte": "MICRO",
        "capital_social": 10_000.00,
        "municipio": "ANAPOLIS",
        "uf": "GO",
        "data_inicio_atividade": "2015-06-01",
    }


def _build_transport(
    responses: dict[str, tuple[int, dict[str, Any] | None] | str],
) -> httpx.MockTransport:
    """MockTransport pra BrasilAPI. ``responses`` mapeia CNPJ → (status, body)
    ou ``"timeout"`` pra simular erro de rede."""

    def handler(request: httpx.Request) -> httpx.Response:
        # URL shape: /api/cnpj/v1/{cnpj}
        cnpj = request.url.path.rsplit("/", 1)[-1]
        spec = responses.get(cnpj)
        headers = {"content-type": "application/json; charset=utf-8"}
        if isinstance(spec, str):
            if spec == "timeout":
                raise httpx.ConnectTimeout("mock timeout")
            raise AssertionError(f"unexpected spec string: {spec!r}")
        if spec is None:
            return httpx.Response(
                404,
                content=json.dumps({"message": "CNPJ não encontrado"}).encode(),
                headers=headers,
            )
        status, body = spec
        return httpx.Response(
            status,
            content=json.dumps(body or {}).encode("utf-8"),
            headers=headers,
        )

    return httpx.MockTransport(handler)


def _build_driver(
    targets: list[dict[str, Any]],
) -> tuple[MagicMock, list[tuple[str, dict[str, Any]]]]:
    """Driver mock: ``session.run(query, params)`` com side_effect.

    Retorna o driver + uma lista que acumula ``(query, params)`` de cada
    chamada, pra inspeção nas asserções (cache TTL etc.).
    """
    driver = MagicMock()
    session_cm = driver.session.return_value
    session = session_cm.__enter__.return_value
    calls: list[tuple[str, dict[str, Any]]] = []

    def run(
        query: str, params: dict[str, Any] | None = None,
    ) -> MagicMock:
        calls.append((query, params or {}))
        result = MagicMock()
        if "MATCH (c:Company)" in query and "RETURN c.cnpj" in query:
            result.__iter__ = lambda _self: iter(targets)
        else:
            result.__iter__ = lambda _self: iter([])
        return result

    session.run.side_effect = run
    return driver, calls


@pytest.fixture()
def archival_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "archival"
    monkeypatch.setenv("BRACC_ARCHIVAL_ROOT", str(root))
    yield root


def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutraliza o rate limit do pipeline pra não engolir a suite."""
    monkeypatch.setattr(
        "bracc_etl.pipelines.brasilapi_cnpj_status.time.sleep",
        lambda _s: None,
    )


def _make_pipeline(
    targets: list[dict[str, Any]],
    responses: dict[str, tuple[int, dict[str, Any] | None] | str],
    *,
    archive_online: bool = True,
    batch_size: int = 10,
) -> tuple[
    BrasilapiCnpjStatusPipeline,
    list[tuple[str, dict[str, Any]]],
]:
    driver, calls = _build_driver(targets)
    transport = _build_transport(responses)

    def factory() -> httpx.Client:
        return httpx.Client(transport=transport, follow_redirects=True)

    pipeline = BrasilapiCnpjStatusPipeline(
        driver=driver,
        data_dir="./data",
        batch_size=batch_size,
        archive_online=archive_online,
        http_client_factory=factory,
    )
    pipeline.run_id = "brasilapi_cnpj_20260418100000"
    return pipeline, calls


# ---------------------------------------------------------------------------
# Metadata / registry wiring
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert BrasilapiCnpjStatusPipeline.name == "brasilapi_cnpj_status"

    def test_source_id(self) -> None:
        assert BrasilapiCnpjStatusPipeline.source_id == _SOURCE_ID
        assert _SOURCE_ID == "brasilapi_cnpj"

    def test_batch_size_capped_at_500(self) -> None:
        """Rate limit diario da BrasilAPI: cap defensivo em 500."""
        pipeline = BrasilapiCnpjStatusPipeline(
            driver=MagicMock(),
            batch_size=1_000,
        )
        assert pipeline.batch_size == 500


# ---------------------------------------------------------------------------
# extract — descobre alvos no grafo + fetch
# ---------------------------------------------------------------------------


class TestExtract:
    def test_grafo_vazio_curto_circuita(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _no_sleep(monkeypatch)
        pipeline, _calls = _make_pipeline(targets=[], responses={})
        pipeline.extract()
        assert pipeline.rows_in == 0
        assert pipeline._updates == []

    def test_cache_ttl_params_passed_to_query(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Cache TTL: a query recebe ``cutoff`` de 7d atras + batch_size."""
        _no_sleep(monkeypatch)
        pipeline, calls = _make_pipeline(targets=[], responses={})
        pipeline.extract()
        # Primeira chamada = discovery query.
        discovery = next(
            (c for c in calls if "RETURN c.cnpj" in c[0]), None,
        )
        assert discovery is not None
        _query, params = discovery
        assert "cutoff" in params
        assert "batch_size" in params
        assert params["batch_size"] == 10
        # ``cutoff`` e ISO 8601 com UTC e representa ~7 dias atras.
        cutoff = datetime.fromisoformat(params["cutoff"])
        delta = datetime.now(tz=UTC) - cutoff
        # 7 dias +/- 1 minuto de slack.
        assert timedelta(days=6, hours=23) <= delta <= timedelta(days=7, hours=1)

    def test_fetch_tres_cnpjs_happy_path(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _no_sleep(monkeypatch)
        targets = [
            {"cnpj": _CNPJ_ATIVA, "razao_social": "EMPRESA ATIVA"},
            {"cnpj": _CNPJ_BAIXADA, "razao_social": "EMPRESA BAIXADA"},
            {"cnpj": _CNPJ_TIMEOUT, "razao_social": "EMPRESA TIMEOUT"},
        ]
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
            _CNPJ_BAIXADA: (200, _payload_baixada()),
            _CNPJ_TIMEOUT: "timeout",
        }
        pipeline, _calls = _make_pipeline(targets, responses)
        pipeline.extract()
        # 2 atualizacoes persistidas (timeout pulado silenciosamente).
        assert len(pipeline._updates) == 2
        situacoes = {
            upd["fields"]["situacao_cadastral"]
            for upd in pipeline._updates
        }
        assert situacoes == {"ATIVA", "BAIXADA"}

    def test_cnpj_invalido_filtrado(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CNPJ com digitos insuficientes cai fora do lote."""
        _no_sleep(monkeypatch)
        targets = [
            {"cnpj": "abc123", "razao_social": "LIXO"},
            {"cnpj": _CNPJ_ATIVA, "razao_social": "OK"},
        ]
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
        }
        pipeline, _calls = _make_pipeline(targets, responses)
        pipeline.extract()
        # So o CNPJ valido foi consultado e gerou update.
        assert len(pipeline._updates) == 1
        assert pipeline._updates[0]["cnpj_digits"] == _CNPJ_ATIVA

    def test_http_404_pulado_silenciosamente(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CNPJ que retorna 404 na BrasilAPI não vira update."""
        _no_sleep(monkeypatch)
        targets = [
            {"cnpj": _CNPJ_ATIVA, "razao_social": "OK"},
            {"cnpj": "00000000000000", "razao_social": "NAO ENCONTRADO"},
        ]
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
            # 00000000000000 nao esta no map → MockTransport devolve 404.
        }
        pipeline, _calls = _make_pipeline(targets, responses)
        pipeline.extract()
        assert len(pipeline._updates) == 1


# ---------------------------------------------------------------------------
# load — SET nas properties + proveniencia + verified_at
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_gera_set_por_cnpj(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _no_sleep(monkeypatch)
        targets = [
            {"cnpj": _CNPJ_ATIVA, "razao_social": "EMPRESA ATIVA"},
            {"cnpj": _CNPJ_BAIXADA, "razao_social": "EMPRESA BAIXADA"},
        ]
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
            _CNPJ_BAIXADA: (200, _payload_baixada()),
        }
        pipeline, calls = _make_pipeline(targets, responses)
        pipeline.extract()
        pipeline.transform()
        pipeline.load()

        set_calls = [
            c for c in calls
            if "SET c.situacao_cadastral" in c[0]
        ]
        assert len(set_calls) == 1
        _query, params = set_calls[0]
        rows = params["rows"]
        assert {r["situacao_cadastral"] for r in rows} == {"ATIVA", "BAIXADA"}
        # Proveniencia: verified_at, source_id e run_id batem no SET.
        assert params["source_id"] == _SOURCE_ID
        assert params["run_id"] == "brasilapi_cnpj_20260418100000"
        assert params["verified_at"]

    def test_load_vazio_nao_chama_set(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _no_sleep(monkeypatch)
        pipeline, calls = _make_pipeline(targets=[], responses={})
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        set_calls = [c for c in calls if "SET c.situacao_cadastral" in c[0]]
        assert set_calls == []


# ---------------------------------------------------------------------------
# Archival — content-addressed snapshots
# ---------------------------------------------------------------------------


class TestArchivalRetrofit:
    def test_snapshot_files_on_disk(
        self,
        archival_root: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _no_sleep(monkeypatch)
        targets = [
            {"cnpj": _CNPJ_ATIVA, "razao_social": "X"},
            {"cnpj": _CNPJ_BAIXADA, "razao_social": "Y"},
        ]
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
            _CNPJ_BAIXADA: (200, _payload_baixada()),
        }
        pipeline, _calls = _make_pipeline(targets, responses)
        pipeline.extract()
        pipeline.transform()

        source_dir = archival_root / _SOURCE_ID
        assert source_dir.exists()
        files = list(source_dir.rglob("*.json"))
        assert len(files) == 2

        for upd in pipeline._updates:
            uri = upd["snapshot_uri"]
            assert isinstance(uri, str) and uri
            parts = uri.split("/")
            assert parts[0] == _SOURCE_ID
            assert parts[1] == "2026-04"
            assert parts[2].endswith(".json")

    def test_snapshot_round_trip(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _no_sleep(monkeypatch)
        targets = [{"cnpj": _CNPJ_ATIVA, "razao_social": "X"}]
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
        }
        pipeline, _calls = _make_pipeline(targets, responses)
        pipeline.extract()
        uri = pipeline._updates[0]["snapshot_uri"]
        assert uri is not None
        restored = restore_snapshot(uri)
        payload = json.loads(restored.decode("utf-8"))
        assert payload["situacao_cadastral"] == "ATIVA"

    def test_archive_online_false_nao_popula_snapshot_uri(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Opt-in preservado: ``archive_online=False`` deixa uri como None."""
        _no_sleep(monkeypatch)
        targets = [{"cnpj": _CNPJ_ATIVA, "razao_social": "X"}]
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
        }
        pipeline, _calls = _make_pipeline(
            targets, responses, archive_online=False,
        )
        pipeline.extract()
        assert pipeline._updates[0]["snapshot_uri"] is None


# ---------------------------------------------------------------------------
# Rate limit — 429 aborta batch pra não queimar cota
# ---------------------------------------------------------------------------


class TestRateLimit:
    def test_429_aborta_batch(
        self,
        archival_root: Path,  # noqa: ARG002
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _no_sleep(monkeypatch)
        targets = [
            {"cnpj": _CNPJ_ATIVA, "razao_social": "X"},
            {"cnpj": _CNPJ_BAIXADA, "razao_social": "Y"},
        ]
        # Primeiro CNPJ passa; segundo devolve 429.
        responses: dict[str, tuple[int, dict[str, Any] | None] | str] = {
            _CNPJ_ATIVA: (200, _payload_ativa()),
            _CNPJ_BAIXADA: (429, {"message": "rate limit"}),
        }
        pipeline, _calls = _make_pipeline(targets, responses)
        pipeline.extract()
        # So o primeiro CNPJ virou update; extract parou no 429.
        assert len(pipeline._updates) == 1
        assert pipeline._updates[0]["cnpj_digits"] == _CNPJ_ATIVA

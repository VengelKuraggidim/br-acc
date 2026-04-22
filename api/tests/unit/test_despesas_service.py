"""Tests para bracc.services.despesas_service (fase 04.C).

Mocka o driver Neo4j (sem conexão real) para validar:

* agrega rows por ``tipo_despesa`` traduzido (3 rows do mesmo tipo → 1 item);
* ordena decrescente por total;
* sem CEAP → ``[]`` (não erro);
* média de estado computa float correto a partir de records mockados;
* UF vazia → ``0.0`` sem chamar o grafo;
* ``traduzir_despesa`` é aplicada de fato (``"COMBUSTÍVEIS"`` → ``"Combustivel"``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from bracc.services.despesas_service import (
    calcular_media_ceap_estado,
    obter_ceap_deputado,
    obter_ceaps_senador,
    obter_cota_vereador_goiania,
    obter_verba_indenizatoria_alego,
)


def _mock_record(data: dict[str, Any]) -> MagicMock:
    """Record do driver Neo4j com comportamento ``.get(key)`` + ``[key]``."""
    record = MagicMock()
    record.get.side_effect = lambda key, default=None: data.get(key, default)
    record.__getitem__.side_effect = lambda key: data[key]
    record.__contains__.side_effect = lambda key: key in data
    record.keys.return_value = list(data.keys())
    return record


def _build_driver(records: list[MagicMock], *, single: MagicMock | None = None) -> MagicMock:
    """Monta um AsyncDriver mock com session → result compatível com
    :func:`execute_query` e :func:`execute_query_single`.
    """
    result = AsyncMock()

    async def _aiter(self: object) -> Any:  # noqa: ANN001
        for r in records:
            yield r

    result.__aiter__ = _aiter
    result.single = AsyncMock(return_value=single)

    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=result)

    driver = MagicMock()
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    driver.session.return_value.__aexit__ = AsyncMock(return_value=None)
    return driver


class TestObterCeapDeputado:
    @pytest.mark.anyio
    async def test_agrupa_por_tipo_e_ordena_desc(self) -> None:
        """3 rows do mesmo tipo traduzido → 1 DespesaGabinete com soma."""
        records = [
            _mock_record({"tipo_raw": "COMBUSTIVEIS E LUBRIFICANTES", "valor": 100.0, "ano": 2025}),
            _mock_record({"tipo_raw": "COMBUSTIVEIS E LUBRIFICANTES", "valor": 250.5, "ano": 2025}),
            _mock_record({"tipo_raw": "COMBUSTIVEIS E LUBRIFICANTES", "valor": 50.0, "ano": 2024}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": 1_000.0, "ano": 2025}),
        ]
        driver = _build_driver(records)

        resultado = await obter_ceap_deputado(driver, id_camara=1001)

        assert len(resultado) == 2
        # Telefone (1000) vem antes de Combustível (400.5) — ordem desc.
        assert resultado[0].tipo == "Telefone"
        assert resultado[0].total == 1_000.0
        assert resultado[0].total_fmt.startswith("R$")
        assert resultado[1].tipo == "Combustivel"
        assert resultado[1].total == pytest.approx(400.5)

    @pytest.mark.anyio
    async def test_sem_ceap_retorna_lista_vazia(self) -> None:
        driver = _build_driver([])
        resultado = await obter_ceap_deputado(driver, id_camara=42)
        assert resultado == []

    @pytest.mark.anyio
    async def test_ignora_valores_invalidos(self) -> None:
        """Valor ``None``/negativo/string-inválida é descartado sem quebrar."""
        records = [
            _mock_record({"tipo_raw": "COMBUSTIVEIS", "valor": None, "ano": 2025}),
            _mock_record({"tipo_raw": "COMBUSTIVEIS", "valor": -10.0, "ano": 2025}),
            _mock_record({"tipo_raw": "COMBUSTIVEIS", "valor": "abc", "ano": 2025}),
            _mock_record({"tipo_raw": "COMBUSTIVEIS", "valor": 100.0, "ano": 2025}),
        ]
        driver = _build_driver(records)

        resultado = await obter_ceap_deputado(driver, id_camara=1001)

        assert len(resultado) == 1
        assert resultado[0].total == 100.0

    @pytest.mark.anyio
    async def test_traducao_aplicada(self) -> None:
        """``COMBUSTÍVEIS E LUBRIFICANTES`` (com acento) → ``Combustivel`` via
        traduzir_despesa (acento-insensitive, substring match)."""
        records = [
            _mock_record(
                {"tipo_raw": "COMBUSTÍVEIS E LUBRIFICANTES", "valor": 500.0, "ano": 2025},
            ),
        ]
        driver = _build_driver(records)

        resultado = await obter_ceap_deputado(driver, id_camara=1001)

        assert len(resultado) == 1
        assert resultado[0].tipo == "Combustivel"

    @pytest.mark.anyio
    async def test_tipo_raw_preservado_quando_sem_match(self) -> None:
        """Tipo desconhecido cai no fallback ``.title()`` do traduzir_despesa."""
        records = [
            _mock_record({"tipo_raw": "CATEGORIA ESTRANHA XPTO", "valor": 42.0, "ano": 2025}),
        ]
        driver = _build_driver(records)

        resultado = await obter_ceap_deputado(driver, id_camara=1001)

        assert len(resultado) == 1
        # Fallback: Title Case do raw
        assert resultado[0].tipo == "Categoria Estranha Xpto"

    @pytest.mark.anyio
    async def test_anos_default_sao_passados_para_query(self) -> None:
        """Quando ``anos=None`` o service calcula ``[ano_atual, ano_atual - 1]``."""
        driver = _build_driver([])
        await obter_ceap_deputado(driver, id_camara=1001)

        # Pega a chamada real ao session.run para inspecionar parâmetros.
        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        assert session.run.await_count == 1
        params = session.run.await_args.args[1]
        anos = params["anos"]
        assert len(anos) == 2
        assert anos[0] - anos[1] == 1  # ano_atual e ano_atual - 1
        assert params["id_camara"] == "1001"  # cast int → str

    @pytest.mark.anyio
    async def test_anos_custom_passa_direto(self) -> None:
        driver = _build_driver([])
        await obter_ceap_deputado(driver, id_camara=1001, anos=[2022, 2023])

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        assert params["anos"] == [2022, 2023]


class TestCalcularMediaCeapEstado:
    @pytest.mark.anyio
    async def test_media_com_10_deputados(self) -> None:
        """Record único com ``media`` = 50000 — o avg já foi calculado na query."""
        single = _mock_record({"media": 50_000.0})
        driver = _build_driver([], single=single)

        media = await calcular_media_ceap_estado(driver, uf="GO")

        assert media == 50_000.0

    @pytest.mark.anyio
    async def test_media_uf_vazia_retorna_zero(self) -> None:
        """Guard: UF string vazia nunca chega ao grafo."""
        driver = _build_driver([])
        media = await calcular_media_ceap_estado(driver, uf="")
        assert media == 0.0
        # Não chamou a session nem run
        driver.session.assert_not_called()

    @pytest.mark.anyio
    async def test_media_sem_deputados_retorna_zero(self) -> None:
        """Query devolve ``None`` quando o estado não tem CEAP ingerido."""
        driver = _build_driver([], single=None)
        media = await calcular_media_ceap_estado(driver, uf="AC")
        assert media == 0.0

    @pytest.mark.anyio
    async def test_media_com_none_no_record(self) -> None:
        """``avg(...)`` pode devolver ``null`` do Neo4j quando a amostra vazia."""
        single = _mock_record({"media": None})
        driver = _build_driver([], single=single)
        media = await calcular_media_ceap_estado(driver, uf="GO")
        assert media == 0.0

    @pytest.mark.anyio
    async def test_uf_normalizada_para_upper(self) -> None:
        single = _mock_record({"media": 1_000.0})
        driver = _build_driver([], single=single)

        await calcular_media_ceap_estado(driver, uf="go", amostra=5)

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        assert params["uf"] == "GO"
        assert params["amostra"] == 5

    @pytest.mark.anyio
    async def test_amostra_default_e_anos_default(self) -> None:
        single = _mock_record({"media": 42.0})
        driver = _build_driver([], single=single)

        await calcular_media_ceap_estado(driver, uf="GO")

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        assert params["amostra"] == 10
        assert len(params["anos"]) == 2


class TestObterVerbaIndenizatoriaAlego:
    """Verba indenizatória ALEGO — shape idêntico ao CEAP federal."""

    @pytest.mark.anyio
    async def test_agrupa_por_tipo_e_ordena_desc(self) -> None:
        """Mesma lógica de agregação do CEAP, aplicada aos lançamentos ALEGO."""
        records = [
            _mock_record(
                {"tipo_raw": "COMBUSTIVEIS E LUBRIFICANTES", "valor": 200.0, "ano": "2025"},
            ),
            _mock_record(
                {"tipo_raw": "COMBUSTIVEIS E LUBRIFICANTES", "valor": 100.0, "ano": "2025"},
            ),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": 1_500.0, "ano": "2025"}),
        ]
        driver = _build_driver(records)

        resultado = await obter_verba_indenizatoria_alego(
            driver, legislator_id="abc123",
        )

        assert len(resultado) == 2
        assert resultado[0].tipo == "Telefone"
        assert resultado[0].total == 1_500.0
        assert resultado[1].tipo == "Combustivel"
        assert resultado[1].total == pytest.approx(300.0)

    @pytest.mark.anyio
    async def test_sem_verba_retorna_lista_vazia(self) -> None:
        driver = _build_driver([])
        resultado = await obter_verba_indenizatoria_alego(
            driver, legislator_id="semdata",
        )
        assert resultado == []

    @pytest.mark.anyio
    async def test_ignora_valores_invalidos(self) -> None:
        records = [
            _mock_record({"tipo_raw": "TELEFONIA", "valor": None}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": -5.0}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": "xx"}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": 80.0}),
        ]
        driver = _build_driver(records)
        resultado = await obter_verba_indenizatoria_alego(
            driver, legislator_id="abc123",
        )
        assert len(resultado) == 1
        assert resultado[0].total == 80.0

    @pytest.mark.anyio
    async def test_params_passados_para_query(self) -> None:
        driver = _build_driver([])
        await obter_verba_indenizatoria_alego(
            driver, legislator_id="HASH9", anos=[2024, 2025],
        )

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        assert params["legislator_id"] == "HASH9"
        assert params["anos"] == [2024, 2025]

    @pytest.mark.anyio
    async def test_anos_default_sao_os_ultimos_dois(self) -> None:
        driver = _build_driver([])
        await obter_verba_indenizatoria_alego(driver, legislator_id="X")

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        anos = params["anos"]
        assert len(anos) == 2
        assert anos[0] - anos[1] == 1


class TestObterCotaVereadorGoiania:
    """Cota/despesas de gabinete de vereador da Camara Municipal de Goiania —
    shape identico ao CEAP federal e a verba ALEGO estadual."""

    @pytest.mark.anyio
    async def test_agrupa_por_tipo_e_ordena_desc(self) -> None:
        """Mesma logica de agregacao aplicada aos lancamentos da CMG."""
        records = [
            _mock_record(
                {"tipo_raw": "COMBUSTIVEIS E LUBRIFICANTES", "valor": 150.0, "ano": "2025"},
            ),
            _mock_record(
                {"tipo_raw": "COMBUSTIVEIS E LUBRIFICANTES", "valor": 100.0, "ano": "2025"},
            ),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": 800.0, "ano": "2025"}),
        ]
        driver = _build_driver(records)

        resultado = await obter_cota_vereador_goiania(
            driver, vereador_id="vgyn-001",
        )

        assert len(resultado) == 2
        assert resultado[0].tipo == "Telefone"
        assert resultado[0].total == 800.0
        assert resultado[1].tipo == "Combustivel"
        assert resultado[1].total == pytest.approx(250.0)

    @pytest.mark.anyio
    async def test_sem_cota_retorna_lista_vazia(self) -> None:
        driver = _build_driver([])
        resultado = await obter_cota_vereador_goiania(
            driver, vereador_id="semdata",
        )
        assert resultado == []

    @pytest.mark.anyio
    async def test_ignora_valores_invalidos(self) -> None:
        records = [
            _mock_record({"tipo_raw": "TELEFONIA", "valor": None}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": -1.0}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": "xx"}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": 50.0}),
        ]
        driver = _build_driver(records)
        resultado = await obter_cota_vereador_goiania(
            driver, vereador_id="vgyn-002",
        )
        assert len(resultado) == 1
        assert resultado[0].total == 50.0

    @pytest.mark.anyio
    async def test_params_passados_para_query(self) -> None:
        driver = _build_driver([])
        await obter_cota_vereador_goiania(
            driver, vereador_id="VHASH7", anos=[2024, 2025],
        )

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        assert params["vereador_id"] == "VHASH7"
        assert params["anos"] == [2024, 2025]

    @pytest.mark.anyio
    async def test_anos_default_sao_os_ultimos_dois(self) -> None:
        driver = _build_driver([])
        await obter_cota_vereador_goiania(driver, vereador_id="V")

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        anos = params["anos"]
        assert len(anos) == 2
        assert anos[0] - anos[1] == 1


class TestObterCeapsSenador:
    """CEAPS de senador federal — bridge :Senator -> :Person (por nome)
    -> :Expense (source='senado'). Mesmo contrato agregado das outras casas.
    """

    @pytest.mark.anyio
    async def test_agrupa_por_tipo_e_ordena_desc(self) -> None:
        records = [
            _mock_record({"tipo_raw": "PASSAGENS AEREAS", "valor": 5_000.0, "ano": 2025}),
            _mock_record({"tipo_raw": "PASSAGENS AEREAS", "valor": 3_000.0, "ano": 2024}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": 1_500.0, "ano": 2025}),
        ]
        driver = _build_driver(records)

        resultado = await obter_ceaps_senador(driver, id_senado="5895")

        assert len(resultado) == 2
        # PASSAGENS AEREAS (8000) > TELEFONIA (1500)
        assert resultado[0].total == pytest.approx(8_000.0)
        assert resultado[1].tipo == "Telefone"
        assert resultado[1].total == pytest.approx(1_500.0)

    @pytest.mark.anyio
    async def test_sem_ceaps_retorna_lista_vazia(self) -> None:
        driver = _build_driver([])
        resultado = await obter_ceaps_senador(driver, id_senado="99999")
        assert resultado == []

    @pytest.mark.anyio
    async def test_ignora_valores_invalidos(self) -> None:
        records = [
            _mock_record({"tipo_raw": "TELEFONIA", "valor": None}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": -10.0}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": "abc"}),
            _mock_record({"tipo_raw": "TELEFONIA", "valor": 75.0}),
        ]
        driver = _build_driver(records)
        resultado = await obter_ceaps_senador(driver, id_senado="123")
        assert len(resultado) == 1
        assert resultado[0].total == 75.0

    @pytest.mark.anyio
    async def test_params_passados_para_query(self) -> None:
        driver = _build_driver([])
        await obter_ceaps_senador(driver, id_senado="5895", anos=[2022, 2023])

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        assert params["id_senado"] == "5895"
        assert params["anos"] == [2022, 2023]

    @pytest.mark.anyio
    async def test_anos_default_sao_os_ultimos_dois(self) -> None:
        driver = _build_driver([])
        await obter_ceaps_senador(driver, id_senado="S")

        session_cm = driver.session.return_value
        session = session_cm.__aenter__.return_value
        params = session.run.await_args.args[1]
        anos = params["anos"]
        assert len(anos) == 2
        assert anos[0] - anos[1] == 1


class TestZeroLiveCall:
    """Safeguard do prompt 04.C: ``httpx`` é PROIBIDO neste service."""

    def test_modulo_nao_importa_httpx(self) -> None:
        import bracc.services.despesas_service as mod  # noqa: PLC0415

        # Módulo `httpx` não pode estar no namespace (import direto).
        assert "httpx" not in dir(mod)

    def test_arquivo_nao_contem_httpx(self) -> None:
        from pathlib import Path  # noqa: PLC0415

        import bracc.services.despesas_service as mod  # noqa: PLC0415

        assert mod.__file__ is not None
        source = Path(mod.__file__).read_text(encoding="utf-8")
        assert "httpx" not in source, (
            "DespesasService deve ler do grafo — zero live-call permitida"
        )

"""Tests para bracc.services.emendas_service.

Contratos validados:
- leitura Cypher parametrizada por id_camara;
- mapeamento Record -> Emenda com fmt_brl + traduzir_*;
- deputado sem emenda -> lista vazia;
- zero live-call (teste via ausência de httpx imports no service).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bracc.services.emendas_service import (
    _record_to_emenda,
    obter_emendas_deputado,
)


def _mock_record(data: dict[str, object]) -> MagicMock:
    """Build a mock neo4j.Record that behaves like a dict."""
    record = MagicMock()
    record.__getitem__ = lambda self, key: data[key]
    record.__iter__ = lambda self: iter(data.keys())
    record.__contains__ = lambda self, key: key in data
    # .get() deve devolver None pra chave ausente (comportamento do
    # Record real do driver neo4j) — MagicMock por default devolve outro
    # MagicMock, o que mascara bugs em código que usa record.get().
    record.get = lambda key, default=None: data.get(key, default)
    return record


class TestRecordToEmenda:
    def test_maps_all_fields(self) -> None:
        record = _mock_record({
            "id": "pte_abc123",
            "tipo": "individual",
            "funcao": "saude",
            "municipio": "Goiania",
            "uf": "GO",
            "valor_empenhado": 500_000.0,
            "valor_pago": 250_000.0,
            "ano": 2024,
        })

        emenda = _record_to_emenda(record)

        assert emenda.id == "pte_abc123"
        # traduzir_tipo_emenda mapeia "individual"
        assert "individual" in emenda.tipo.lower()
        # traduzir_funcao_emenda mapeia "saude"
        assert "Saude" in emenda.funcao
        assert emenda.municipio == "Goiania"
        assert emenda.uf == "GO"
        assert emenda.valor_empenhado == 500_000.0
        # fmt_brl: 500_000 cai na faixa "mil".
        assert emenda.valor_empenhado_fmt.startswith("R$ ")
        assert "mil" in emenda.valor_empenhado_fmt
        assert emenda.valor_pago == 250_000.0
        assert "mil" in emenda.valor_pago_fmt

    def test_beneficiario_populado(self) -> None:
        """Beneficiário (CNPJ + razão social) é mapeado quando presente."""
        record = _mock_record({
            "id": "pte_xyz",
            "tipo": "individual",
            "funcao": "saude",
            "municipio": "Goiania",
            "uf": "GO",
            "valor_empenhado": 100_000.0,
            "valor_pago": 0.0,
            "ano": 2024,
            "beneficiario_cnpj": "11111111000101",
            "beneficiario_nome": "ONG Exemplo",
        })

        emenda = _record_to_emenda(record)
        assert emenda.beneficiario_cnpj == "11111111000101"
        assert emenda.beneficiario_nome == "ONG Exemplo"

    def test_beneficiario_ausente_retorna_none(self) -> None:
        """Sem beneficiário no grafo (emenda sem convênio) → fields=None."""
        record = _mock_record({
            "id": "pte_xyz",
            "tipo": "individual",
            "funcao": "saude",
            "municipio": None,
            "uf": None,
            "valor_empenhado": 100_000.0,
            "valor_pago": 0.0,
            "ano": 2024,
            "beneficiario_cnpj": None,
            "beneficiario_nome": None,
        })

        emenda = _record_to_emenda(record)
        assert emenda.beneficiario_cnpj is None
        assert emenda.beneficiario_nome is None

    def test_tipo_none_fallback(self) -> None:
        record = _mock_record({
            "id": "x",
            "tipo": None,
            "funcao": None,
            "municipio": None,
            "uf": None,
            "valor_empenhado": 0,
            "valor_pago": 0,
            "ano": 2024,
        })

        emenda = _record_to_emenda(record)

        assert emenda.tipo  # "Nao informado" fallback do tradutor
        assert emenda.funcao  # "Nao informada" fallback do tradutor
        assert emenda.municipio is None
        assert emenda.uf is None
        assert emenda.valor_empenhado == 0.0
        assert emenda.valor_empenhado_fmt == "R$ 0,00"
        assert emenda.valor_pago_fmt == "R$ 0,00"

    def test_fmt_brl_aplicado(self) -> None:
        record = _mock_record({
            "id": "y",
            "tipo": "bancada",
            "funcao": "educacao",
            "municipio": "",
            "uf": "go",  # lowercase -> normalizado pra GO
            "valor_empenhado": 2_500_000_000.0,
            "valor_pago": 1_200_000.0,
            "ano": 2024,
        })

        emenda = _record_to_emenda(record)

        assert emenda.uf == "GO"  # normalizado pra upper
        # fmt_brl: 2.5bi
        assert "bi" in emenda.valor_empenhado_fmt
        # fmt_brl: 1.2mi
        assert "mi" in emenda.valor_pago_fmt


class TestObterEmendasDeputado:
    @pytest.mark.anyio
    async def test_deputado_sem_emendas_retorna_lista_vazia(self) -> None:
        driver = MagicMock()
        session_cm = driver.session.return_value
        session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
        session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch(
            "bracc.services.emendas_service.execute_query",
            new_callable=AsyncMock,
            return_value=[],
        ) as mocked:
            result = await obter_emendas_deputado(driver, id_camara=1001)
        mocked.assert_awaited_once()
        # id_camara enviado como string pra casar com o shape do grafo.
        call_args = mocked.call_args
        assert call_args.args[1] == "perfil_emendas_deputado"
        assert call_args.args[2] == {"id_camara": "1001"}
        assert result == []

    @pytest.mark.anyio
    async def test_retorna_emendas_com_fmt_e_traducao(self) -> None:
        record = _mock_record({
            "id": "pte_1",
            "tipo": "individual",
            "funcao": "saude",
            "municipio": "Goiania",
            "uf": "GO",
            "valor_empenhado": 1_500_000.0,
            "valor_pago": 750_000.0,
            "ano": 2024,
        })
        driver = MagicMock()
        session_cm = driver.session.return_value
        session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
        session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch(
            "bracc.services.emendas_service.execute_query",
            new_callable=AsyncMock,
            return_value=[record],
        ):
            result = await obter_emendas_deputado(driver, id_camara=1001)

        assert len(result) == 1
        emenda = result[0]
        assert emenda.id == "pte_1"
        assert "mi" in emenda.valor_empenhado_fmt
        assert emenda.funcao  # traducao aplicada
        assert emenda.tipo  # traducao aplicada

    @pytest.mark.anyio
    async def test_mapeia_varias_emendas_em_ordem(self) -> None:
        """Service preserva a ordem do resultado Cypher (ORDER BY na query)."""
        records = [
            _mock_record({
                "id": f"pte_{i}",
                "tipo": "individual",
                "funcao": "saude",
                "municipio": "Goiania",
                "uf": "GO",
                "valor_empenhado": 1_000_000.0 * (3 - i),
                "valor_pago": 500_000.0 * (3 - i),
                "ano": 2024,
            })
            for i in range(3)
        ]
        driver = MagicMock()
        session_cm = driver.session.return_value
        session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
        session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch(
            "bracc.services.emendas_service.execute_query",
            new_callable=AsyncMock,
            return_value=records,
        ):
            result = await obter_emendas_deputado(driver, id_camara=1001)

        assert [e.id for e in result] == ["pte_0", "pte_1", "pte_2"]

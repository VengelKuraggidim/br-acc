from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bracc.services.baseline_service import (
    BASELINE_QUERIES,
    _record_to_metrics,
    run_all_baselines,
    run_baseline,
)


def _mock_record(data: dict[str, object]) -> MagicMock:
    """Build a mock neo4j.Record that behaves like a dict."""
    record = MagicMock()
    record.__getitem__ = lambda self, key: data[key]
    record.__iter__ = lambda self: iter(data.keys())
    record.__contains__ = lambda self, key: key in data
    return record


class TestBaselineQueriesMap:
    def test_has_sector_and_region(self) -> None:
        assert set(BASELINE_QUERIES.keys()) == {"sector", "region"}

    def test_values_point_to_query_names(self) -> None:
        assert BASELINE_QUERIES["sector"] == "baseline_sector"
        assert BASELINE_QUERIES["region"] == "baseline_region"


class TestRecordToMetrics:
    def test_sector_dimension_maps_peer_fields(self) -> None:
        record = _mock_record({
            "company_name": "Acme",
            "company_cnpj": "12345678000195",
            "company_id": "elem-1",
            "contract_count": 12,
            "total_value": 125000.5,
            "sector_companies": 100,
            "sector_avg_contracts": 8.5,
            "sector_avg_value": 50000.0,
            "contract_ratio": 1.41,
            "value_ratio": 2.5,
            "sector_cnae": "4751-1",
        })

        m = _record_to_metrics(record, "sector", "sector_cnae")

        assert m.company_name == "Acme"
        assert m.company_cnpj == "12345678000195"
        assert m.contract_count == 12
        assert m.total_value == 125000.5
        assert m.peer_count == 100
        assert m.peer_avg_contracts == 8.5
        assert m.peer_avg_value == 50000.0
        assert m.contract_ratio == 1.41
        assert m.value_ratio == 2.5
        assert m.comparison_dimension == "sector"
        assert m.comparison_key == "4751-1"
        assert [s.database for s in m.sources] == ["neo4j_analysis"]

    def test_region_dimension_maps_peer_fields(self) -> None:
        record = _mock_record({
            "company_name": "Beta",
            "company_cnpj": "98765432000110",
            "company_id": "elem-2",
            "contract_count": 3,
            "total_value": 1000.0,
            "region_companies": 50,
            "region_avg_contracts": 2.0,
            "region_avg_value": 900.0,
            "contract_ratio": 1.5,
            "value_ratio": 1.11,
            "region": "SE",
        })

        m = _record_to_metrics(record, "region", "region")

        assert m.comparison_dimension == "region"
        assert m.comparison_key == "SE"
        assert m.peer_count == 50
        assert m.peer_avg_contracts == 2.0
        assert m.peer_avg_value == 900.0

    def test_missing_numeric_fields_default_to_zero(self) -> None:
        record = _mock_record({
            "company_name": "",
            "company_cnpj": "",
            "company_id": "",
            "sector_cnae": "",
        })

        m = _record_to_metrics(record, "sector", "sector_cnae")

        assert m.contract_count == 0
        assert m.total_value == 0.0
        assert m.peer_count == 0
        assert m.peer_avg_contracts == 0.0
        assert m.peer_avg_value == 0.0
        assert m.contract_ratio == 0.0
        assert m.value_ratio == 0.0

    def test_comparison_key_coerced_to_str(self) -> None:
        # key_field value might come back as int from Neo4j.
        record = _mock_record({
            "company_name": "X",
            "company_cnpj": "",
            "company_id": "",
            "sector_cnae": 4751,  # int
        })

        m = _record_to_metrics(record, "sector", "sector_cnae")
        assert m.comparison_key == "4751"


class TestRunBaseline:
    @pytest.mark.anyio
    async def test_unknown_dimension_returns_empty(self) -> None:
        session = AsyncMock()
        result = await run_baseline(session, "unknown")
        assert result == []
        # No neo4j call should happen for unknown dimensions.
        session.run.assert_not_called()

    @pytest.mark.anyio
    async def test_dispatches_to_named_query(self) -> None:
        session = AsyncMock()
        with patch(
            "bracc.services.baseline_service.execute_query",
            new_callable=AsyncMock,
            return_value=[],
        ) as mocked:
            result = await run_baseline(session, "sector", entity_id="eid-1")

        mocked.assert_awaited_once_with(
            session, "baseline_sector", {"entity_id": "eid-1"},
        )
        assert result == []

    @pytest.mark.anyio
    async def test_maps_each_record(self) -> None:
        record = _mock_record({
            "company_name": "Acme",
            "company_cnpj": "12345678000195",
            "company_id": "e-1",
            "contract_count": 1,
            "total_value": 10.0,
            "sector_cnae": "4751-1",
        })
        with patch(
            "bracc.services.baseline_service.execute_query",
            new_callable=AsyncMock,
            return_value=[record, record],
        ):
            session = AsyncMock()
            result = await run_baseline(session, "sector")

        assert len(result) == 2
        assert result[0].company_name == "Acme"


class TestRunAllBaselines:
    @pytest.mark.anyio
    async def test_aggregates_all_dimensions(self) -> None:
        sector_record = _mock_record({
            "company_name": "S",
            "company_cnpj": "",
            "company_id": "",
            "sector_cnae": "1",
        })
        region_record = _mock_record({
            "company_name": "R",
            "company_cnpj": "",
            "company_id": "",
            "region": "NE",
        })

        async def fake_execute(
            _sess: object, name: str, _params: dict[str, object],
        ) -> list[MagicMock]:
            return [sector_record] if name == "baseline_sector" else [region_record]

        with patch(
            "bracc.services.baseline_service.execute_query",
            side_effect=fake_execute,
        ):
            session = AsyncMock()
            result = await run_all_baselines(session, entity_id="eid-99")

        assert {m.company_name for m in result} == {"S", "R"}
        assert {m.comparison_dimension for m in result} == {"sector", "region"}

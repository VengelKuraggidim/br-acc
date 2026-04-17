"""Contract tests for bracc_etl.schemas.transparencia."""

from __future__ import annotations

import pandas as pd
import pandera.errors as pa_errors
import pytest

from bracc_etl.schemas.transparencia import (
    amendments_schema,
    contracts_schema,
    offices_schema,
)


def _valid_contract_row() -> dict[str, object]:
    return {
        "contract_id": "contract_2024_001",
        "object": "Servicos de TI",
        "value": 150000.0,
        "contracting_org": "Ministerio X",
        "date": "2024-03-15",
        "cnpj": "12.345.678/0001-95",
        "razao_social": "ACME LTDA",
    }


def _valid_office_row() -> dict[str, object]:
    return {
        "office_id": "office_12345",
        "servidor_id": "srv_001",
        "cpf_partial": "*****123**",
        "name": "FULANO DE TAL",
        "org": "Ministerio Y",
        "salary": 8500.0,
    }


def _valid_amendment_row() -> dict[str, object]:
    return {
        "amendment_id": "amend_2024_001",
        "author_key": "deputy_xyz",
        "name": "Emenda A",
        "object": "Saude",
        "value": 500000.0,
    }


class TestContractsSchema:
    def test_valid_row_passes(self) -> None:
        contracts_schema.validate(pd.DataFrame([_valid_contract_row()]))

    def test_empty_contract_id_rejected(self) -> None:
        row = _valid_contract_row()
        row["contract_id"] = ""
        with pytest.raises(pa_errors.SchemaError):
            contracts_schema.validate(pd.DataFrame([row]))

    def test_negative_value_rejected(self) -> None:
        row = _valid_contract_row()
        row["value"] = -0.01
        with pytest.raises(pa_errors.SchemaError):
            contracts_schema.validate(pd.DataFrame([row]))

    def test_raw_14_digit_cnpj_accepted(self) -> None:
        row = _valid_contract_row()
        row["cnpj"] = "12345678000195"
        contracts_schema.validate(pd.DataFrame([row]))

    def test_malformed_cnpj_rejected(self) -> None:
        row = _valid_contract_row()
        row["cnpj"] = "1234567800019"  # 13 digits
        with pytest.raises(pa_errors.SchemaError):
            contracts_schema.validate(pd.DataFrame([row]))

    def test_cpf_shape_in_cnpj_rejected(self) -> None:
        row = _valid_contract_row()
        row["cnpj"] = "123.456.789-01"
        with pytest.raises(pa_errors.SchemaError):
            contracts_schema.validate(pd.DataFrame([row]))

    def test_nullable_cnpj(self) -> None:
        row = _valid_contract_row()
        row["cnpj"] = None
        contracts_schema.validate(pd.DataFrame([row]))

    def test_strict_false_allows_extra_columns(self) -> None:
        row = _valid_contract_row()
        row["new_column"] = "value"
        contracts_schema.validate(pd.DataFrame([row]))


class TestOfficesSchema:
    def test_valid_row_passes(self) -> None:
        offices_schema.validate(pd.DataFrame([_valid_office_row()]))

    def test_empty_office_id_rejected(self) -> None:
        row = _valid_office_row()
        row["office_id"] = ""
        with pytest.raises(pa_errors.SchemaError):
            offices_schema.validate(pd.DataFrame([row]))

    def test_empty_servidor_id_rejected(self) -> None:
        row = _valid_office_row()
        row["servidor_id"] = ""
        with pytest.raises(pa_errors.SchemaError):
            offices_schema.validate(pd.DataFrame([row]))

    def test_negative_salary_rejected(self) -> None:
        row = _valid_office_row()
        row["salary"] = -1.0
        with pytest.raises(pa_errors.SchemaError):
            offices_schema.validate(pd.DataFrame([row]))

    def test_zero_salary_accepted(self) -> None:
        row = _valid_office_row()
        row["salary"] = 0.0
        offices_schema.validate(pd.DataFrame([row]))

    def test_nullable_cpf_partial(self) -> None:
        row = _valid_office_row()
        row["cpf_partial"] = None
        offices_schema.validate(pd.DataFrame([row]))


class TestAmendmentsSchema:
    def test_valid_row_passes(self) -> None:
        amendments_schema.validate(pd.DataFrame([_valid_amendment_row()]))

    def test_empty_amendment_id_rejected(self) -> None:
        row = _valid_amendment_row()
        row["amendment_id"] = ""
        with pytest.raises(pa_errors.SchemaError):
            amendments_schema.validate(pd.DataFrame([row]))

    def test_empty_author_key_rejected(self) -> None:
        row = _valid_amendment_row()
        row["author_key"] = ""
        with pytest.raises(pa_errors.SchemaError):
            amendments_schema.validate(pd.DataFrame([row]))

    def test_negative_value_rejected(self) -> None:
        row = _valid_amendment_row()
        row["value"] = -1.0
        with pytest.raises(pa_errors.SchemaError):
            amendments_schema.validate(pd.DataFrame([row]))

    def test_nullable_value(self) -> None:
        row = _valid_amendment_row()
        row["value"] = None
        amendments_schema.validate(pd.DataFrame([row]))

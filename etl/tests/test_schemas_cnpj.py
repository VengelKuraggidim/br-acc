"""Contract tests for bracc_etl.schemas.cnpj."""

from __future__ import annotations

import pandas as pd
import pandera.errors as pa_errors
import pytest

from bracc_etl.schemas.cnpj import (
    empresas_schema,
    socio_relationship_schema,
    socios_partial_schema,
    socios_pf_schema,
)


def _valid_empresa_row() -> dict[str, object]:
    return {
        "cnpj": "12.345.678/0001-95",
        "razao_social": "ACME LTDA",
        "natureza_juridica": "2062",
        "cnae_principal": "4751-2",
        "capital_social": 100000.0,
        "uf": "SP",
        "municipio": "SAO PAULO",
        "porte_empresa": "02",
    }


def _valid_socio_pf_row() -> dict[str, object]:
    return {
        "name": "FULANO DE TAL",
        "cpf": "123.456.789-01",
        "tipo_socio": "2",
    }


def _valid_socio_partial_row() -> dict[str, object]:
    return {
        "partner_id": "hash_abc123",
        "name": "BELTRANO",
        "doc_raw": "***.123.456-**",
        "doc_digits": "123456",
        "doc_partial": "***.123.456-**",
        "doc_type": "cpf_partial",
        "tipo_socio": "2",
        "identity_quality": "partial",
        "source": "cnpj",
    }


def _valid_socio_rel_row() -> dict[str, object]:
    return {
        "source_key": "12.345.678/0001-95",
        "target_key": "123.456.789-01",
        "tipo_socio": "2",
        "qualificacao": "49",
        "data_entrada": "2020-01-15",
        "snapshot_date": "2024-03-01",
    }


class TestEmpresasSchema:
    def test_valid_formatted_cnpj(self) -> None:
        empresas_schema.validate(pd.DataFrame([_valid_empresa_row()]))

    def test_accepts_8_digit_cnpj_root(self) -> None:
        # Schema allows 8–14 raw digits (partial CNPJ / root).
        row = _valid_empresa_row()
        row["cnpj"] = "12345678"
        empresas_schema.validate(pd.DataFrame([row]))

    def test_accepts_14_digit_raw(self) -> None:
        row = _valid_empresa_row()
        row["cnpj"] = "12345678000195"
        empresas_schema.validate(pd.DataFrame([row]))

    def test_rejects_7_digit_cnpj(self) -> None:
        row = _valid_empresa_row()
        row["cnpj"] = "1234567"
        with pytest.raises(pa_errors.SchemaError):
            empresas_schema.validate(pd.DataFrame([row]))

    def test_nullable_cnpj(self) -> None:
        row = _valid_empresa_row()
        row["cnpj"] = None
        empresas_schema.validate(pd.DataFrame([row]))

    def test_negative_capital_social_rejected(self) -> None:
        row = _valid_empresa_row()
        row["capital_social"] = -1.0
        with pytest.raises(pa_errors.SchemaError):
            empresas_schema.validate(pd.DataFrame([row]))

    def test_uf_accepts_2_uppercase_letters(self) -> None:
        for uf in ("SP", "RJ", "GO", "DF"):
            row = _valid_empresa_row()
            row["uf"] = uf
            empresas_schema.validate(pd.DataFrame([row]))

    def test_uf_rejects_lowercase(self) -> None:
        row = _valid_empresa_row()
        row["uf"] = "sp"
        with pytest.raises(pa_errors.SchemaError):
            empresas_schema.validate(pd.DataFrame([row]))

    def test_uf_accepts_empty(self) -> None:
        row = _valid_empresa_row()
        row["uf"] = ""
        empresas_schema.validate(pd.DataFrame([row]))

    def test_strict_false_allows_extra_columns(self) -> None:
        row = _valid_empresa_row()
        row["new_col"] = "x"
        empresas_schema.validate(pd.DataFrame([row]))


class TestSociosPfSchema:
    def test_valid_formatted_cpf(self) -> None:
        socios_pf_schema.validate(pd.DataFrame([_valid_socio_pf_row()]))

    def test_valid_raw_11_digit_cpf(self) -> None:
        row = _valid_socio_pf_row()
        row["cpf"] = "12345678901"
        socios_pf_schema.validate(pd.DataFrame([row]))

    def test_rejects_10_digit_cpf(self) -> None:
        row = _valid_socio_pf_row()
        row["cpf"] = "1234567890"
        with pytest.raises(pa_errors.SchemaError):
            socios_pf_schema.validate(pd.DataFrame([row]))

    def test_rejects_cnpj_in_cpf(self) -> None:
        row = _valid_socio_pf_row()
        row["cpf"] = "12.345.678/0001-95"
        with pytest.raises(pa_errors.SchemaError):
            socios_pf_schema.validate(pd.DataFrame([row]))

    def test_nullable_cpf(self) -> None:
        row = _valid_socio_pf_row()
        row["cpf"] = None
        socios_pf_schema.validate(pd.DataFrame([row]))


class TestSociosPartialSchema:
    def test_valid_row_passes(self) -> None:
        socios_partial_schema.validate(pd.DataFrame([_valid_socio_partial_row()]))

    def test_empty_partner_id_rejected(self) -> None:
        row = _valid_socio_partial_row()
        row["partner_id"] = ""
        with pytest.raises(pa_errors.SchemaError):
            socios_partial_schema.validate(pd.DataFrame([row]))

    @pytest.mark.parametrize("quality", ["partial", "unknown", ""])
    def test_identity_quality_accepts_allowed_values(self, quality: str) -> None:
        row = _valid_socio_partial_row()
        row["identity_quality"] = quality
        socios_partial_schema.validate(pd.DataFrame([row]))

    def test_identity_quality_rejects_other_values(self) -> None:
        row = _valid_socio_partial_row()
        row["identity_quality"] = "strong"  # not in allowed set
        with pytest.raises(pa_errors.SchemaError):
            socios_partial_schema.validate(pd.DataFrame([row]))


class TestSocioRelationshipSchema:
    def test_valid_row_passes(self) -> None:
        socio_relationship_schema.validate(pd.DataFrame([_valid_socio_rel_row()]))

    def test_empty_source_key_rejected(self) -> None:
        row = _valid_socio_rel_row()
        row["source_key"] = ""
        with pytest.raises(pa_errors.SchemaError):
            socio_relationship_schema.validate(pd.DataFrame([row]))

    def test_empty_target_key_rejected(self) -> None:
        row = _valid_socio_rel_row()
        row["target_key"] = ""
        with pytest.raises(pa_errors.SchemaError):
            socio_relationship_schema.validate(pd.DataFrame([row]))

    def test_optional_fields_nullable(self) -> None:
        row = _valid_socio_rel_row()
        row["data_entrada"] = None
        row["snapshot_date"] = None
        row["qualificacao"] = None
        socio_relationship_schema.validate(pd.DataFrame([row]))

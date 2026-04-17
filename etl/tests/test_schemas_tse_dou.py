"""Contract tests for bracc_etl.schemas.tse and schemas.dou."""

from __future__ import annotations

import pandas as pd
import pandera.errors as pa_errors
import pytest

from bracc_etl.schemas.dou import (
    acts_schema,
    company_rels_schema,
    person_rels_schema,
)
from bracc_etl.schemas.tse import (
    candidates_schema,
    donations_schema,
    elections_schema,
)

# ---- TSE fixtures --------------------------------------------------


def _valid_candidate_row() -> dict[str, object]:
    return {
        "sq_candidato": "250000000001",
        "name": "FULANO DE TAL",
        "partido": "PL",
        "uf": "GO",
        "cpf": "123.456.789-01",
    }


def _valid_election_row() -> dict[str, object]:
    return {
        "year": 2024,
        "cargo": "VEREADOR",
        "uf": "GO",
        "municipio": "GOIANIA",
        "candidate_sq": "250000000001",
    }


def _valid_donation_row() -> dict[str, object]:
    return {
        "candidate_sq": "250000000001",
        "donor_doc": "12.345.678/0001-95",
        "donor_name": "ACME LTDA",
        "donor_is_company": True,
        "valor": 5000.0,
        "year": 2024,
    }


# ---- DOU fixtures --------------------------------------------------


def _valid_act_row() -> dict[str, object]:
    return {
        "act_id": "act_abc_2024",
        "title": "DECRETO DE NOMEACAO",
        "act_type": "nomeacao",
        "date": "2024-01-15",
        "section": "secao_1",
        "agency": "Ministerio X",
        "category": "Atos do Poder Executivo",
        "text_excerpt": "Nomear FULANO para o cargo de Diretor.",
        "url": "https://www.in.gov.br/web/dou/-/decreto-de-nomeacao-123",
        "source": "imprensa_nacional",
    }


def _valid_person_rel_row() -> dict[str, object]:
    return {
        "source_key": "123.456.789-01",
        "target_key": "act_abc_2024",
    }


def _valid_company_rel_row() -> dict[str, object]:
    return {
        "source_key": "12.345.678/0001-95",
        "target_key": "act_abc_2024",
    }


# ---- TSE tests -----------------------------------------------------


class TestCandidatesSchema:
    def test_valid_row_passes(self) -> None:
        candidates_schema.validate(pd.DataFrame([_valid_candidate_row()]))

    def test_empty_sq_candidato_rejected(self) -> None:
        row = _valid_candidate_row()
        row["sq_candidato"] = ""
        with pytest.raises(pa_errors.SchemaError):
            candidates_schema.validate(pd.DataFrame([row]))

    def test_lowercase_uf_rejected(self) -> None:
        row = _valid_candidate_row()
        row["uf"] = "go"
        with pytest.raises(pa_errors.SchemaError):
            candidates_schema.validate(pd.DataFrame([row]))

    def test_cpf_field_optional(self) -> None:
        # TSE 2024 masks candidate CPF as "-4"; pipeline drops the key
        # entirely. Schema must accept rows without a cpf column.
        row = _valid_candidate_row()
        del row["cpf"]
        candidates_schema.validate(pd.DataFrame([row]))

    def test_cpf_nullable(self) -> None:
        row = _valid_candidate_row()
        row["cpf"] = None
        candidates_schema.validate(pd.DataFrame([row]))

    def test_cpf_shape_enforced_when_present(self) -> None:
        row = _valid_candidate_row()
        row["cpf"] = "not-a-cpf"
        with pytest.raises(pa_errors.SchemaError):
            candidates_schema.validate(pd.DataFrame([row]))


class TestElectionsSchema:
    def test_valid_row_passes(self) -> None:
        elections_schema.validate(pd.DataFrame([_valid_election_row()]))

    def test_year_in_range(self) -> None:
        for year in (1945, 2000, 2030):
            row = _valid_election_row()
            row["year"] = year
            elections_schema.validate(pd.DataFrame([row]))

    def test_year_below_range_rejected(self) -> None:
        row = _valid_election_row()
        row["year"] = 1944
        with pytest.raises(pa_errors.SchemaError):
            elections_schema.validate(pd.DataFrame([row]))

    def test_year_above_range_rejected(self) -> None:
        row = _valid_election_row()
        row["year"] = 2031
        with pytest.raises(pa_errors.SchemaError):
            elections_schema.validate(pd.DataFrame([row]))


class TestDonationsSchema:
    def test_valid_cnpj_donor(self) -> None:
        donations_schema.validate(pd.DataFrame([_valid_donation_row()]))

    def test_valid_cpf_donor(self) -> None:
        row = _valid_donation_row()
        row["donor_doc"] = "123.456.789-01"
        row["donor_is_company"] = False
        donations_schema.validate(pd.DataFrame([row]))

    def test_valid_raw_11_digit_donor(self) -> None:
        row = _valid_donation_row()
        row["donor_doc"] = "12345678901"
        row["donor_is_company"] = False
        donations_schema.validate(pd.DataFrame([row]))

    def test_malformed_donor_doc_rejected(self) -> None:
        row = _valid_donation_row()
        row["donor_doc"] = "not-a-doc"
        with pytest.raises(pa_errors.SchemaError):
            donations_schema.validate(pd.DataFrame([row]))

    def test_negative_valor_rejected(self) -> None:
        row = _valid_donation_row()
        row["valor"] = -1.0
        with pytest.raises(pa_errors.SchemaError):
            donations_schema.validate(pd.DataFrame([row]))

    def test_year_range_enforced(self) -> None:
        row = _valid_donation_row()
        row["year"] = 2031
        with pytest.raises(pa_errors.SchemaError):
            donations_schema.validate(pd.DataFrame([row]))


# ---- DOU tests -----------------------------------------------------


class TestActsSchema:
    def test_valid_row_passes(self) -> None:
        acts_schema.validate(pd.DataFrame([_valid_act_row()]))

    @pytest.mark.parametrize(
        "act_type",
        ["nomeacao", "exoneracao", "contrato", "penalidade", "outro"],
    )
    def test_all_allowed_act_types(self, act_type: str) -> None:
        row = _valid_act_row()
        row["act_type"] = act_type
        acts_schema.validate(pd.DataFrame([row]))

    def test_unknown_act_type_rejected(self) -> None:
        row = _valid_act_row()
        row["act_type"] = "banido"
        with pytest.raises(pa_errors.SchemaError):
            acts_schema.validate(pd.DataFrame([row]))

    def test_text_excerpt_length_capped(self) -> None:
        row = _valid_act_row()
        row["text_excerpt"] = "x" * 501
        with pytest.raises(pa_errors.SchemaError):
            acts_schema.validate(pd.DataFrame([row]))

    def test_text_excerpt_500_chars_accepted(self) -> None:
        row = _valid_act_row()
        row["text_excerpt"] = "y" * 500
        acts_schema.validate(pd.DataFrame([row]))

    def test_url_must_be_http(self) -> None:
        row = _valid_act_row()
        row["url"] = "ftp://server/file"
        with pytest.raises(pa_errors.SchemaError):
            acts_schema.validate(pd.DataFrame([row]))

    def test_source_enum(self) -> None:
        row = _valid_act_row()
        row["source"] = "other_source"
        with pytest.raises(pa_errors.SchemaError):
            acts_schema.validate(pd.DataFrame([row]))


class TestPersonRelsSchema:
    def test_valid_row_passes(self) -> None:
        person_rels_schema.validate(pd.DataFrame([_valid_person_rel_row()]))

    def test_cnpj_in_source_key_rejected(self) -> None:
        # Person rels must have CPF-shaped source_key; a CNPJ there is a
        # concrete bug to catch early.
        row = _valid_person_rel_row()
        row["source_key"] = "12.345.678/0001-95"
        with pytest.raises(pa_errors.SchemaError):
            person_rels_schema.validate(pd.DataFrame([row]))

    def test_empty_target_key_rejected(self) -> None:
        row = _valid_person_rel_row()
        row["target_key"] = ""
        with pytest.raises(pa_errors.SchemaError):
            person_rels_schema.validate(pd.DataFrame([row]))


class TestCompanyRelsSchema:
    def test_valid_row_passes(self) -> None:
        company_rels_schema.validate(pd.DataFrame([_valid_company_rel_row()]))

    def test_cpf_in_source_key_rejected(self) -> None:
        row = _valid_company_rel_row()
        row["source_key"] = "123.456.789-01"
        with pytest.raises(pa_errors.SchemaError):
            company_rels_schema.validate(pd.DataFrame([row]))

    def test_empty_target_key_rejected(self) -> None:
        row = _valid_company_rel_row()
        row["target_key"] = ""
        with pytest.raises(pa_errors.SchemaError):
            company_rels_schema.validate(pd.DataFrame([row]))

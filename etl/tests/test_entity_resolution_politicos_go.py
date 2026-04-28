"""Tests do pipeline ``entity_resolution_politicos_go``.

Cobre:

* helpers puros — ``_normalize_name``, ``_strip_honorifics``,
  ``_canonical_id_for``;
* happy path Senator ↔ Person por nome exato (caso Kajuru simplificado);
* happy path Fed ↔ Person com honorífico (caso Ismael Alexandrino);
* happy path CPF exact pra StateLegislator;
* ambiguidade de nome vira audit log (skip silencioso);
* shadow Person (sem CPF, sem UF) anexado conservadoramente;
* idempotência — segunda rodada não duplica edges;
* provenance — todo row carrega os 5 campos obrigatórios.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from bracc_etl.pipelines.entity_resolution_politicos_go import (
    _SOURCE_ID,
    EntityResolutionPoliticosGoPipeline,
    _canonical_id_for,
    _cargo_person_share_token,
    _cargo_tokens_subset_of_person,
    _contentful_tokens,
    _normalize_name,
    _strip_honorifics,
    _visible_cpf_suffix,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_primary_url_cache() -> Iterator[None]:
    """O ``primary_url_for`` cacheia a leitura do registry; limpa a cada test."""
    from bracc_etl.provenance import _reset_cache_for_tests

    _reset_cache_for_tests()
    yield
    _reset_cache_for_tests()


def _build_driver(
    discovery_rows: list[dict[str, Any]],
) -> tuple[MagicMock, list[tuple[str, dict[str, Any]]]]:
    """Monta driver mock que serve ``_DISCOVERY_QUERY`` e captura writes."""
    driver = MagicMock()
    session_cm = driver.session.return_value
    session = session_cm.__enter__.return_value
    calls: list[tuple[str, dict[str, Any]]] = []

    def run(query: str, params: dict[str, Any] | None = None) -> MagicMock:
        calls.append((query, params or {}))
        result = MagicMock()
        # Discovery query: devolve as rows preparadas. Tudo mais
        # (ingestion_run upsert, loader, ...) devolve vazio.
        if "UNION ALL" in query and "Senator" in query:
            result.__iter__ = lambda _self: iter(
                [_row_to_record(r) for r in discovery_rows],
            )
        else:
            result.__iter__ = lambda _self: iter([])
        return result

    session.run.side_effect = run
    return driver, calls


def _row_to_record(row: dict[str, Any]) -> MagicMock:
    """Transforma dict em record-like (suporta ``dict(record)``)."""
    record = MagicMock()
    record.keys.return_value = list(row.keys())
    record.__iter__ = lambda _self: iter(row.keys())
    record.__getitem__ = lambda _self, key: row[key]
    record.data.return_value = row
    return record


def _make_pipeline(
    discovery_rows: list[dict[str, Any]],
    tmp_path: Path,
) -> tuple[EntityResolutionPoliticosGoPipeline, MagicMock, list[tuple[str, dict[str, Any]]]]:
    driver, calls = _build_driver(discovery_rows)
    pipeline = EntityResolutionPoliticosGoPipeline(
        driver=driver,
        data_dir=str(tmp_path),
    )
    pipeline.run_id = f"{_SOURCE_ID}_20260418120000"
    return pipeline, driver, calls


def _senator(
    element_id: str, name: str, partido: str = "PSB", id_senado: str = "5895",
) -> dict[str, Any]:
    return {
        "labels": ["Senator"],
        "element_id": element_id,
        "stable_key": f"senado_{id_senado}",
        "id_senado": id_senado,
        "id_camara": None,
        "legislator_id": None,
        "sq_candidato": None,
        "name": name,
        "cpf": None,
        "partido": partido,
        "uf": "GO",
    }


def _fed(element_id: str, name: str, id_camara: str, partido: str = "PSD") -> dict[str, Any]:
    return {
        "labels": ["FederalLegislator"],
        "element_id": element_id,
        "stable_key": f"camara_{id_camara}",
        "id_senado": None,
        "id_camara": id_camara,
        "legislator_id": f"camara_{id_camara}",
        "sq_candidato": None,
        "name": name,
        "cpf": "***.***.*31-53",  # masked, vindo da API da Câmara
        "partido": partido,
        "uf": "GO",
    }


def _state(
    element_id: str,
    name: str,
    legislator_id: str = "alego_42",
    cpf: str = "111.222.333-44",
    partido: str = "PT",
) -> dict[str, Any]:
    return {
        "labels": ["StateLegislator"],
        "element_id": element_id,
        "stable_key": legislator_id,
        "id_senado": None,
        "id_camara": None,
        "legislator_id": legislator_id,
        "sq_candidato": None,
        "name": name,
        "cpf": cpf,
        "partido": partido,
        "uf": "GO",
    }


def _person(
    element_id: str,
    name: str,
    *,
    cpf: str | None = "000.000.000-00",
    partido: str | None = None,
    sq_candidato: str | None = None,
    uf: str | None = "GO",
    cargo_tse_values: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "labels": ["Person"],
        "element_id": element_id,
        "stable_key": cpf or name,
        "id_senado": None,
        "id_camara": None,
        "legislator_id": None,
        "sq_candidato": sq_candidato,
        "name": name,
        "cpf": cpf,
        "partido": partido,
        "uf": uf,
        "cargo_tse_values": cargo_tse_values or [],
    }


def _shadow(element_id: str, name: str) -> dict[str, Any]:
    """Person bare só com name — sem CPF, sem UF, sem sq_candidato."""
    return _person(element_id, name, cpf=None, uf=None)


# ---------------------------------------------------------------------------
# Helpers puros
# ---------------------------------------------------------------------------


class TestNormalizeName:
    def test_upper_sem_acento(self) -> None:
        assert _normalize_name("José Ferreira") == "JOSE FERREIRA"

    def test_colapsa_espacos(self) -> None:
        assert _normalize_name("  João  Silva  ") == "JOAO SILVA"

    def test_pontuacao_vira_espaco(self) -> None:
        assert _normalize_name("DR. JOSÉ") == "DR JOSE"

    def test_none_vira_string_vazia(self) -> None:
        assert _normalize_name(None) == ""


class TestStripHonorifics:
    def test_prefix_dr(self) -> None:
        assert _strip_honorifics("DR JOSE FERREIRA") == "JOSE FERREIRA"

    def test_prefix_cel(self) -> None:
        assert _strip_honorifics("CEL MARCOS") == "MARCOS"

    def test_suffix_junior(self) -> None:
        assert _strip_honorifics("MARCONI PERILLO JUNIOR") == "MARCONI PERILLO"

    def test_prefix_dr_ponto_ja_normalizado(self) -> None:
        # Após _normalize_name, "DR. JOSE" vira "DR JOSE" — o strip
        # tolera ambos os casos (conjunto tem "DR" e "DR.").
        assert _strip_honorifics("DR JOSE") == "JOSE"

    def test_nada_pra_stripar(self) -> None:
        assert _strip_honorifics("JORGE KAJURU") == "JORGE KAJURU"

    def test_alexandrino_case(self) -> None:
        # Fed: "DR. ISMAEL ALEXANDRINO" → stripped.
        # Person TSE: "ISMAEL ALEXANDRINO JUNIOR" → stripped.
        # Ambos colapsam no mesmo valor.
        fed = _strip_honorifics(_normalize_name("DR. ISMAEL ALEXANDRINO"))
        tse = _strip_honorifics(_normalize_name("ISMAEL ALEXANDRINO JUNIOR"))
        assert fed == tse == "ISMAEL ALEXANDRINO"


class TestVisibleCpfSuffix:
    def test_mascarado_camara(self) -> None:
        assert _visible_cpf_suffix("***.***.*71-34") == "7134"

    def test_pleno(self) -> None:
        assert _visible_cpf_suffix("547.795.371-34") == "7134"

    def test_none_vira_string_vazia(self) -> None:
        assert _visible_cpf_suffix(None) == ""

    def test_muito_curto(self) -> None:
        assert _visible_cpf_suffix("12") == ""


class TestContentfulTokens:
    def test_descarta_stopwords(self) -> None:
        assert _contentful_tokens("JOAO DA SILVA") == ["JOAO", "SILVA"]

    def test_descarta_honorificos(self) -> None:
        # "DR" e "JUNIOR" são dropados.
        assert _contentful_tokens("DR JOAO DA SILVA JUNIOR") == ["JOAO", "SILVA"]

    def test_descarta_tokens_curtos(self) -> None:
        # "E" filtrado por stopword; "DA" idem; "OS" pelo tamanho.
        assert _contentful_tokens("MARIA OS E JOSE") == ["MARIA", "JOSE"]


class TestCargoTokensSubset:
    def test_subset_exato_passa(self) -> None:
        assert _cargo_tokens_subset_of_person("FLAVIA MORAIS", "FLAVIA MORAIS")

    def test_person_estendido_passa(self) -> None:
        assert _cargo_tokens_subset_of_person(
            "FLAVIA MORAIS", "FLAVIA CARREIRO ALBUQUERQUE MORAIS",
        )

    def test_nome_completamente_diferente_falha(self) -> None:
        assert not _cargo_tokens_subset_of_person(
            "CELIO SILVEIRA", "WEBER TIAGO PIRES",
        )

    def test_falta_um_token_falha(self) -> None:
        # "SILVEIRA" não aparece no nome do Person.
        assert not _cargo_tokens_subset_of_person(
            "CELIO SILVEIRA", "CELIO ANTONIO DOS SANTOS",
        )

    def test_cargo_so_com_stopwords_falha(self) -> None:
        # Sem tokens contentfuls no cargo → não dá pra validar.
        assert not _cargo_tokens_subset_of_person("DE DA", "DE DA JOAO")


class TestCargoPersonShareToken:
    def test_um_token_em_comum_passa(self) -> None:
        # Caso ADRIANO DO BALDY ↔ ADRIANO ANTONIO AVELAR — nome de
        # campanha reescrito mas "ADRIANO" sobrevive nos dois.
        assert _cargo_person_share_token(
            "ADRIANO DO BALDY", "ADRIANO ANTONIO AVELAR",
        )

    def test_zero_tokens_em_comum_falha(self) -> None:
        # Caso GLAUSTIN DA FOKUS ↔ GLAUSKSTON BATISTA RIOS — nada
        # bate exato (apesar do CPF + cargo serem o mesmo).
        assert not _cargo_person_share_token(
            "GLAUSTIN DA FOKUS", "GLAUSKSTON BATISTA RIOS",
        )

    def test_so_stopwords_em_comum_falha(self) -> None:
        # "DE" / "DA" não contam — _contentful_tokens descarta.
        assert not _cargo_person_share_token(
            "JOAO DE SOUZA", "MARIA DA SILVA",
        )

    def test_cargo_vazio_falha(self) -> None:
        assert not _cargo_person_share_token("", "ALCIDES RIBEIRO")

    def test_person_vazio_falha(self) -> None:
        assert not _cargo_person_share_token("ALCIDES RIBEIRO", "")


class TestCanonicalId:
    def test_senado(self) -> None:
        node = {"id_senado": "5895"}
        assert _canonical_id_for("Senator", node) == "canon_senado_5895"

    def test_camara(self) -> None:
        node = {"id_camara": "204372"}
        assert _canonical_id_for("FederalLegislator", node) == "canon_camara_204372"

    def test_alego(self) -> None:
        node = {"legislator_id": "alego_42"}
        assert _canonical_id_for("StateLegislator", node) == "canon_alego_42"

    def test_person_cpf(self) -> None:
        node = {"cpf": "218.405.711-87"}
        assert _canonical_id_for("Person", node) == "canon_cpf_21840571187"

    def test_person_sem_cpf_levanta(self) -> None:
        with pytest.raises(ValueError):
            _canonical_id_for("Person", {"cpf": None})

    def test_person_cpf_zero_levanta(self) -> None:
        # CPF "000.000.000-00" é placeholder, não serve como chave.
        with pytest.raises(ValueError):
            _canonical_id_for("Person", {"cpf": "000.000.000-00"})


# ---------------------------------------------------------------------------
# Pipeline end-to-end (driver mockado)
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_senator_matcha_person_por_nome_exato(self, tmp_path: Path) -> None:
        # Caso Kajuru simplificado: Senator + 1 Person(GO, com CPF) +
        # 1 shadow Person (só name). Todos no mesmo cluster.
        rows = [
            _senator("n1", "JORGE KAJURU REIS DA COSTA NASSER", partido="PSB", id_senado="5895"),
            _person(
                "n2",
                "JORGE KAJURU REIS DA COSTA NASSER",
                cpf="218.405.711-87",
                partido="PRP",  # PRP de 2014 — cluster deve preferir o PSB do Senator
                sq_candidato="90000613472",
                uf="GO",
            ),
            _shadow("n3", "JORGE KAJURU REIS DA COSTA NASSER"),
        ]
        pipeline, driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        assert len(pipeline.canonical_rows) == 1
        canonical = pipeline.canonical_rows[0]
        assert canonical["canonical_id"] == "canon_senado_5895"
        assert canonical["display_name"] == "JORGE KAJURU REIS DA COSTA NASSER"
        assert canonical["cargo_ativo"] == "senador"
        assert canonical["partido"] == "PSB"
        assert canonical["num_sources"] == 3

        # Edges: 1 cargo_root (Senator) + 1 name_exact (Person) + 1
        # shadow_name_exact.
        edges = pipeline.represents_rels
        assert len(edges) == 3
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "name_exact", "shadow_name_exact"]

    def test_fed_matcha_person_stripped_honorific(self, tmp_path: Path) -> None:
        # Ismael Alexandrino: Fed "DR. ISMAEL ALEXANDRINO" ↔ Person TSE
        # "ISMAEL ALEXANDRINO JUNIOR". Resolução via name_stripped.
        rows = [
            _fed("n1", "DR. ISMAEL ALEXANDRINO", id_camara="204378", partido="PSD"),
            _person(
                "n2",
                "ISMAEL ALEXANDRINO JUNIOR",
                cpf="702.251.501-82",
                partido="PSD",
                sq_candidato="250000600000",
                uf="GO",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        assert len(pipeline.canonical_rows) == 1
        assert pipeline.canonical_rows[0]["canonical_id"] == "canon_camara_204378"
        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "name_stripped"]
        # Confidence no stripped é 0.85.
        stripped_edge = next(e for e in edges if e["method"] == "name_stripped")
        assert stripped_edge["confidence"] == pytest.approx(0.85)

    def test_fed_matcha_person_por_cpf_suffix_name(self, tmp_path: Path) -> None:
        # Caso Flavia Morais: Fed "FLAVIA MORAIS" (CPF mascarado) +
        # Person TSE "FLAVIA CARREIRO ALBUQUERQUE MORAIS" (CPF pleno).
        # name_exact falha (nomes diferentes); cpf_exact pula (mascarado).
        # A fase cpf_suffix_name casa pelos 4 últimos dígitos + tokens.
        fed = _fed("n1", "FLAVIA MORAIS", id_camara="160598", partido="PDT")
        fed["cpf"] = "***.***.*71-34"
        rows = [
            fed,
            _person(
                "n2",
                "FLAVIA CARREIRO ALBUQUERQUE MORAIS",
                cpf="547.795.371-34",  # suffix 7134 bate com *71-34
                partido="PDT",
                uf="GO",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "cpf_suffix_name"]
        suffix_edge = next(e for e in edges if e["method"] == "cpf_suffix_name")
        assert suffix_edge["confidence"] == pytest.approx(0.92)
        assert suffix_edge["target_element_id"] == "n2"

    def test_fed_cpf_suffix_nao_casa_quando_nomes_divergem(self, tmp_path: Path) -> None:
        # Pessoa completamente diferente que casualmente tem CPF com
        # mesmo suffix. Tokens do cargo ausentes → não casa.
        fed = _fed("n1", "CELIO SILVEIRA", id_camara="178876", partido="MDB")
        fed["cpf"] = "***.***.*61-20"
        rows = [
            fed,
            _person(
                "n2",
                "WEBER TIAGO PIRES",  # suffix bate (957.509.161-20)
                cpf="957.509.161-20",
                uf="GO",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        # Só o cargo_root — sem match no suffix rule por nome divergente.
        assert methods == ["cargo_root"]

    def test_fed_cpf_suffix_ambiguidade_vira_audit(self, tmp_path: Path) -> None:
        # 2 Persons GO com suffix batendo E tokens do nome do cargo em
        # ambos (improvável mas possível com nomes curtos) → audit + skip.
        fed = _fed("n1", "JOAO SILVA", id_camara="42", partido="PT")
        fed["cpf"] = "***.***.*11-11"
        rows = [
            fed,
            _person(
                "n2",
                "JOAO SILVA PEREIRA",
                cpf="111.222.311-11",  # suffix 1111
                uf="GO",
            ),
            _person(
                "n3",
                "JOAO SILVA JUNIOR",
                cpf="999.888.711-11",  # suffix 1111
                uf="GO",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        # Nada anexado — ambiguidade logada.
        assert "cpf_suffix_name" not in methods
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        assert any(e["type"] == "cpf_suffix_ambiguous" for e in entries)

    def test_fed_cpf_suffix_anexa_alem_do_name_exact(self, tmp_path: Path) -> None:
        # Caso real: cluster já tem 1 Person via name_exact (nome curto
        # "FLAVIA MORAIS" sem CPF). A fase 3 DEVE anexar o Person TSE
        # full-name com CPF adicional — múltiplos Persons por cluster.
        fed = _fed("n1", "FLAVIA MORAIS", id_camara="160598", partido="PDT")
        fed["cpf"] = "***.***.*71-34"
        rows = [
            fed,
            # Person curto sem CPF: casa pelo name_exact.
            _person("n2", "FLAVIA MORAIS", cpf=None, uf="GO"),
            # Person TSE pleno: só a fase 3 (cpf_suffix_name) pega.
            _person(
                "n3",
                "FLAVIA CARREIRO ALBUQUERQUE MORAIS",
                cpf="547.795.371-34",
                partido="PDT",
                uf="GO",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = sorted(e["method"] for e in pipeline.represents_rels)
        assert methods == ["cargo_root", "cpf_suffix_name", "name_exact"]
        targets = sorted(
            e["target_element_id"] for e in pipeline.represents_rels
            if e["method"] != "cargo_root"
        )
        assert targets == ["n2", "n3"]

    def test_fed_cpf_suffix_token_overlap_casa_alcides(
        self, tmp_path: Path,
    ) -> None:
        # Caso PROFESSOR ALCIDES: Fed "PROFESSOR ALCIDES" (CPF mascarado)
        # + Person TSE "ALCIDES RIBEIRO FILHO" com cargo_tse_2022=
        # "Deputado Federal". cpf_suffix_name falha porque "PROFESSOR"
        # não está no Person (honorífico não-padrão), mas o token
        # "ALCIDES" é comum aos dois. Fase 3.5 pega: suffix +
        # cargo_tse + ≥1 token comum + match único.
        fed = _fed("n1", "PROFESSOR ALCIDES", id_camara="204390", partido="PSDB")
        fed["cpf"] = "***.***.*31-49"
        rows = [
            fed,
            _person(
                "n2",
                "ALCIDES RIBEIRO FILHO",
                cpf="092.426.431-49",
                uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "cpf_suffix_token_overlap"]
        suffix_edge = next(e for e in edges if e["method"] == "cpf_suffix_token_overlap")
        assert suffix_edge["confidence"] == pytest.approx(0.88)
        assert suffix_edge["target_element_id"] == "n2"

    def test_fed_cpf_suffix_token_overlap_resolve_adriano_do_baldy(
        self, tmp_path: Path,
    ) -> None:
        # Caso real ADRIANO DO BALDY: 3 candidatos Deputado Federal GO
        # com sufixo 3153, mas só ADRIANO ANTONIO AVELAR compartilha o
        # token "ADRIANO" com o cargo. Fase 3 strict falha (token
        # "BALDY" não aparece em nenhum), fase 4 ambígua (3 candidatos
        # com cargo_tse=Federal). Fase 3.5 desambigua via overlap único.
        fed = _fed("n1", "ADRIANO DO BALDY", id_camara="121948", partido="PP")
        fed["cpf"] = "***.***.*31-53"
        rows = [
            fed,
            _person(
                "n2", "ADRIANO ANTONIO AVELAR",
                cpf="507.465.531-53", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n3", "DANNILLO DA CUNHA PEREIRA",
                cpf="597.308.031-53", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n4", "ELAINE RODRIGUES DE SOUZA",
                cpf="924.155.631-53", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "cpf_suffix_token_overlap"]
        suffix_edge = next(e for e in edges if e["method"] == "cpf_suffix_token_overlap")
        assert suffix_edge["target_element_id"] == "n2"

    def test_fed_cpf_suffix_token_overlap_resolve_zacharias(
        self, tmp_path: Path,
    ) -> None:
        # Caso real DR. ZACHARIAS CALIL ↔ ZACARIAS CALIL HAMU: grafia
        # divergente ZH↔Z derruba "ZACHARIAS" do match exato, mas
        # "CALIL" é token comum. 4 candidatos Deputado Federal sufixo
        # 0100 — só ZACARIAS HAMU compartilha um token.
        fed = _fed("n1", "DR. ZACHARIAS CALIL", id_camara="204412", partido="UNIÃO")
        fed["cpf"] = "***.***.*01-00"
        rows = [
            fed,
            _person(
                "n2", "ZACARIAS CALIL HAMU",
                cpf="118.330.501-00", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n3", "CARLOS ANTONIO DE SOUSA COSTA",
                cpf="247.784.001-00", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n4", "RODNEY ROCHA MIRANDA",
                cpf="317.252.101-00", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n5", "EDILSON CHAVES DE ARAUJO",
                cpf="850.449.201-00", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "cpf_suffix_token_overlap"]
        suffix_edge = next(e for e in edges if e["method"] == "cpf_suffix_token_overlap")
        assert suffix_edge["target_element_id"] == "n2"

    def test_fed_cpf_suffix_token_overlap_ambiguidade_vira_audit(
        self, tmp_path: Path,
    ) -> None:
        # 2 Persons Deputado Federal sufixo igual e ambos compartilham
        # ≥1 token contentful com o cargo → audit, sem attach.
        fed = _fed("n1", "JOAO SILVA", id_camara="42", partido="PT")
        fed["cpf"] = "***.***.*11-11"
        rows = [
            fed,
            _person(
                "n2", "JOAO PEREIRA SANTOS",
                cpf="111.222.311-11", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n3", "SILVA ALMEIDA RIBEIRO",
                cpf="999.888.711-11", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        assert "cpf_suffix_token_overlap" not in methods
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        overlap_audits = [
            e for e in entries
            if e["type"] == "cpf_suffix_token_overlap_ambiguous"
        ]
        assert len(overlap_audits) == 1
        assert overlap_audits[0]["cpf_suffix"] == "1111"
        assert len(overlap_audits[0]["candidates"]) == 2

    def test_fed_cpf_suffix_token_overlap_skipa_quando_strict_resolve(
        self, tmp_path: Path,
    ) -> None:
        # Regressão zero: quando fase 3 strict (cpf_suffix_name) já
        # casa, fase 3.5 não deve roubar o Person nem disparar pra
        # outro candidato. Person fica claimed pela fase 3.
        fed = _fed("n1", "FLAVIA MORAIS", id_camara="160598", partido="PDT")
        fed["cpf"] = "***.***.*71-34"
        rows = [
            fed,
            _person(
                "n2",
                "FLAVIA CARREIRO ALBUQUERQUE MORAIS",
                cpf="547.795.371-34", partido="PDT", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = sorted(e["method"] for e in pipeline.represents_rels)
        assert methods == ["cargo_root", "cpf_suffix_name"]
        assert "cpf_suffix_token_overlap" not in methods

    def test_fed_cpf_suffix_token_overlap_ignora_person_de_cargo_diferente(
        self, tmp_path: Path,
    ) -> None:
        # Person GO Deputado Estadual com sufixo igual + token comum
        # "ALCIDES" não deve anexar a um :FederalLegislator pela fase
        # 3.5. Filtro cargo_tse_set bloqueia.
        fed = _fed("n1", "PROFESSOR ALCIDES", id_camara="204390", partido="PSDB")
        fed["cpf"] = "***.***.*31-49"
        rows = [
            fed,
            _person(
                "n2",
                "ALCIDES RIBEIRO FILHO",
                cpf="092.426.431-49",
                uf="GO",
                cargo_tse_values=["Deputado Estadual"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        assert "cpf_suffix_token_overlap" not in methods
        assert methods == ["cargo_root"]

    def test_fed_cpf_suffix_token_overlap_ignora_person_sem_cargo_tse(
        self, tmp_path: Path,
    ) -> None:
        # Person GO sem cargo_tse_* (veio por outra via — receitas, bens
        # genéricos, etc.) com sufixo + token comum não basta. Fase 3.5
        # exige cargo_tse pra filtrar candidatos do mesmo nível.
        fed = _fed("n1", "PROFESSOR ALCIDES", id_camara="204390", partido="PSDB")
        fed["cpf"] = "***.***.*31-49"
        rows = [
            fed,
            _person(
                "n2",
                "ALCIDES RIBEIRO FILHO",
                cpf="092.426.431-49",
                uf="GO",
                cargo_tse_values=[],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        assert "cpf_suffix_token_overlap" not in methods

    def test_fed_cpf_suffix_token_overlap_roda_mesmo_com_shadow_sem_cpf(
        self, tmp_path: Path,
    ) -> None:
        # Caso PROFESSOR ALCIDES com Person shadow uf=GO sem CPF
        # matched via name_exact em fase 1: o cluster ainda é elegível
        # pra fase 3.5 (Person sem CPF não conta como "evidência forte
        # de identidade"). Anexa o Person TSE com CPF pleno.
        fed = _fed("n1", "PROFESSOR ALCIDES", id_camara="204390", partido="PSDB")
        fed["cpf"] = "***.***.*31-49"
        rows = [
            fed,
            _person("n2", "PROFESSOR ALCIDES", cpf=None, uf="GO"),
            _person(
                "n3",
                "ALCIDES RIBEIRO FILHO",
                cpf="092.426.431-49",
                uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "cpf_suffix_token_overlap", "name_exact"]
        targets = {e["target_element_id"] for e in edges if e["method"] != "cargo_root"}
        assert targets == {"n2", "n3"}

    def test_fed_cpf_suffix_cargo_skipa_cluster_nao_orfao(
        self, tmp_path: Path,
    ) -> None:
        # Regressão real 2026-04-23: RUBENS OTONI (Fed, suffix 7149)
        # pegou RUBENS OTONI GOMIDE via cpf_suffix_name. Sem a trava de
        # "só roda em órfão", fase 4 anexava GLEICY MARIA (suffix 7149
        # + cargo Deputado Federal, outra pessoa) como "único
        # não-claimed". Agora o cluster não-órfão é pulado inteiro.
        fed = _fed("n1", "RUBENS OTONI", id_camara="74371", partido="PT")
        fed["cpf"] = "***.***.*71-49"
        rows = [
            fed,
            # Person TSE correto: casa por cpf_suffix_name (tokens
            # "RUBENS" + "OTONI" presentes).
            _person(
                "n2",
                "RUBENS OTONI GOMIDE",
                cpf="133.347.271-49",
                partido="PT",
                uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            # Colisão de suffix: outro candidato Deputado Federal GO
            # com 4 últimos dígitos iguais. Sem a trava, seria
            # anexado como falso positivo.
            _person(
                "n3",
                "GLEICY MARIA BARBOSA DOS SANTOS GUERRA",
                cpf="449.778.671-49",
                uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        # cargo_root + cpf_suffix_name apenas. cpf_suffix_cargo não
        # pode aparecer — cluster já tem Person anexado e fase 4 pula.
        assert methods == ["cargo_root", "cpf_suffix_name"]
        attached = {e["target_element_id"] for e in edges}
        assert "n3" not in attached

    def test_fed_cpf_suffix_cargo_ambiguidade_vira_audit(
        self, tmp_path: Path,
    ) -> None:
        # Múltiplos Persons TSE Deputado Federal com mesmo sufixo e
        # nenhum token contentful compartilhado com o cargo (caso
        # "GLAUSTIN DA FOKUS"-like onde nem fase 3 nem 3.5 resolvem) →
        # cai na fase 4, que vê 3 candidatos com cargo igual e manda
        # audit.
        fed = _fed("n1", "GLAUSTIN DA FOKUS", id_camara="204419", partido="PSC")
        fed["cpf"] = "***.***.*61-91"
        rows = [
            fed,
            _person(
                "n2", "CARLOS ANTONIO DE SOUSA COSTA",
                cpf="247.784.061-91", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n3", "RODNEY ROCHA MIRANDA",
                cpf="317.252.161-91", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
            _person(
                "n4", "EDILSON CHAVES DE ARAUJO",
                cpf="850.449.261-91", uf="GO",
                cargo_tse_values=["Deputado Federal"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        assert "cpf_suffix_cargo" not in methods
        assert "cpf_suffix_token_overlap" not in methods
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        suffix_cargo_audits = [
            e for e in entries if e["type"] == "cpf_suffix_cargo_ambiguous"
        ]
        assert len(suffix_cargo_audits) == 1
        assert suffix_cargo_audits[0]["cpf_suffix"] == "6191"
        assert suffix_cargo_audits[0]["expected_cargo_tse"] == "DEPUTADO FEDERAL"
        assert len(suffix_cargo_audits[0]["candidates"]) == 3

    def test_fed_cpf_suffix_cargo_ignora_person_de_cargo_diferente(
        self, tmp_path: Path,
    ) -> None:
        # Person GO tem suffix batendo mas cargo_tse_2022 é "Deputado
        # Estadual" — fase 4 não deve anexar a um :FederalLegislator.
        fed = _fed("n1", "PROFESSOR ALCIDES", id_camara="204390", partido="PSDB")
        fed["cpf"] = "***.***.*31-49"
        rows = [
            fed,
            _person(
                "n2",
                "ALCIDES RIBEIRO FILHO",
                cpf="092.426.431-49",
                uf="GO",
                cargo_tse_values=["Deputado Estadual"],  # nível errado
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        assert methods == ["cargo_root"]

    def test_fed_cpf_suffix_cargo_ignora_person_sem_cargo_tse(
        self, tmp_path: Path,
    ) -> None:
        # Person GO sem nenhum cargo_tse_* (veio por outra via, ex.:
        # camara_politicos_go, wikidata) não é candidato da fase 4 mesmo
        # se suffix bate.
        fed = _fed("n1", "PROFESSOR ALCIDES", id_camara="204390", partido="PSDB")
        fed["cpf"] = "***.***.*31-49"
        rows = [
            fed,
            _person(
                "n2",
                "ALCIDES RIBEIRO FILHO",
                cpf="092.426.431-49",
                uf="GO",
                cargo_tse_values=[],  # não é candidato TSE conhecido
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = [e["method"] for e in pipeline.represents_rels]
        assert methods == ["cargo_root"]

    def test_state_matcha_person_por_cpf(self, tmp_path: Path) -> None:
        # StateLegislator tem CPF pleno (pipeline alego não mascara).
        rows = [
            _state("n1", "MAURO RUBEM", cpf="111.222.333-44", partido="PT"),
            _person(
                "n2",
                "MAURO RUBEM PEREIRA",  # nome levemente diferente
                cpf="111.222.333-44",  # bate por CPF
                uf="GO",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        edges = pipeline.represents_rels
        methods = sorted(e["method"] for e in edges)
        assert methods == ["cargo_root", "cpf_exact"]
        cpf_edge = next(e for e in edges if e["method"] == "cpf_exact")
        assert cpf_edge["confidence"] == 1.0


class TestAmbiguity:
    def test_dois_persons_com_mesmo_nome_sem_partido_unico_vira_audit(self, tmp_path: Path) -> None:
        rows = [
            _fed("n1", "JOAO SILVA", id_camara="999999", partido="MDB"),
            # 2 Persons com mesmo nome, ambos sem partido → ambíguo.
            _person("n2", "JOAO SILVA", cpf="111.111.111-11", uf="GO"),
            _person("n3", "JOAO SILVA", cpf="222.222.222-22", uf="GO"),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        # Cluster do Fed existe, mas só tem o cargo_root.
        assert len(pipeline.canonical_rows) == 1
        methods = [e["method"] for e in pipeline.represents_rels]
        assert methods == ["cargo_root"]
        # Audit log registra a ambiguidade.
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        assert len(audit_files) == 1
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        assert any(e["type"] == "cargo_name_ambiguous" for e in entries)

    def test_ambiguidade_desambiguada_por_partido(self, tmp_path: Path) -> None:
        rows = [
            _fed("n1", "JOAO SILVA", id_camara="1001", partido="MDB"),
            _person("n2", "JOAO SILVA", cpf="111.111.111-11", uf="GO", partido="PT"),
            _person("n3", "JOAO SILVA", cpf="222.222.222-22", uf="GO", partido="MDB"),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        methods = sorted(e["method"] for e in pipeline.represents_rels)
        assert methods == ["cargo_root", "name_exact_partido"]
        # O Person do MDB é o escolhido.
        matched = next(e for e in pipeline.represents_rels if e["method"] == "name_exact_partido")
        assert matched["target_element_id"] == "n3"

    def test_senador_sem_cpf_multiplos_persons_mesmo_partido_anexa_todos(
        self, tmp_path: Path,
    ) -> None:
        # Caso Vanderlan: Senator PSD/GO sem CPF + 2 Persons homônimos
        # PSD/GO (registros TSE de anos diferentes). name_exact pega
        # ambos → disambiguate_by_partido retorna None (ambos PSD) →
        # cairia no audit. Nova fase ``name_partido_multi`` anexa os
        # dois sob a premissa de que homonimia real com partido+UF
        # idênticos é virtualmente zero.
        rows = [
            _senator(
                "n1", "VANDERLAN VIEIRA CARDOSO", partido="PSD",
                id_senado="5899",
            ),
            _person(
                "n2", "VANDERLAN VIEIRA CARDOSO",
                cpf="144.649.692-91", partido="PSD", uf="GO",
            ),
            _person(
                "n3", "VANDERLAN VIEIRA CARDOSO",
                cpf=None, partido="PSD", uf="GO",
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        assert len(pipeline.canonical_rows) == 1
        canonical = pipeline.canonical_rows[0]
        assert canonical["canonical_id"] == "canon_senado_5899"
        assert canonical["num_sources"] == 3

        methods = sorted(e["method"] for e in pipeline.represents_rels)
        assert methods == [
            "cargo_root", "name_partido_multi", "name_partido_multi",
        ]
        # Confidence 0.78 (abaixo do name_exact=0.95).
        multi_edges = [
            e for e in pipeline.represents_rels
            if e["method"] == "name_partido_multi"
        ]
        for edge in multi_edges:
            assert edge["confidence"] == pytest.approx(0.78)
        # Ambos Persons foram anexados.
        attached_ids = {e["target_element_id"] for e in multi_edges}
        assert attached_ids == {"n2", "n3"}

    def test_sentinel_sq_cpf_nao_polui_indice_cpf(self, tmp_path: Path) -> None:
        # Person com cpf='sq:90002105951' NÃO entra em persons_by_cpf
        # (sem isso, virava o CPF fake '90002105951' e poderia colidir
        # com cargo real). Validado indiretamente: Fed com CPF pleno
        # cujos 4 últimos dígitos batem com o sq não puxa o sentinel
        # pelo path cpf_suffix_*.
        fed = _fed("n1", "DIFFERENT PERSON", id_camara="9999", partido="PT")
        fed["cpf"] = "***.***.*51-00"  # suffix 5100
        rows = [
            fed,
            _person(
                "n2", "ALGUEM COM SQ",
                cpf="sq:12345675100",  # suffix 5100 casualmente
                partido="PT", uf="GO",
                cargo_tse_values=["DEPUTADO FEDERAL"],
            ),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()
        # Só o cargo_root — sentinel sq não entrou em nenhum índice CPF.
        methods = [e["method"] for e in pipeline.represents_rels]
        assert methods == ["cargo_root"]

    def test_shadow_com_multiplos_clusters_vira_audit(self, tmp_path: Path) -> None:
        # 2 Feds com mesmo nome (improvável mas garante que shadow
        # não "chuta" entre eles).
        rows = [
            _fed("n1", "CICLANO DE TAL", id_camara="1", partido="PT"),
            _fed("n2", "CICLANO DE TAL", id_camara="2", partido="PL"),
            _person("n3", "CICLANO DE TAL", cpf="111.111.111-11", uf="GO", partido="PT"),
            _person("n4", "CICLANO DE TAL", cpf="222.222.222-22", uf="GO", partido="PL"),
            _shadow("n5", "CICLANO DE TAL"),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        # Shadow não entra em nenhum cluster.
        assert not any(
            e["method"] == "shadow_name_exact" for e in pipeline.represents_rels
        )
        audit_files = list((tmp_path / _SOURCE_ID).glob("audit_*.jsonl"))
        entries = [
            json.loads(line)
            for line in audit_files[0].read_text(encoding="utf-8").splitlines()
        ]
        assert any(e["type"] == "shadow_ambiguous" for e in entries)


class TestProvenance:
    def test_todo_canonical_tem_provenance(self, tmp_path: Path) -> None:
        rows = [_senator("n1", "JORGE KAJURU", id_senado="5895")]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        canonical = pipeline.canonical_rows[0]
        for field in ("source_id", "source_url", "ingested_at", "run_id"):
            assert canonical.get(field), f"canonical missing {field}"
        assert canonical["source_id"] == _SOURCE_ID
        assert canonical["source_url"].startswith("http")

    def test_todo_edge_tem_provenance(self, tmp_path: Path) -> None:
        rows = [
            _senator("n1", "JORGE KAJURU", id_senado="5895"),
            _person("n2", "JORGE KAJURU", cpf="218.405.711-87", uf="GO"),
        ]
        pipeline, _driver, _calls = _make_pipeline(rows, tmp_path)
        pipeline.run()

        for edge in pipeline.represents_rels:
            for field in ("source_id", "source_url", "ingested_at", "run_id"):
                assert edge.get(field), f"edge missing {field}"
            assert "_source_name_norm" not in edge  # campo de trabalho limpo


class TestIdempotence:
    def test_cluster_id_estavel_entre_runs(self, tmp_path: Path) -> None:
        """Chamar extract()+transform() duas vezes produz mesmos canonical_ids.

        O pipeline não deveria depender de ordem ou run_id do canonical_id.
        """
        rows = [
            _senator("n1", "JORGE KAJURU", id_senado="5895"),
            _person("n2", "JORGE KAJURU", cpf="218.405.711-87", uf="GO"),
        ]
        pipeline_a, _, _ = _make_pipeline(rows, tmp_path)
        pipeline_a.extract()
        pipeline_a.transform()
        ids_a = {c["canonical_id"] for c in pipeline_a.canonical_rows}

        pipeline_b, _, _ = _make_pipeline(rows, tmp_path / "b")
        pipeline_b.run_id = f"{_SOURCE_ID}_20260619090000"  # run_id diferente
        pipeline_b.extract()
        pipeline_b.transform()
        ids_b = {c["canonical_id"] for c in pipeline_b.canonical_rows}

        assert ids_a == ids_b == {"canon_senado_5895"}


class TestMetadata:
    def test_name_source_id(self) -> None:
        # Fios simples: não deixam o pipeline acidentalmente renomeado
        # quebrar config/bootstrap_all_contract.yml sem um refactor
        # intencional.
        assert EntityResolutionPoliticosGoPipeline.name == "entity_resolution_politicos_go"
        assert EntityResolutionPoliticosGoPipeline.source_id == "entity_resolution_politicos_go"

    def test_runner_registra_pipeline(self) -> None:
        from bracc_etl.runner import PIPELINES

        assert "entity_resolution_politicos_go" in PIPELINES
        assert PIPELINES["entity_resolution_politicos_go"] is EntityResolutionPoliticosGoPipeline

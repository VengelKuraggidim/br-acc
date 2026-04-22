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

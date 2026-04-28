"""Tests for the TCM-GO sanctions scaffold pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest

from bracc_etl.archival import restore_snapshot
from bracc_etl.pipelines.tcmgo_sancoes import (
    TcmgoSancoesPipeline,
    _build_ajax_payload,
    _extract_viewstate,
    _normalize_csv_header,
    _normalize_lai_impedidos_headers,
    _parse_jsf_table,
    _parse_partial_response,
    fetch_impedidos_jsf,
    fetch_to_disk,
)
from tests._mock_helpers import mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TcmgoSancoesPipeline:
    # ``archive_online=False`` pra fixture offline: evita hit network no
    # endpoint público do TCM-GO durante teste unitário rodado local.
    # O caminho com archival online é coberto em ``TestArchivalRetrofit``
    # abaixo, onde ``httpx.Client`` é monkeypatched com ``MockTransport``.
    return TcmgoSancoesPipeline(
        driver=MagicMock(), data_dir=str(FIXTURES), archive_online=False,
    )


class TestMetadata:
    def test_name(self) -> None:
        assert TcmgoSancoesPipeline.name == "tcmgo_sancoes"

    def test_source_id(self) -> None:
        assert TcmgoSancoesPipeline.source_id == "tcmgo_sancoes"


class TestTransform:
    def test_impedidos_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 3 do CSV REST (contas-irregulares: 1 CNPJ + 1 CPF cru + 1 CPF
        # pre-mascarado) + 3 do CSV JSF (impedidos_licitar: 2 CNPJ + 1 CPF
        # pre-mascarado).
        assert len(pipeline.impedidos) == 6

    def test_rejected_accounts_count(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.rejected_accounts) == 1

    def test_cnpj_and_cpf_distinguished(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        kinds = {r["document_kind"] for r in pipeline.impedidos}
        assert kinds == {"CNPJ", "CPF"}

    def test_list_kind_carimbado_nos_dois_fluxos(self) -> None:
        """Impedidos do CSV REST ganham list_kind='contas_irregulares';
        do CSV JSF ganham list_kind='impedidos_licitar' — permite query
        de API distinguir as duas populacoes."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        kinds = {r["list_kind"] for r in pipeline.impedidos}
        assert kinds == {"contas_irregulares", "impedidos_licitar"}
        contas = [r for r in pipeline.impedidos if r["list_kind"] == "contas_irregulares"]
        impedidos_jsf = [r for r in pipeline.impedidos if r["list_kind"] == "impedidos_licitar"]
        assert len(contas) == 3
        assert len(impedidos_jsf) == 3

    def test_cpf_masked(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        cpfs = [
            r["document"] for r in pipeline.impedidos
            if r["document_kind"] == "CPF"
        ]
        assert all("***" in c for c in cpfs)

    def test_premasked_cpf_classified_as_cpf(self) -> None:
        """Upstream TCM-GO entrega CPFs ja mascarados (``NN***.***-***``) —
        pipeline precisa reconhecer esse shape e carimbar ``kind=CPF`` +
        preservar a mascara. Sem isso, as 1422 rows de producao caem em
        ``kind=""`` e quebram a validation query documentada no TODO 03.
        """
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        premasked = [
            r for r in pipeline.impedidos
            if r["name"] == "RESPONSAVEL PRE MASCARADO"
        ]
        assert len(premasked) == 1
        assert premasked[0]["document_kind"] == "CPF"
        assert premasked[0]["document"] == "76***.***-***"

    def test_impedido_rels_only_for_cnpj(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        # 1 rel do CSV REST (1 CNPJ) + 2 rels do CSV JSF (2 CNPJs — linhas 1
        # e 3 da fixture impedidos_licitar.csv).
        assert len(pipeline.impedido_rels) == 3
        # list_kind fica gravado na rel tambem pra permitir query filtrar.
        kinds = {r.get("list_kind") for r in pipeline.impedido_rels}
        assert kinds == {"contas_irregulares", "impedidos_licitar"}

    def test_uf_and_source(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.impedidos + pipeline.rejected_accounts:
            assert r["uf"] == "GO"
            assert r["source"] == "tcmgo_sancoes"

    def test_provenance_stamped_on_impedidos(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        assert pipeline.impedidos
        for r in pipeline.impedidos:
            assert r["source_id"] == "tcmgo_sancoes"
            # document|processo composite.
            assert "|" in r["source_record_id"]
            assert r["source_url"].startswith("http")
            assert r["ingested_at"].startswith("20")
            assert r["run_id"].startswith("tcmgo_sancoes_")
        for rel in pipeline.impedido_rels:
            assert rel["source_id"] == "tcmgo_sancoes"
            assert "|" in rel["source_record_id"]

    def test_provenance_stamped_on_rejected_accounts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        for r in pipeline.rejected_accounts:
            assert r["source_id"] == "tcmgo_sancoes"
            # cod_ibge|exercicio|processo composite.
            assert r["source_record_id"].count("|") == 2
            assert r["source_url"].startswith("http")


class TestLoad:
    def test_load_runs(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        session = mock_session(pipeline)
        assert session.run.call_count > 0


# ---------------------------------------------------------------------------
# Archival — snapshot do CSV de contas-irregulares no momento do fetch
# (retrofit #5 do plano em
# todo-list-prompts/high_priority/11-archival-retrofit-go.md).
#
# Estratégia: fixture local (``impedidos.csv`` + ``rejeitados.csv``) fornece
# as rows; mockamos ``httpx.Client`` no módulo ``tcmgo_sancoes`` com um
# ``MockTransport`` que devolve bytes determinísticos pro endpoint público
# (``ws.tcm.go.gov.br/api/rest/dados/contas-irregulares``). Daí:
#  * snapshot gravado em ``BRACC_ARCHIVAL_ROOT/tcmgo_sancoes/YYYY-MM/*.csv``;
#  * todas as rows de impedidos ganham ``source_snapshot_uri``;
#  * impedido_rels (CNPJ) também recebem URI;
#  * rows de ``rejeitados`` continuam sem URI — não há fonte pública
#    correspondente, então o contrato opt-in vale;
#  * ``restore_snapshot`` devolve os bytes originais do CSV mockado.
# O path offline (``archive_online=False``) NÃO deve popular o campo —
# rodado em ``TestTransform`` acima pra garantir que o retrofit continua
# opt-in.
# ---------------------------------------------------------------------------


_FAKE_CONTAS_CSV = (
    b"CPF;Nome;Assunto;Processo/Fase\n"
    b"12345678000199;EMPRESA MOCK TCMGO LTDA;Irregularidade fake;"
    b"2024.MOCK.001\n"
)


def _tcmgo_handler() -> httpx.MockTransport:
    """MockTransport que emula ws.tcm.go.gov.br (contas-irregulares CSV)."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/contas-irregulares"):
            return httpx.Response(
                200,
                content=_FAKE_CONTAS_CSV,
                headers={"content-type": "text/csv; charset=utf-8"},
            )
        return httpx.Response(
            404,
            content=b"not found",
            headers={"content-type": "text/plain"},
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
    archival_root: Path,  # noqa: ARG001 — just activates the env var
    monkeypatch: pytest.MonkeyPatch,
) -> TcmgoSancoesPipeline:
    """Pipeline com HTTP mockado, ``archive_online=True`` e fixtures locais.

    ``data_dir`` reusa a fixture ``impedidos.csv`` pra transform produzir
    rows determinísticas (o parsing continua a partir do disk), enquanto o
    mock devolve um CSV fake pro endpoint público — o snapshot gravado vem
    desses bytes mockados, não do fixture de disco.
    """
    transport = _tcmgo_handler()
    original_client = httpx.Client

    def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(
        "bracc_etl.pipelines.tcmgo_sancoes.httpx.Client",
        _client_factory,
    )
    pipeline = TcmgoSancoesPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
        archive_online=True,
    )
    # run_id canônico (``{source}_YYYYMMDDHHMMSS``) cai no bucket 2024-09,
    # só pra facilitar conferência visual do path no assert.
    pipeline.run_id = "tcmgo_sancoes_20240915000000"
    return pipeline


class TestArchivalRetrofit:
    """Retrofit: tcmgo_sancoes agora grava snapshots do CSV público."""

    def test_carimba_source_snapshot_uri_em_rows(
        self,
        online_pipeline: TcmgoSancoesPipeline,
        archival_root: Path,
    ) -> None:
        online_pipeline.extract()
        online_pipeline.transform()

        # Apenas os impedidos do CSV REST (contas-irregulares) ganham URI
        # de snapshot — o fluxo JSF tem fluxo de archival separado e fica
        # sem URI nesse teste.
        contas_rows = [
            imp for imp in online_pipeline.impedidos
            if imp.get("list_kind") == "contas_irregulares"
        ]
        assert contas_rows
        expected_uri: str | None = None
        for imp in contas_rows:
            uri = imp.get("source_snapshot_uri")
            assert isinstance(uri, str) and uri
            # Shape: ``tcmgo_sancoes/YYYY-MM/hash12.csv``.
            parts = uri.split("/")
            assert parts[0] == "tcmgo_sancoes"
            assert parts[1] == "2024-09"
            assert parts[2].endswith(".csv")
            if expected_uri is None:
                expected_uri = uri
            else:
                assert uri == expected_uri

        # Rows do fluxo JSF nao recebem URI do archival online (fluxo distinto).
        jsf_rows = [
            imp for imp in online_pipeline.impedidos
            if imp.get("list_kind") == "impedidos_licitar"
        ]
        for imp in jsf_rows:
            assert "source_snapshot_uri" not in imp

        # impedido_rels derivadas do CSV REST replicam a URI do impedido-pai.
        contas_rels = [
            r for r in online_pipeline.impedido_rels
            if r.get("list_kind") == "contas_irregulares"
        ]
        assert contas_rels
        for rel in contas_rels:
            assert rel.get("source_snapshot_uri") == expected_uri

        # rejeitados.csv não tem fonte pública — row continua sem URI.
        assert online_pipeline.rejected_accounts
        for rej in online_pipeline.rejected_accounts:
            assert "source_snapshot_uri" not in rej

        # Storage: arquivo fisicamente presente sob o root configurado.
        assert expected_uri is not None
        absolute = archival_root / expected_uri
        assert absolute.exists(), f"snapshot ausente em {absolute}"

        # Round-trip: restore_snapshot devolve os bytes originais do mock.
        restored = restore_snapshot(expected_uri)
        assert restored == _FAKE_CONTAS_CSV

    def test_offline_path_nao_popula_snapshot_uri(self) -> None:
        """Pipeline com ``archive_online=False`` deixa o campo fora (opt-in)."""
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert pipeline.impedidos
        for imp in pipeline.impedidos:
            # Ausência do campo == opt-in não ativado (contrato do
            # attach_provenance: só injeta a chave quando snapshot_uri
            # não é None).
            assert "source_snapshot_uri" not in imp
        for rel in pipeline.impedido_rels:
            assert "source_snapshot_uri" not in rel
        for rej in pipeline.rejected_accounts:
            assert "source_snapshot_uri" not in rej


# ---------------------------------------------------------------------------
# JSF scraper — impedidos-de-licitar (widget PrimeFaces)
#
# Scraping 100% testado offline via fixtures HTML/XML. A funcao fetch_impedidos_jsf
# aceita um httpx.Client injetado — em producao roda com cliente real, em teste
# com MockTransport devolvendo as fixtures abaixo.
# ---------------------------------------------------------------------------


class TestJsfParserHelpers:
    def test_extract_viewstate_encontra_token_no_html_inicial(self) -> None:
        html = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_initial.html").read_text(
            encoding="utf-8",
        )
        vs = _extract_viewstate(html)
        assert vs == "FAKE_INITIAL_VIEWSTATE_7f3a9c"

    def test_extract_viewstate_retorna_none_quando_ausente(self) -> None:
        assert _extract_viewstate("<html><body>sem token</body></html>") is None

    def test_parse_jsf_table_extrai_colunas(self) -> None:
        html = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_initial.html").read_text(
            encoding="utf-8",
        )
        rows = _parse_jsf_table(html)
        assert len(rows) == 2
        assert rows[0]["nome"] == "EMPRESA FAKE LTDA"
        assert rows[0]["cpf_cnpj"] == "11.222.333/0001-81"
        assert rows[0]["processo"] == "00001/2024"
        assert rows[1]["cpf_cnpj"] == "76***.***-***"

    def test_parse_jsf_table_ignora_linhas_com_menos_colunas(self) -> None:
        fragment = (
            "<table><tbody>"
            "<tr><td>Nome</td><td>Cpf</td></tr>"  # so 2 colunas
            "</tbody></table>"
        )
        assert _parse_jsf_table(fragment) == []

    def test_parse_partial_response_devolve_rows_e_viewstate(self) -> None:
        xml = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_page2.xml").read_text(
            encoding="utf-8",
        )
        rows, vs = _parse_partial_response(xml)
        assert len(rows) == 1
        assert rows[0]["cpf_cnpj"] == "99.888.777/0001-66"
        assert vs == "FAKE_UPDATED_VIEWSTATE_b4e2d1"

    def test_parse_partial_response_sinaliza_fim_via_lista_vazia(self) -> None:
        xml = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_empty.xml").read_text(
            encoding="utf-8",
        )
        rows, vs = _parse_partial_response(xml)
        assert rows == []
        assert vs == "FAKE_UPDATED_VIEWSTATE_final"

    def test_build_ajax_payload_tem_campos_obrigatorios(self) -> None:
        payload = _build_ajax_payload("VS123", first=40, rows_per_page=20)
        assert payload["javax.faces.ViewState"] == "VS123"
        assert payload["form:impedimentos_first"] == "40"
        assert payload["form:impedimentos_rows"] == "20"
        assert payload["javax.faces.partial.ajax"] == "true"
        assert payload["javax.faces.source"] == "form:impedimentos"


class TestFetchImpedidosJsf:
    """Ponta a ponta: GET inicial + POST pagina2 + POST pagina3 (vazia)."""

    def _handler_factory(self, initial_html: str, pages_xml: list[str]) -> httpx.MockTransport:
        """MockTransport que responde GET inicial + POSTs paginados em ordem."""
        pages_iter = iter(pages_xml)

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(
                    200,
                    content=initial_html.encode("utf-8"),
                    headers={"content-type": "text/html; charset=utf-8"},
                )
            if request.method == "POST":
                try:
                    xml = next(pages_iter)
                except StopIteration:
                    xml = ""
                return httpx.Response(
                    200,
                    content=xml.encode("utf-8"),
                    headers={"content-type": "application/xml; charset=utf-8"},
                )
            return httpx.Response(405)

        return httpx.MockTransport(handler)

    def test_scraper_pagina_inicial_mais_paginas_posteriores(
        self, tmp_path: Path,
    ) -> None:
        initial = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_initial.html").read_text(
            encoding="utf-8",
        )
        page2 = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_page2.xml").read_text(
            encoding="utf-8",
        )
        empty = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_empty.xml").read_text(
            encoding="utf-8",
        )

        transport = self._handler_factory(initial, [page2, empty])
        with httpx.Client(transport=transport) as client:
            out_csv = fetch_impedidos_jsf(
                tmp_path,
                client=client,
                rate_limit_seconds=0.0,  # desliga sleep pra testes rapidos.
            )

        assert out_csv.exists()
        assert out_csv.name == "impedidos_licitar.csv"
        content = out_csv.read_text(encoding="utf-8")
        # 3 linhas (2 da inicial + 1 da pagina 2) + header.
        assert content.count("\n") == 4
        assert "EMPRESA FAKE LTDA" in content
        assert "TERCEIRA EMPRESA FAKE LTDA" in content
        assert "76***.***-***" in content

    def test_scraper_respeita_limit(self, tmp_path: Path) -> None:
        initial = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_initial.html").read_text(
            encoding="utf-8",
        )
        empty = (FIXTURES / "tcmgo_sancoes" / "impedidos_jsf_empty.xml").read_text(
            encoding="utf-8",
        )
        transport = self._handler_factory(initial, [empty])
        with httpx.Client(transport=transport) as client:
            out_csv = fetch_impedidos_jsf(
                tmp_path, client=client, limit=1, rate_limit_seconds=0.0,
            )
        content = out_csv.read_text(encoding="utf-8")
        # 1 linha + header.
        assert content.count("\n") == 2

    def test_scraper_aborta_se_viewstate_ausente(self, tmp_path: Path) -> None:
        """GET inicial sem token ViewState → RuntimeError (widget mudou)."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"<html><body>pagina sem viewstate</body></html>",
                headers={"content-type": "text/html"},
            )
        transport = httpx.MockTransport(handler)
        with (
            httpx.Client(transport=transport) as client,
            pytest.raises(RuntimeError, match="sem ViewState"),
        ):
            fetch_impedidos_jsf(
                tmp_path, client=client, rate_limit_seconds=0.0,
            )


# ---------------------------------------------------------------------------
# LAI hardening — extract tolera variacoes do CSV exportado pelo TCM-GO via
# pedido LAI/e-SIC. O scraper JSF emite shape canonico, mas o portal do TCM-GO
# pode devolver export com headers PT-BR, separadores variados, accented
# columns. Os testes abaixo garantem que o pipeline ingere sem mexer em codigo
# pra cada variante esperada.
# ---------------------------------------------------------------------------


class TestNormalizeCsvHeader:
    def test_lowercase_strips_accents(self) -> None:
        assert _normalize_csv_header("Órgão Sancionador") == "orgao_sancionador"

    def test_collapses_punct_runs(self) -> None:
        assert _normalize_csv_header("CPF / CNPJ") == "cpf_cnpj"
        # ``Nº`` decompoe pra ``No`` no NFKD strip — ambos no_processo e
        # n_processo estao mapeados em _LAI_IMPEDIDOS_HEADER_ALIASES.
        assert _normalize_csv_header("Nº-Processo") == "no_processo"
        assert _normalize_csv_header("  início  ") == "inicio"

    def test_idempotent_on_canonical(self) -> None:
        for canon in (
            "nome", "cpf_cnpj", "data_inicio", "data_fim",
            "orgao", "processo", "situacao",
        ):
            assert _normalize_csv_header(canon) == canon


class TestNormalizeLaiImpedidosHeaders:
    def test_renames_ptbr_export_columns(self) -> None:
        df = pd.DataFrame([{
            "Nome": "X", "CPF/CNPJ": "1", "Início": "01/01/2025",
            "Término": "02/01/2025", "Órgão Sancionador": "Y",
            "Nº Processo": "P", "Situação": "I",
        }])
        out = _normalize_lai_impedidos_headers(df)
        assert set(out.columns) == {
            "nome", "cpf_cnpj", "data_inicio", "data_fim",
            "orgao", "processo", "situacao",
        }

    def test_preserves_unknown_columns_normalized(self) -> None:
        df = pd.DataFrame([{"Nome": "X", "Coluna Bizarra Nova": "v"}])
        out = _normalize_lai_impedidos_headers(df)
        assert "nome" in out.columns
        assert "coluna_bizarra_nova" in out.columns

    def test_empty_df_passes_through(self) -> None:
        assert _normalize_lai_impedidos_headers(pd.DataFrame()).empty


class TestLaiCsvVariantsIngest:
    """Pipeline ingere CSVs no shape que pode chegar via LAI/e-SIC.

    Cada fixture testa uma variacao real esperada: headers PT-BR full
    (com acento), separador `,`, separador `\\t`. Apos o normalizer +
    _read_csv_optional, os impedidos caem em ``list_kind='impedidos_licitar'``
    com document_kind correto.
    """

    def _run_with_fixture_dir(self, fixture_dir: str) -> TcmgoSancoesPipeline:
        pipeline = TcmgoSancoesPipeline(
            driver=MagicMock(),
            data_dir=str(FIXTURES / fixture_dir),
            archive_online=False,
        )
        pipeline.extract()
        pipeline.transform()
        return pipeline

    def test_ptbr_full_headers_semicolon(self) -> None:
        pipeline = self._run_with_fixture_dir("tcmgo_sancoes_lai")
        impedidos = [
            r for r in pipeline.impedidos
            if r["list_kind"] == "impedidos_licitar"
        ]
        assert len(impedidos) == 3
        cnpjs = {r["document"] for r in impedidos if r["document_kind"] == "CNPJ"}
        assert "11.222.333/0001-81" in cnpjs
        assert "99.888.777/0001-66" in cnpjs
        # Datas convertidas pra ISO independente do header PT-BR.
        assert all(r["data_inicio"].startswith("2025-") for r in impedidos)
        # Orgao foi mapeado de "Órgão Sancionador".
        assert any("GOIANIA" in r["orgao"] for r in impedidos)

    def test_comma_separator(self) -> None:
        pipeline = self._run_with_fixture_dir("tcmgo_sancoes_lai_comma")
        impedidos = [
            r for r in pipeline.impedidos
            if r["list_kind"] == "impedidos_licitar"
        ]
        assert len(impedidos) == 2
        # "Documento" → cpf_cnpj; "Nome do Sancionado" → nome.
        names = {r["name"] for r in impedidos}
        assert "EMPRESA LAI COMMA LTDA" in names

    def test_tab_separator(self) -> None:
        pipeline = self._run_with_fixture_dir("tcmgo_sancoes_lai_tab")
        impedidos = [
            r for r in pipeline.impedidos
            if r["list_kind"] == "impedidos_licitar"
        ]
        assert len(impedidos) == 2
        # "Razão Social" → nome; "Entidade" → orgao; "Tipo de Sanção" → situacao.
        names = {r["name"] for r in impedidos}
        assert "EMPRESA LAI TAB LTDA" in names
        assert all(r["orgao"] for r in impedidos)


class TestContasIrregularesRename:
    """Rename 2026-04: ``impedidos.csv`` → ``contas_irregulares.csv``.

    Extract prefere o nome canonico novo; cai no legacy se o canonico nao
    existir, pra nao quebrar deploys que ainda nao re-fetcharam.
    """

    def test_le_arquivo_canonico_quando_presente(self) -> None:
        pipeline = TcmgoSancoesPipeline(
            driver=MagicMock(),
            data_dir=str(FIXTURES / "tcmgo_sancoes_renamed"),
            archive_online=False,
        )
        pipeline.extract()
        pipeline.transform()
        # Mesma fixture do legacy — 3 rows de contas-irregulares.
        contas = [
            r for r in pipeline.impedidos
            if r["list_kind"] == "contas_irregulares"
        ]
        assert len(contas) == 3

    def test_le_legacy_impedidos_csv_quando_canonico_ausente(self) -> None:
        # Diretorio padrao tem so ``impedidos.csv`` (legacy) — fallback ativa.
        pipeline = TcmgoSancoesPipeline(
            driver=MagicMock(),
            data_dir=str(FIXTURES),
            archive_online=False,
        )
        pipeline.extract()
        pipeline.transform()
        contas = [
            r for r in pipeline.impedidos
            if r["list_kind"] == "contas_irregulares"
        ]
        assert len(contas) == 3


class TestFetchToDiskWritesCanonicalName:
    """``fetch_to_disk`` grava ``contas_irregulares.csv`` (nome novo)."""

    def test_grava_arquivo_com_nome_canonico(self, tmp_path: Path) -> None:
        fake_csv = (
            "CPF;Nome;Assunto;Processo/Fase\n"
            "12345678000199;EMPRESA MOCK LTDA;Irreg fake;2024.MOCK.001\n"
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=fake_csv.encode("utf-8"),
                headers={"content-type": "text/csv; charset=utf-8"},
            )

        transport = httpx.MockTransport(handler)
        original_client = httpx.Client

        def _client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
            kwargs["transport"] = transport
            return original_client(*args, **kwargs)

        # fetch_to_disk usa httpx.Client diretamente; monkeypatchamos pelo
        # contexto local sem MonkeyPatch fixture pra manter o teste autonomo.
        import bracc_etl.pipelines.tcmgo_sancoes as module
        saved = module.httpx.Client
        module.httpx.Client = _client_factory  # type: ignore[assignment]
        try:
            paths = fetch_to_disk(tmp_path)
        finally:
            module.httpx.Client = saved  # type: ignore[assignment]

        assert len(paths) == 1
        assert paths[0].name == "contas_irregulares.csv"
        assert paths[0].exists()
        assert "EMPRESA MOCK LTDA" in paths[0].read_text(encoding="utf-8")

"""Tests for the ``tse_prestacao_contas_go`` pipeline.

Covers:

* ZIP fixture built em memória com as 3 CSVs mínimas (receitas, despesas,
  bens) pra não depender de network.
* Archival retrofit — ZIP inteiro vira snapshot content-addressed e toda
  row carimba ``source_snapshot_uri`` apontando pra ele.
* Propriedades atualizadas em ``:Person`` com o shape esperado pelo Flask
  (``gerar_validacao_tse``): ``total_tse_2022``, ``tse_2022_partido``,
  ``tse_2022_pessoa_fisica``, ``tse_2022_proprios``, ``tse_2022_fin_coletivo``
  + ``patrimonio_declarado`` / ``patrimonio_ano``.
* Filtro UF=GO — rows de outras UFs são descartadas antes do load.
* LGPD — CPF de doador pessoa física é mascarado em todas as
  estruturas expostas.
* Provenance — 6 campos + ``source_snapshot_uri``.
"""

from __future__ import annotations

import io
import zipfile
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import httpx
import pytest

from bracc_etl.pipelines.tse_prestacao_contas_go import (
    _SOURCE_ID,
    TsePrestacaoContasGoPipeline,
    _classify_origem,
)
from tests._mock_helpers import mock_driver, mock_session

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# ---------------------------------------------------------------------------
# Fixture ZIP — 3 candidatos GO + 1 candidato SP (pra testar filtro UF).
# ---------------------------------------------------------------------------

_YEAR = 2022

# Header + rows de receitas_candidatos_2022_BRASIL.csv (TSE usa ; + latin-1).
_RECEITAS_HEADER = (
    "ANO_ELEICAO;SG_UF;SQ_CANDIDATO;NR_CPF_CANDIDATO;NM_CANDIDATO;"
    "DS_ORIGEM_RECEITA;VR_RECEITA;NR_CPF_CNPJ_DOADOR;NM_DOADOR"
)

# 3 candidatos GO com 5 doações cada + 1 candidato SP (5 linhas pra
# garantir que o filtro UF=GO exclui o SP inteiro).
# Candidato 1 (GO): cpf 11144477735 — 5 doações (5 buckets distintos).
# Candidato 2 (GO): cpf 52998224725 — 5 doações PF.
# Candidato 3 (GO): cpf 22233344456 — 5 doações próprio.
# Candidato 4 (SP, descartado): cpf 99988877766.
_RECEITAS_ROWS_GO_C1 = [
    # partido
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "Recursos de partido político", "1000,00", "12345678000100", "PARTIDO X"),
    # proprios
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "Recursos próprios", "500,50", "11144477735", "CANDIDATO UM"),
    # pessoa_fisica
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "Recursos de pessoas físicas", "200,00", "22233344456", "ZE DA SILVA"),
    # fin_coletivo
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "Recursos de financiamento coletivo", "300,00", "", ""),
    # outros (linha com origem vazia)
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "", "50,00", "", ""),
]
_RECEITAS_ROWS_GO_C2 = [
    (_YEAR, "GO", "GO0002", "52998224725", "CANDIDATA DOIS",
     "Recursos de pessoas físicas", "100,00", f"555666777{n:02d}", f"DOADOR {n}")
    for n in range(88, 93)
]
_RECEITAS_ROWS_GO_C3 = [
    (_YEAR, "GO", "GO0003", "22233344456", "CANDIDATO TRES",
     "Recursos próprios", "400,00", "22233344456", "CANDIDATO TRES")
    for _ in range(5)
]
_RECEITAS_ROWS_SP = [
    (_YEAR, "SP", "SP0001", "99988877766", "CANDIDATO SP",
     "Recursos de pessoas físicas", "9999,00", "11122233344", "DOADOR SP")
    for _ in range(5)
]


def _rows_to_csv(header: str, rows: list[tuple[Any, ...]]) -> bytes:
    lines = [header]
    for r in rows:
        lines.append(";".join(str(x) for x in r))
    return ("\n".join(lines) + "\n").encode("latin-1")


_DESPESAS_HEADER = (
    "ANO_ELEICAO;SG_UF;SQ_CANDIDATO;NR_CPF_CANDIDATO;NM_CANDIDATO;"
    "DS_TIPO_DESPESA;VR_PAGTO_DESPESA;NR_CPF_CNPJ_FORNECEDOR;NM_FORNECEDOR"
)
_DESPESAS_ROWS = [
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "Publicidade", "300,00", "22222222000100", "GRAFICA GO"),
    (_YEAR, "GO", "GO0002", "52998224725", "CANDIDATA DOIS",
     "Combustível", "150,00", "33333333000100", "POSTO GO"),
    (_YEAR, "SP", "SP0001", "99988877766", "CANDIDATO SP",
     "Aluguel", "5000,00", "44444444000100", "IMOBILIARIA SP"),
]

_BENS_HEADER = (
    "ANO_ELEICAO;SG_UF;SQ_CANDIDATO;NR_CPF_CANDIDATO;NM_CANDIDATO;"
    "DS_TIPO_BEM_CANDIDATO;DS_BEM_CANDIDATO;VR_BEM_CANDIDATO"
)
_BENS_ROWS = [
    # 2 bens por candidato GO (3 candidatos × 2).
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "Apartamento", "Apartamento em Goiânia", "250000,00"),
    (_YEAR, "GO", "GO0001", "11144477735", "CANDIDATO UM",
     "Veículo", "Carro popular", "30000,00"),
    (_YEAR, "GO", "GO0002", "52998224725", "CANDIDATA DOIS",
     "Casa", "Residência principal", "450000,00"),
    (_YEAR, "GO", "GO0002", "52998224725", "CANDIDATA DOIS",
     "Quotas", "Quotas empresa", "50000,00"),
    (_YEAR, "GO", "GO0003", "22233344456", "CANDIDATO TRES",
     "Terreno", "Terreno rural", "100000,00"),
    (_YEAR, "GO", "GO0003", "22233344456", "CANDIDATO TRES",
     "Veículo", "Motocicleta", "15000,00"),
    # SP — descartado por filtro
    (_YEAR, "SP", "SP0001", "99988877766", "CANDIDATO SP",
     "Imóvel", "Mansão em SP", "9999999,00"),
]


def _build_zip_bytes() -> bytes:
    """Monta ZIP mínimo compatível com o layout do TSE."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            f"receitas_candidatos_{_YEAR}_BRASIL.csv",
            _rows_to_csv(
                _RECEITAS_HEADER,
                _RECEITAS_ROWS_GO_C1
                + _RECEITAS_ROWS_GO_C2
                + _RECEITAS_ROWS_GO_C3
                + _RECEITAS_ROWS_SP,
            ),
        )
        zf.writestr(
            f"despesas_pagas_candidatos_{_YEAR}_BRASIL.csv",
            _rows_to_csv(_DESPESAS_HEADER, _DESPESAS_ROWS),
        )
        zf.writestr(
            f"bens_candidato_{_YEAR}_BRASIL.csv",
            _rows_to_csv(_BENS_HEADER, _BENS_ROWS),
        )
    return buf.getvalue()


_ZIP_BYTES = _build_zip_bytes()


def _build_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        # Devolve o mesmo ZIP pra qualquer GET da CDN TSE.
        return httpx.Response(
            200,
            content=_ZIP_BYTES,
            headers={"content-type": "application/zip"},
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
def pipeline(
    archival_root: Path,  # noqa: ARG001 — fixture só ativa o env var
    tmp_path: Path,
) -> TsePrestacaoContasGoPipeline:
    driver = MagicMock()
    transport = _build_transport()

    def factory() -> httpx.Client:
        return httpx.Client(transport=transport, follow_redirects=True)

    return TsePrestacaoContasGoPipeline(
        driver=driver,
        data_dir=str(tmp_path),
        http_client_factory=factory,
        year=_YEAR,
        uf="GO",
    )


# ---------------------------------------------------------------------------
# Metadata / registry
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_name(self) -> None:
        assert TsePrestacaoContasGoPipeline.name == "tse_prestacao_contas_go"

    def test_source_id(self) -> None:
        assert TsePrestacaoContasGoPipeline.source_id == _SOURCE_ID
        assert _SOURCE_ID == "tse_prestacao_contas"

    def test_year_parametrizable(self, archival_root: Path) -> None:  # noqa: ARG002
        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
            year=2026,
        )
        assert p.year == 2026
        # URL points to 2026 zip.
        assert str(2026) in p._zip_url


# ---------------------------------------------------------------------------
# _classify_origem — unit test dos buckets
# ---------------------------------------------------------------------------


class TestClassifyOrigem:
    @pytest.mark.parametrize(("raw", "expected"), [
        ("Recursos de partido político", "partido"),
        ("Fundo Partidário", "partido"),
        ("Fundo Especial de Financiamento de Campanha (FEFC)", "partido"),
        ("Recursos próprios", "proprios"),
        ("Autofinanciamento", "proprios"),
        ("Recursos de pessoas físicas", "pessoa_fisica"),
        ("Recursos de pessoa jurídica", "pessoa_juridica"),
        ("Recursos de financiamento coletivo", "fin_coletivo"),
        ("Vaquinha online", "fin_coletivo"),
        ("", "outros"),
        ("Categoria desconhecida", "outros"),
    ])
    def test_mapping(self, raw: str, expected: str) -> None:
        assert _classify_origem(raw) == expected


# ---------------------------------------------------------------------------
# Extract — download + archival + UF filter
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_filters_uf_go(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        # Só rows GO — SP é descartado.
        assert all(
            (r.get("SG_UF") or "").upper() == "GO"
            for r in pipeline._receitas_raw
        )
        assert all(
            (r.get("SG_UF") or "").upper() == "GO"
            for r in pipeline._despesas_raw
        )
        assert all(
            (r.get("SG_UF") or "").upper() == "GO"
            for r in pipeline._bens_raw
        )

    def test_extract_reads_all_go_rows(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        # 3 candidatos × 5 receitas cada = 15
        assert len(pipeline._receitas_raw) == 15
        # 2 despesas GO
        assert len(pipeline._despesas_raw) == 2
        # 3 candidatos × 2 bens cada = 6
        assert len(pipeline._bens_raw) == 6

    def test_extract_archives_zip(
        self,
        pipeline: TsePrestacaoContasGoPipeline,
        archival_root: Path,
    ) -> None:
        pipeline.extract()
        # Snapshot URI setado
        assert pipeline._snapshot_uri
        assert pipeline._snapshot_uri.startswith(f"{_SOURCE_ID}/")
        # Arquivo gravado no archival (extensão é .bin pois ``application/
        # zip`` não mapeia em ``_CONTENT_TYPE_EXTENSIONS`` — conteúdo é
        # idêntico, só muda o hint no nome).
        snapshot_dir = archival_root / _SOURCE_ID
        files = [p for p in snapshot_dir.rglob("*") if p.is_file()]
        assert len(files) == 1
        # Conteúdo preservado byte-a-byte.
        assert files[0].read_bytes() == _ZIP_BYTES

    def test_extract_uses_cached_zip_when_present(
        self,
        tmp_path: Path,
        archival_root: Path,  # noqa: ARG002
    ) -> None:
        """Se ZIP já existe em ``{data_dir}/tse_prestacao_contas/...``,
        pipeline usa ele em vez de baixar."""
        cache_dir = tmp_path / "tse_prestacao_contas"
        cache_dir.mkdir(parents=True)
        (cache_dir / f"prestacao_de_contas_eleitorais_candidatos_{_YEAR}.zip").write_bytes(
            _ZIP_BYTES,
        )

        def factory() -> httpx.Client:
            # Transport que SEMPRE retorna 500 — se o pipeline tentar baixar
            # o teste quebra.
            return httpx.Client(
                transport=httpx.MockTransport(
                    lambda r: httpx.Response(500),  # noqa: ARG005
                ),
            )

        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir=str(tmp_path),
            http_client_factory=factory,
            year=_YEAR,
        )
        p.extract()
        assert len(p._receitas_raw) == 15


# ---------------------------------------------------------------------------
# Transform — properties, buckets, LGPD, provenance
# ---------------------------------------------------------------------------


class TestTransform:
    def test_produces_three_go_candidates(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.persons) == 3
        cpfs = {p["cpf"] for p in pipeline.persons}
        assert cpfs == {
            "111.444.777-35",
            "529.982.247-25",
            "222.333.444-56",
        }

    def test_person_has_expected_tse_properties(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # Candidato 1 breakdown: 1000 partido + 500,50 proprios +
        # 200 pf + 300 fin_coletivo + 50 outros = 2050,50 total
        c1 = next(p for p in pipeline.persons if p["cpf"] == "111.444.777-35")
        assert c1["total_tse_2022"] == pytest.approx(2050.50)
        assert c1["tse_2022_partido"] == pytest.approx(1000.00)
        assert c1["tse_2022_proprios"] == pytest.approx(500.50)
        assert c1["tse_2022_pessoa_fisica"] == pytest.approx(200.00)
        assert c1["tse_2022_fin_coletivo"] == pytest.approx(300.00)
        assert c1["tse_2022_outros"] == pytest.approx(50.00)
        # Patrimônio = 250000 + 30000 = 280000
        assert c1["patrimonio_declarado"] == pytest.approx(280000.00)
        assert c1["patrimonio_ano"] == 2022
        assert c1["uf"] == "GO"
        assert c1["name"]  # preenchido
        assert c1["numero_candidato"] == "GO0001"

    def test_keys_match_flask_gerar_validacao_tse(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """Guard contra regressão das keys consumidas pelo Flask.

        ``backend/app.py::gerar_validacao_tse`` lê as chaves literais:
        ``total_tse_2022``, ``tse_2022_partido``,
        ``tse_2022_pessoa_fisica``, ``tse_2022_proprios``,
        ``tse_2022_fin_coletivo``. Quebrar essas chaves quebra
        ``/politico``.
        """
        pipeline.extract()
        pipeline.transform()
        required = {
            "total_tse_2022",
            "tse_2022_partido",
            "tse_2022_pessoa_fisica",
            "tse_2022_proprios",
            "tse_2022_fin_coletivo",
            "patrimonio_declarado",
            "patrimonio_ano",
        }
        for p in pipeline.persons:
            missing = required - p.keys()
            assert not missing, f"missing {missing} em {p['cpf']}"

    def test_provenance_on_every_person(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for p in pipeline.persons:
            assert p["source_id"] == _SOURCE_ID
            assert p["source_record_id"].endswith(":2022")
            assert p["source_url"].startswith(
                "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/",
            )
            assert p["source_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
            assert p["run_id"].startswith(f"{_SOURCE_ID}_")
            assert p["ingested_at"].startswith("20")

    def test_donation_nodes_produced(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # 5 + 5 + 5 = 15 doações (todas GO — SP foi filtrado).
        assert len(pipeline.donations) == 15
        # Tipagem do doador
        pf = [d for d in pipeline.donations if d["doador_tipo"] == "pf"]
        pj = [d for d in pipeline.donations if d["doador_tipo"] == "pj"]
        assert pf, "esperava pelo menos uma doação PF"
        assert pj, "esperava pelo menos uma doação PJ"

    def test_cpf_doador_is_masked(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        """LGPD — CPF do doador PF nunca aparece cru nos dicts produzidos."""
        pipeline.extract()
        pipeline.transform()
        # Coletar valores que possam conter CPF cru
        raw_cpfs_expostos = {
            "22233344456",  # ZE DA SILVA (doador de C1)
            "55566677788",
            "55566677789",
            "55566677790",
            "55566677791",
            "55566677792",
        }
        for donation in pipeline.donations:
            if donation["doador_tipo"] != "pf":
                continue
            doador_id = str(donation["doador_id"])
            for crua in raw_cpfs_expostos:
                # doador_id não pode ser o CPF completo sem máscara.
                assert doador_id != crua
                # O valor armazenado tem que ter "*" (máscara aplicada).
            assert "*" in doador_id
        # Rels também
        for rel in pipeline.donation_rels:
            if rel.get("doador_tipo") != "pf":
                continue
            src = str(rel["source_key"])
            for crua in raw_cpfs_expostos:
                assert src != crua

    def test_donation_provenance_with_snapshot(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        for d in pipeline.donations:
            assert d["source_id"] == _SOURCE_ID
            assert d["source_snapshot_uri"].startswith(f"{_SOURCE_ID}/")
            assert d["source_url"].startswith("https://cdn.tse.jus.br")

    def test_expenses_produced(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        # Só as 2 despesas GO
        assert len(pipeline.expenses) == 2
        for e in pipeline.expenses:
            assert e["uf"] == "GO"
            assert e["valor"] > 0
            assert e["ano"] == 2022

    def test_expense_rels_route_to_person(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        assert len(pipeline.expense_rels) == 2
        for rel in pipeline.expense_rels:
            # source_key é CPF formatado do candidato.
            assert "." in rel["source_key"]
            assert "-" in rel["source_key"]


# ---------------------------------------------------------------------------
# Load — smoke (mock driver)
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_noop_without_persons(self, archival_root: Path) -> None:  # noqa: ARG002
        p = TsePrestacaoContasGoPipeline(
            driver=MagicMock(),
            data_dir="./data",
        )
        # não extraímos — persons está vazio
        p.load()
        mock_driver(p).session.assert_not_called()

    def test_load_hits_session_multiple_times(
        self, pipeline: TsePrestacaoContasGoPipeline,
    ) -> None:
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
        # Person + donation nodes + donation rels + expense nodes +
        # expense rels — mínimo 3 chamadas ao session.
        assert mock_session(pipeline).run.call_count >= 3

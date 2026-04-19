"""Unit tests para :mod:`bracc.services.perfil_service` (Fase 04.F).

Mocka driver Neo4j + sub-services (obter_ceap_deputado, obter_emendas_deputado,
calcular_media_ceap_estado) pra validar:

* Assembly completo do ``PerfilPolitico`` com entidade focal + conexões.
* 404 (``EntityNotFoundError``) quando o nó não existe OU não tem label
  de político.
* 502 (``DriverError``) quando o driver levanta.
* Deputado federal completo — 22 campos top-level populados.
* Político sem mandato — CEAP/emendas vazios, ``aviso_despesas`` preenchido.
* LGPD: ``model_dump_json`` nunca contém o CPF pleno dos doadores/família
  (mascaramento obrigatório do :mod:`bracc.services.formatacao_service`).
* ``ProvenanceBlock`` no topo é populado a partir dos props do nó focal
  (ou ``None`` pra nós legados sem carimbo).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bracc.models.entity import ProvenanceBlock
from bracc.models.perfil import DespesaGabinete, Emenda, PerfilPolitico
from bracc.services.perfil_service import (
    DriverError,
    EntityNotFoundError,
    obter_perfil,
)

# --- Helpers ---------------------------------------------------------------


def _mock_record(data: dict[str, Any]) -> MagicMock:
    """Record-like do driver neo4j com ``.get(key)`` + ``[key]``."""
    record = MagicMock()
    record.get.side_effect = lambda key, default=None: data.get(key, default)
    record.__getitem__.side_effect = lambda key: data[key]
    record.__contains__.side_effect = lambda key: key in data
    return record


def _build_driver() -> MagicMock:
    """Driver mock com ``session()`` context manager."""
    mock_session = AsyncMock()
    driver = MagicMock()
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    driver.session.return_value.__aexit__ = AsyncMock(return_value=None)
    return driver


PROV_FIELDS = {
    "source_id": "camara_deputados",
    "source_record_id": "1001",
    "source_url": "https://dadosabertos.camara.leg.br/api/v2/deputados/1001",
    "ingested_at": "2026-04-18T00:00:00+00:00",
    "run_id": "camara_deputados_20260418000000",
    "source_snapshot_uri": "camara_deputados/2026-04/abc.json",
}


def _legislator_node(
    *,
    id_camara: str = "1001",
    include_provenance: bool = True,
    patrimonio: float | None = 1_500_000.0,
    cpf: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Props canônicos de um ``:FederalLegislator`` (pipeline camara_politicos_go)."""
    props: dict[str, Any] = {
        "name": "Deputado Exemplo",
        "cpf": cpf,
        "partido": "ABC",
        "uf": "GO",
        "id_camara": id_camara,
        "role": "deputado federal",
        "patrimonio_declarado": patrimonio,
        "is_pep": False,
        "element_id": "4:abc:1",
        "labels": ["Person", "FederalLegislator"],
    }
    if include_provenance:
        props.update(PROV_FIELDS)
    props.update(extra)
    return props


def _patch_ceap_and_emendas(
    despesas: list[DespesaGabinete] | None = None,
    emendas: list[Emenda] | None = None,
    media_estado: float = 0.0,
    verba_alego: list[DespesaGabinete] | None = None,
    cota_gyn: list[DespesaGabinete] | None = None,
) -> Any:
    """Patch dos sub-services paralelos do service 04.C/04.D + ALEGO (verba) +
    CMG (cota vereador Goiania)."""
    return (
        patch(
            "bracc.services.perfil_service.obter_ceap_deputado",
            new_callable=AsyncMock,
            return_value=despesas or [],
        ),
        patch(
            "bracc.services.perfil_service.obter_emendas_deputado",
            new_callable=AsyncMock,
            return_value=emendas or [],
        ),
        patch(
            "bracc.services.perfil_service.calcular_media_ceap_estado",
            new_callable=AsyncMock,
            return_value=media_estado,
        ),
        patch(
            "bracc.services.perfil_service.obter_verba_indenizatoria_alego",
            new_callable=AsyncMock,
            return_value=verba_alego or [],
        ),
        patch(
            "bracc.services.perfil_service.obter_cota_vereador_goiania",
            new_callable=AsyncMock,
            return_value=cota_gyn or [],
        ),
    )


# --- 1. Errors --------------------------------------------------------------


class TestErrors:
    @pytest.mark.anyio
    async def test_404_quando_query_retorna_none(self) -> None:
        driver = _build_driver()
        with patch(
            "bracc.services.perfil_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=None,
        ), pytest.raises(EntityNotFoundError):
            await obter_perfil(driver, "inexistente")

    @pytest.mark.anyio
    async def test_404_quando_politico_none_no_record(self) -> None:
        driver = _build_driver()
        record = _mock_record({"politico": None, "conexoes": []})
        with patch(
            "bracc.services.perfil_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=record,
        ), pytest.raises(EntityNotFoundError):
            await obter_perfil(driver, "4:xyz:99")

    @pytest.mark.anyio
    async def test_404_quando_nao_e_politico(self) -> None:
        """Nó existe mas não tem label de Person/Legislator."""
        driver = _build_driver()
        bad_node: dict[str, Any] = {
            "name": "ACME LTDA",
            "labels": ["Company"],
            "element_id": "4:abc:9",
        }
        record = _mock_record({"politico": bad_node, "conexoes": []})
        with patch(
            "bracc.services.perfil_service.execute_query_single",
            new_callable=AsyncMock,
            return_value=record,
        ), pytest.raises(EntityNotFoundError):
            await obter_perfil(driver, "4:abc:9")

    @pytest.mark.anyio
    async def test_502_quando_driver_levanta(self) -> None:
        driver = _build_driver()
        with patch(
            "bracc.services.perfil_service.execute_query_single",
            new_callable=AsyncMock,
            side_effect=RuntimeError("neo4j down"),
        ), pytest.raises(DriverError):
            await obter_perfil(driver, "4:abc:1")


# --- 2. Happy path: deputado federal completo -------------------------------


class TestDeputadoFederalCompleto:
    @pytest.mark.anyio
    async def test_shape_completo_22_campos_top_level(self) -> None:
        """Happy path: deputado federal com CEAP, emenda, doador, sócio, família."""
        driver = _build_driver()
        legislator = _legislator_node()

        # Conexões: 1 doador empresa, 1 família, 1 emenda embutida.
        conexoes = [
            {
                "rel_type": "DOOU",
                "rel_props": {"valor": 50_000.0},
                "source_id": "4:empresa:1",
                "target_id": "4:abc:1",
                "target_element_id": "4:empresa:1",
                "target_type": "Company",
                "target_labels": ["Company"],
                "target_props": {
                    "cnpj": "11222333000181",
                    "razao_social": "ACME LTDA",
                    "name": "ACME LTDA",
                },
            },
            {
                "rel_type": "CONJUGE_DE",
                "rel_props": {},
                "source_id": "4:abc:1",
                "target_id": "4:pessoa:2",
                "target_element_id": "4:pessoa:2",
                "target_type": "Person",
                "target_labels": ["Person"],
                "target_props": {
                    "name": "Conjuge Exemplo",
                    "cpf": "12345678909",  # pleno — service deve mascarar
                },
            },
            {
                "rel_type": "PROPOS",
                "rel_props": {},
                "source_id": "4:abc:1",
                "target_id": "4:amend:1",
                "target_element_id": "4:amend:1",
                "target_type": "Amendment",
                "target_labels": ["Amendment"],
                "target_props": {
                    "amendment_id": "EM-2024-001",
                    "type": "individual",
                    "function": "saude",
                    "municipality": "Goiania",
                    "uf": "GO",
                    "value_committed": 100_000.0,
                    "value_paid": 80_000.0,
                },
            },
        ]
        record = _mock_record({"politico": legislator, "conexoes": conexoes})

        despesas_fake = [
            DespesaGabinete(tipo="Combustivel", total=5_000.0, total_fmt="R$ 5.0 mil"),
        ]

        patches = _patch_ceap_and_emendas(despesas=despesas_fake)
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        assert isinstance(perfil, PerfilPolitico)
        # --- Provenance no topo ---
        assert perfil.provenance is not None
        assert isinstance(perfil.provenance, ProvenanceBlock)
        assert perfil.provenance.source_id == "camara_deputados"
        assert perfil.provenance.snapshot_url == PROV_FIELDS["source_snapshot_uri"]

        # --- Político focal ---
        assert perfil.politico.id == "4:abc:1"
        assert perfil.politico.nome == "Deputado Exemplo"
        assert perfil.politico.uf == "GO"
        assert perfil.politico.cargo == "Deputado(a) Federal"
        assert perfil.politico.patrimonio == 1_500_000.0
        assert perfil.politico.patrimonio_formatado is not None

        # --- 22 campos top-level (existência) ---
        top = perfil.model_dump()
        for key in (
            "provenance", "politico", "resumo", "emendas",
            "total_emendas_valor", "total_emendas_valor_fmt",
            "empresas", "contratos", "despesas_gabinete",
            "total_despesas_gabinete", "total_despesas_gabinete_fmt",
            "comparacao_cidada", "comparacao_cidada_resumo",
            "alertas", "conexoes_total", "fonte_emendas",
            "descricao_conexoes", "doadores_empresa", "doadores_pessoa",
            "total_doacoes", "total_doacoes_fmt",
            "socios", "familia", "aviso_despesas", "validacao_tse",
        ):
            assert key in top, f"Missing top-level field: {key}"

        # --- Classificação de conexões ---
        assert len(perfil.doadores_empresa) == 1
        assert perfil.doadores_empresa[0].valor_total == 50_000.0
        assert len(perfil.familia) == 1
        assert len(perfil.emendas) == 1
        assert perfil.emendas[0].id == "EM-2024-001"

        # --- CEAP ingerido ---
        assert len(perfil.despesas_gabinete) == 1
        assert perfil.despesas_gabinete[0].tipo == "Combustivel"
        assert perfil.total_despesas_gabinete == 5_000.0

        # --- Totais ---
        assert perfil.total_emendas_valor == 80_000.0  # value_paid
        assert perfil.total_doacoes == 50_000.0
        assert perfil.conexoes_total == 3

        # --- fonte_emendas = bracc (vieram do grafo) ---
        assert perfil.fonte_emendas == "bracc"

        # --- Resumo ---
        assert "Deputado Exemplo" in perfil.resumo
        assert "Deputado(a) Federal" in perfil.resumo

        # --- Descrição conexões ---
        assert "1 empresa(s)" in perfil.descricao_conexoes  # doador empresa
        assert "1 familiar(es)" in perfil.descricao_conexoes


# --- 3. Político sem mandato (não deputado federal) ------------------------


class TestPoliticoSemMandato:
    @pytest.mark.anyio
    async def test_sem_mandato_ceap_vazio_e_aviso_preenchido(self) -> None:
        """Person sem label FederalLegislator → sem CEAP, sem emenda, aviso."""
        driver = _build_driver()
        # Só Person — sem id_camara.
        pessoa = {
            "name": "Vereador Exemplo",
            "cpf": None,
            "uf": "GO",
            "role": "vereador",
            "is_pep": False,
            "element_id": "4:pes:3",
            "labels": ["Person"],
            **PROV_FIELDS,
        }
        record = _mock_record({"politico": pessoa, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:pes:3")

        assert perfil.despesas_gabinete == []
        assert perfil.emendas == []
        assert perfil.total_despesas_gabinete == 0.0
        assert perfil.total_emendas_valor == 0.0
        assert perfil.total_doacoes == 0.0
        assert perfil.fonte_emendas is None
        # Aviso explicativo presente pra não-deputado-federal — texto
        # genérico do fallback "Ainda nao temos os dados ..." (caso
        # vereador/outros).
        assert "Ainda nao temos os dados" in perfil.aviso_despesas
        assert "verba indenizatoria da ALEGO" in perfil.aviso_despesas

        # Alertas — deve ter o fallback "Avaliação indisponível" porque
        # não tem nenhum dado (ou algum alerta de rotina). No mínimo
        # 1 alerta (nunca vazio).
        assert len(perfil.alertas) >= 1


# --- 4. LGPD: CPF pleno jamais no response ---------------------------------


class TestLgpd:
    @pytest.mark.anyio
    async def test_cpf_pleno_nunca_aparece_no_json_final(self) -> None:
        """Mesmo com CPF pleno nos dados do grafo, response não vaza."""
        driver = _build_driver()
        # Legislator com CPF pleno nos props + doador pessoa com CPF pleno
        # nas conexões. Nenhum deles pode aparecer puro no JSON final —
        # o political cpf é exibido mas só o valor já mascarado que vier
        # do pipeline (o middleware CPF masking é a rede de segurança).
        legislator = _legislator_node(cpf=None)
        conexoes = [
            {
                "rel_type": "DOOU",
                "rel_props": {"valor": 1_000.0},
                "source_id": "4:pessoa:7",
                "target_id": "4:abc:1",
                "target_element_id": "4:pessoa:7",
                "target_type": "Person",
                "target_labels": ["Person"],
                "target_props": {
                    "name": "Doador",
                    "cpf": "98765432100",  # pleno — tem que mascarar
                },
            },
            {
                "rel_type": "PARENTE_DE",
                "rel_props": {},
                "source_id": "4:abc:1",
                "target_id": "4:pessoa:8",
                "target_element_id": "4:pessoa:8",
                "target_type": "Person",
                "target_labels": ["Person"],
                "target_props": {
                    "name": "Parente",
                    "cpf": "11122233344",  # pleno — tem que mascarar
                },
            },
        ]
        record = _mock_record({"politico": legislator, "conexoes": conexoes})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        # Serialização JSON completa (equivalente ao que sai no response HTTP).
        payload = perfil.model_dump_json()

        # Nenhum CPF pleno pode aparecer no payload.
        assert "98765432100" not in payload, (
            "CPF pleno do doador pessoa vazou no response (LGPD)"
        )
        assert "11122233344" not in payload, (
            "CPF pleno do familiar vazou no response (LGPD)"
        )

        # Sanity: a máscara no formato canônico deve estar lá.
        assert "***.***.***-00" in payload  # do doador (termina em 00)
        assert "***.***.***-44" in payload  # do parente (termina em 44)


# --- 5. Provenance ausente → None ------------------------------------------


class TestProvenance:
    @pytest.mark.anyio
    async def test_no_sem_provenance_resulta_em_none(self) -> None:
        """Nó legado sem ``source_*`` → ``provenance: None`` no topo."""
        driver = _build_driver()
        legislator = _legislator_node(include_provenance=False)
        record = _mock_record({"politico": legislator, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        assert perfil.provenance is None

    @pytest.mark.anyio
    async def test_emenda_e_doador_empresa_populam_provenance_via_conexoes(
        self,
    ) -> None:
        """Sub-rows (``Emenda`` / ``DoadorEmpresa``) carregam provenance
        quando os props do nó alvo carregam os 5+1 campos."""
        driver = _build_driver()
        legislator = _legislator_node()
        conexoes = [
            {
                "rel_type": "DOOU",
                "rel_props": {"valor": 25_000.0},
                "source_id": "4:empresa:1",
                "target_id": "4:abc:1",
                "target_element_id": "4:empresa:1",
                "target_type": "Company",
                "target_labels": ["Company"],
                "target_props": {
                    "cnpj": "11222333000181",
                    "razao_social": "ACME LTDA",
                    "source_id": "tse_prestacao_contas",
                    "source_record_id": "DOC-42",
                    "source_url": (
                        "https://divulgacandcontas.tse.jus.br/.../DOC-42"
                    ),
                    "ingested_at": "2026-04-10T12:00:00+00:00",
                    "run_id": "tse_prestacao_contas_20260410120000",
                },
            },
            {
                "rel_type": "PROPOS",
                "rel_props": {},
                "source_id": "4:abc:1",
                "target_id": "4:amend:1",
                "target_element_id": "4:amend:1",
                "target_type": "Amendment",
                "target_labels": ["Amendment"],
                "target_props": {
                    "amendment_id": "EM-2024-007",
                    "type": "individual",
                    "function": "saude",
                    "value_committed": 200_000.0,
                    "value_paid": 150_000.0,
                    "source_id": "siop",
                    "source_url": "https://siop.planejamento.gov.br/.../EM-2024-007",
                    "ingested_at": "2026-04-12T00:00:00+00:00",
                    "run_id": "siop_20260412000000",
                },
            },
        ]
        record = _mock_record({"politico": legislator, "conexoes": conexoes})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        assert len(perfil.emendas) == 1
        assert perfil.emendas[0].provenance is not None
        assert perfil.emendas[0].provenance.source_id == "siop"

        assert len(perfil.doadores_empresa) == 1
        assert perfil.doadores_empresa[0].provenance is not None
        assert perfil.doadores_empresa[0].provenance.source_id == (
            "tse_prestacao_contas"
        )


# --- 6. Teto de gastos (novo campo TetoGastos) -----------------------------


class TestTetoGastos:
    @pytest.mark.anyio
    async def test_com_despesas_tse_teto_populado(self) -> None:
        """``total_despesas_tse_2022`` + ``cargo_tse_2022`` no grafo → teto preenchido."""
        driver = _build_driver()
        legislator = _legislator_node(
            total_despesas_tse_2022=1_800_000.0,
            cargo_tse_2022="DEPUTADO FEDERAL",
        )
        record = _mock_record({"politico": legislator, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        assert perfil.teto_gastos is not None
        assert perfil.teto_gastos.valor_limite == 2_100_000.0
        assert perfil.teto_gastos.classificacao == "alto"
        assert perfil.teto_gastos.ano_eleicao == 2022
        # Alerta info associado (alto < limite = 'info').
        assert any(
            a.get("tipo") == "info" and "teto" in a.get("texto", "").lower()
            for a in perfil.alertas
        )

    @pytest.mark.anyio
    async def test_sem_despesas_tse_teto_none(self) -> None:
        """Sem ``total_despesas_tse_2022`` no grafo → ``teto_gastos=None``."""
        driver = _build_driver()
        legislator = _legislator_node()  # sem total_despesas_tse_2022
        record = _mock_record({"politico": legislator, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        assert perfil.teto_gastos is None

    @pytest.mark.anyio
    async def test_ultrapassou_gera_alerta_grave(self) -> None:
        """Gasto > 100% do teto → alerta ``grave`` no perfil."""
        driver = _build_driver()
        legislator = _legislator_node(
            total_despesas_tse_2022=2_500_000.0,  # 119% do teto 2.1M
            cargo_tse_2022="DEPUTADO FEDERAL",
        )
        record = _mock_record({"politico": legislator, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        assert perfil.teto_gastos is not None
        assert perfil.teto_gastos.classificacao == "ultrapassou"
        # Deve existir um alerta grave com a fonte legal citada.
        graves = [a for a in perfil.alertas if a.get("tipo") == "grave"]
        assert any(
            "23.607/2019" in a.get("texto", "") for a in graves
        ), f"Nao encontrou alerta grave com fonte legal: {graves}"


# --- 7. Roteamento federal vs estadual (verba ALEGO) -----------------------


def _state_legislator_node(
    *, legislator_id: str = "alego-xyz", uf: str = "GO",
    include_provenance: bool = True, **extra: Any,
) -> dict[str, Any]:
    """Props canônicos de um ``:StateLegislator`` (pipeline ``alego``)."""
    props: dict[str, Any] = {
        "name": "Deputado Estadual",
        "cpf": "",
        "partido": "XYZ",
        "legislature": "",
        "uf": uf,
        "legislator_id": legislator_id,
        "is_pep": False,
        "element_id": "4:state:9",
        "labels": ["StateLegislator"],
        "source": "alego",
    }
    if include_provenance:
        # Usa um source_id próprio do ALEGO pra não confundir com CEAP.
        props.update({
            "source_id": "alego",
            "source_record_id": "dep-go-42",
            "source_url": "https://transparencia.al.go.leg.br/",
            "ingested_at": "2026-04-18T00:00:00+00:00",
            "run_id": "alego_20260418000000",
            "source_snapshot_uri": "alego/2026-04/snap.json",
        })
    props.update(extra)
    return props


class TestRoteamentoDespesasEstadualGo:
    """``obter_verba_indenizatoria_alego`` é chamado quando o político é
    ``:StateLegislator`` GO, e o CEAP federal NÃO é chamado. Inversão
    simétrica no teste federal (outro grupo de testes já cobre)."""

    @pytest.mark.anyio
    async def test_estadual_go_usa_verba_alego(self) -> None:
        driver = _build_driver()
        state = _state_legislator_node(legislator_id="hash-go-1")
        record = _mock_record({"politico": state, "conexoes": []})

        verba_fake = [
            DespesaGabinete(tipo="Combustivel", total=2_000.0, total_fmt="R$ 2,0 mil"),
            DespesaGabinete(tipo="Telefone", total=400.0, total_fmt="R$ 400,00"),
        ]
        patches = _patch_ceap_and_emendas(verba_alego=verba_fake)
        ceap_patch, emendas_patch, media_patch, verba_patch, cota_gyn_patch = patches
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            ceap_patch as ceap_mock,
            emendas_patch as emendas_mock,
            media_patch,
            verba_patch as verba_mock,
            cota_gyn_patch as cota_gyn_mock,
        ):
            perfil = await obter_perfil(driver, "4:state:9")

        # Não chamou CEAP federal nem emendas federais nem cota GYN.
        ceap_mock.assert_not_awaited()
        emendas_mock.assert_not_awaited()
        cota_gyn_mock.assert_not_awaited()
        # Chamou a verba ALEGO com o legislator_id do nó focal.
        verba_mock.assert_awaited_once()
        call_args = verba_mock.await_args
        assert call_args.args[1] == "hash-go-1"

        # Despesas_gabinete populadas a partir da verba ALEGO.
        assert len(perfil.despesas_gabinete) == 2
        assert perfil.despesas_gabinete[0].tipo == "Combustivel"
        assert perfil.total_despesas_gabinete == pytest.approx(2_400.0)

        # Aviso específico da ALEGO (estadual), não o fallback federal.
        assert "ALEGO" in perfil.aviso_despesas
        assert "Camara Federal" not in perfil.aviso_despesas

    @pytest.mark.anyio
    async def test_estadual_sem_uf_go_nao_roteia(self) -> None:
        """StateLegislator de outra UF (defesa em profundidade) não chama ALEGO."""
        driver = _build_driver()
        # Hipotético estadual MG — pipeline só cria uf=GO hoje, mas o guard
        # deve bloquear de qualquer jeito (garante escopo GO-only do service).
        state = _state_legislator_node(legislator_id="mg-1", uf="MG")
        record = _mock_record({"politico": state, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        ceap_patch, emendas_patch, media_patch, verba_patch, cota_gyn_patch = patches
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            ceap_patch, emendas_patch, media_patch, verba_patch as verba_mock, cota_gyn_patch,
        ):
            perfil = await obter_perfil(driver, "4:state:9")

        verba_mock.assert_not_awaited()
        assert perfil.despesas_gabinete == []

    @pytest.mark.anyio
    async def test_federal_nao_chama_verba_alego(self) -> None:
        """Safeguard: deputado federal só usa CEAP federal, nunca a verba ALEGO
        nem cota GYN."""
        driver = _build_driver()
        legislator = _legislator_node()
        record = _mock_record({"politico": legislator, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        ceap_patch, emendas_patch, media_patch, verba_patch, cota_gyn_patch = patches
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            ceap_patch as ceap_mock,
            emendas_patch as emendas_mock,
            media_patch, verba_patch as verba_mock,
            cota_gyn_patch as cota_gyn_mock,
        ):
            await obter_perfil(driver, "4:abc:1")

        # Federal chama CEAP + emendas, nunca verba ALEGO nem cota GYN.
        ceap_mock.assert_awaited_once()
        emendas_mock.assert_awaited_once()
        verba_mock.assert_not_awaited()
        cota_gyn_mock.assert_not_awaited()


class TestAvisoDespesasDinamico:
    """``aviso_despesas`` tem 4 ramos (federal/estadual GO/municipal GYN/sem fonte)."""

    @pytest.mark.anyio
    async def test_aviso_federal_menciona_ceap(self) -> None:
        driver = _build_driver()
        legislator = _legislator_node()
        record = _mock_record({"politico": legislator, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:abc:1")

        assert "Camara Federal" in perfil.aviso_despesas
        assert "CEAP" in perfil.aviso_despesas

    @pytest.mark.anyio
    async def test_aviso_estadual_menciona_alego(self) -> None:
        driver = _build_driver()
        state = _state_legislator_node()
        record = _mock_record({"politico": state, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:state:9")

        assert "ALEGO" in perfil.aviso_despesas
        assert "Assembleia Legislativa de Goias" in perfil.aviso_despesas

    @pytest.mark.anyio
    async def test_aviso_sem_fonte_menciona_futuras(self) -> None:
        driver = _build_driver()
        pessoa = {
            "name": "Vereador Qualquer",
            "cpf": None,
            "uf": "GO",
            "role": "vereador",
            "is_pep": False,
            "element_id": "4:pes:10",
            "labels": ["Person"],
            **PROV_FIELDS,
        }
        record = _mock_record({"politico": pessoa, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:pes:10")

        assert "Ainda nao temos os dados" in perfil.aviso_despesas
        assert "ALEGO" in perfil.aviso_despesas
        # Fallback tambem menciona CMG (Goiania) como fonte municipal.
        assert "Camara Municipal de Goiania" in perfil.aviso_despesas

    @pytest.mark.anyio
    async def test_aviso_vereador_goiania_menciona_cmg(self) -> None:
        """Vereador(a) com label ``:GoVereador`` (Goiania) → aviso CMG."""
        driver = _build_driver()
        vereador = _go_vereador_node()
        record = _mock_record({"politico": vereador, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            patches[0], patches[1], patches[2], patches[3], patches[4],
        ):
            perfil = await obter_perfil(driver, "4:vereador:1")

        assert "Camara Municipal de Goiania" in perfil.aviso_despesas
        assert "CMG" in perfil.aviso_despesas
        # Aviso municipal nao deve mencionar CEAP/ALEGO como fonte primaria.
        assert "Camara Federal" not in perfil.aviso_despesas
        assert "ALEGO" not in perfil.aviso_despesas


# --- 8. Roteamento municipal GYN (cota Camara Municipal de Goiania) --------


def _go_vereador_node(
    *, vereador_id: str = "gyn-hash-1", municipality: str = "Goiania",
    include_provenance: bool = True, **extra: Any,
) -> dict[str, Any]:
    """Props canonicos de um ``:GoVereador`` (pipeline ``camara_goiania``)."""
    props: dict[str, Any] = {
        "name": "Vereador Municipal",
        "cpf": None,
        "partido": "ABC",
        "legislature": "",
        "uf": "GO",
        "municipality": municipality,
        "municipality_code": "5208707",
        "vereador_id": vereador_id,
        "is_pep": False,
        "element_id": "4:vereador:1",
        "labels": ["GoVereador"],
        "source": "camara_goiania",
    }
    if include_provenance:
        props.update({
            "source_id": "camara_goiania",
            "source_record_id": "vereador-001",
            "source_url": "https://www.goiania.go.leg.br/",
            "ingested_at": "2026-04-18T00:00:00+00:00",
            "run_id": "camara_goiania_20260418000000",
            "source_snapshot_uri": "camara_goiania/2026-04/snap.json",
        })
    props.update(extra)
    return props


class TestRoteamentoDespesasMunicipalGyn:
    """``obter_cota_vereador_goiania`` e chamado quando o politico e
    ``:GoVereador`` com ``municipality='Goiania'`` — e CEAP federal, emendas
    federais e verba ALEGO NAO sao chamados."""

    @pytest.mark.anyio
    async def test_vereador_goiania_usa_cota_cmg(self) -> None:
        driver = _build_driver()
        vereador = _go_vereador_node(vereador_id="gyn-hash-99")
        record = _mock_record({"politico": vereador, "conexoes": []})

        cota_fake = [
            DespesaGabinete(tipo="Telefone", total=900.0, total_fmt="R$ 900,00"),
            DespesaGabinete(tipo="Combustivel", total=300.0, total_fmt="R$ 300,00"),
        ]
        patches = _patch_ceap_and_emendas(cota_gyn=cota_fake)
        ceap_patch, emendas_patch, media_patch, verba_patch, cota_gyn_patch = patches
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            ceap_patch as ceap_mock,
            emendas_patch as emendas_mock,
            media_patch,
            verba_patch as verba_mock,
            cota_gyn_patch as cota_mock,
        ):
            perfil = await obter_perfil(driver, "4:vereador:1")

        # Nao chamou CEAP federal, emendas federais nem verba ALEGO.
        ceap_mock.assert_not_awaited()
        emendas_mock.assert_not_awaited()
        verba_mock.assert_not_awaited()
        # Chamou a cota CMG com o vereador_id do no focal.
        cota_mock.assert_awaited_once()
        assert cota_mock.await_args.args[1] == "gyn-hash-99"

        # Despesas_gabinete populadas a partir da cota CMG.
        assert len(perfil.despesas_gabinete) == 2
        assert perfil.despesas_gabinete[0].tipo == "Telefone"
        assert perfil.total_despesas_gabinete == pytest.approx(1_200.0)

        # Aviso especifico da CMG (municipal).
        assert "Camara Municipal de Goiania" in perfil.aviso_despesas
        assert "CMG" in perfil.aviso_despesas

    @pytest.mark.anyio
    async def test_vereador_outro_municipio_nao_roteia(self) -> None:
        """GoVereador com municipality != "Goiania" (defesa em profundidade)
        nao chama cota CMG — escopo hoje e so capital."""
        driver = _build_driver()
        vereador = _go_vereador_node(
            vereador_id="outro-1", municipality="Anapolis",
        )
        record = _mock_record({"politico": vereador, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        ceap_patch, emendas_patch, media_patch, verba_patch, cota_gyn_patch = patches
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            ceap_patch, emendas_patch, media_patch, verba_patch,
            cota_gyn_patch as cota_mock,
        ):
            perfil = await obter_perfil(driver, "4:vereador:1")

        cota_mock.assert_not_awaited()
        assert perfil.despesas_gabinete == []

    @pytest.mark.anyio
    async def test_estadual_nao_chama_cota_gyn(self) -> None:
        """Safeguard: deputado estadual GO nunca chama cota CMG."""
        driver = _build_driver()
        state = _state_legislator_node()
        record = _mock_record({"politico": state, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        ceap_patch, emendas_patch, media_patch, verba_patch, cota_gyn_patch = patches
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ),
            ceap_patch, emendas_patch, media_patch,
            verba_patch as verba_mock,
            cota_gyn_patch as cota_mock,
        ):
            await obter_perfil(driver, "4:state:9")

        verba_mock.assert_awaited_once()
        cota_mock.assert_not_awaited()


class TestCanonicalIdPassthrough:
    """``obter_perfil`` aceita ``canon_*`` IDs sem pré-processar.

    A resolução canonical → nó-fonte mais oficial mora no Cypher
    (``perfil_politico_connections.cypher``, via ``CALL { ... UNION ... }``).
    O service só passa o id adiante — essa bateria de tests trava a invariante.
    """

    @pytest.mark.anyio
    async def test_passa_canonical_id_pra_query(self) -> None:
        driver = _build_driver()
        senator = {
            "name": "JORGE KAJURU REIS DA COSTA NASSER",
            "partido": "PSB",
            "uf": "GO",
            "id_senado": "5895",
            "senator_id": "senado_5895",
            "foto_url": "http://senado/5895.jpg",
            "element_id": "4:senator:1",
            "labels": ["Senator"],
            **PROV_FIELDS,
        }
        record = _mock_record({"politico": senator, "conexoes": []})

        patches = _patch_ceap_and_emendas()
        ceap_patch, emendas_patch, media_patch, verba_patch, cota_gyn_patch = patches
        with (
            patch(
                "bracc.services.perfil_service.execute_query_single",
                new_callable=AsyncMock,
                return_value=record,
            ) as query_mock,
            ceap_patch, emendas_patch, media_patch,
            verba_patch, cota_gyn_patch,
        ):
            perfil = await obter_perfil(driver, "canon_senado_5895")

        # Query recebe o id canônico sem tradução.
        call = query_mock.await_args
        assert call.args[1] == "perfil_politico_connections"
        assert call.args[2] == {"entity_id": "canon_senado_5895"}
        # E o perfil devolve o Senator (o Cypher resolveu pro source mais oficial).
        assert perfil.politico.nome == "JORGE KAJURU REIS DA COSTA NASSER"
        assert perfil.politico.foto_url == "http://senado/5895.jpg"
        assert perfil.politico.partido == "PSB"

"""Unit tests para :mod:`bracc.services.teto_service`.

Valida:
* Classificação por percentual (ok/alto/limite/ultrapassou).
* Degradação silenciosa (None) para cargos/UFs não mapeados.
* Normalização de cargo (MAIÚSCULO / minúsculo / com acento).
* Governador depende de UF; senador/deputado federal não.
* Ano fora de 2022 retorna None (MVP).
"""

from __future__ import annotations

import pytest

from bracc.services.teto_service import calcular_teto


class TestDeputadoFederal:
    def test_dentro_faixa_ok(self) -> None:
        teto = calcular_teto(
            cargo="DEPUTADO FEDERAL",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=1_000_000.0,
        )
        assert teto is not None
        assert teto.valor_limite == 2_100_000.0
        assert teto.pct_usado == pytest.approx(47.6, abs=0.1)
        assert teto.classificacao == "ok"
        assert teto.ano_eleicao == 2022
        assert "23.607/2019" in teto.fonte_legal

    def test_alto_entre_70_e_90(self) -> None:
        # 1.8M / 2.1M = 85.7%
        teto = calcular_teto(
            cargo="deputado federal",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=1_800_000.0,
        )
        assert teto is not None
        assert teto.classificacao == "alto"
        assert teto.pct_usado_fmt == "86%"

    def test_limite_entre_90_e_100(self) -> None:
        # 2.0M / 2.1M = 95.2%
        teto = calcular_teto(
            cargo="Deputado Federal",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=2_000_000.0,
        )
        assert teto is not None
        assert teto.classificacao == "limite"

    def test_ultrapassou_acima_100(self) -> None:
        # 2.5M / 2.1M = 119%
        teto = calcular_teto(
            cargo="DEPUTADO FEDERAL",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=2_500_000.0,
        )
        assert teto is not None
        assert teto.classificacao == "ultrapassou"
        assert teto.pct_usado > 100

    def test_uf_nao_importa_para_federal(self) -> None:
        """Deputado federal tem teto nacional — UF não afeta resultado."""
        t_go = calcular_teto("DEPUTADO FEDERAL", "GO", 2022, 1_000_000.0)
        t_sp = calcular_teto("DEPUTADO FEDERAL", "SP", 2022, 1_000_000.0)
        assert t_go is not None
        assert t_sp is not None
        assert t_go.valor_limite == t_sp.valor_limite


class TestGovernador:
    def test_governador_go_ultrapassou(self) -> None:
        # 25M / 21M = 119%
        teto = calcular_teto(
            cargo="GOVERNADOR",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=25_000_000.0,
        )
        assert teto is not None
        assert teto.valor_limite == 21_000_000.0
        assert teto.classificacao == "ultrapassou"
        assert teto.pct_usado_fmt == "119%"

    def test_governador_sem_uf_retorna_none(self) -> None:
        assert calcular_teto("Governador", None, 2022, 5_000_000.0) is None

    def test_governador_uf_nao_mapeada_retorna_none(self) -> None:
        """AP não está na tabela hardcoded — degradação silenciosa."""
        assert calcular_teto("GOVERNADOR", "AP", 2022, 5_000_000.0) is None

    def test_vice_governador_usa_teto_governador(self) -> None:
        """Vice- segue o mesmo teto do titular."""
        teto = calcular_teto(
            cargo="vice-governador",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=10_000_000.0,
        )
        assert teto is not None
        assert teto.valor_limite == 21_000_000.0


class TestSenador:
    def test_senador_nacional(self) -> None:
        teto = calcular_teto("SENADOR", "GO", 2022, 2_000_000.0)
        assert teto is not None
        assert teto.valor_limite == 5_000_000.0
        assert teto.classificacao == "ok"


class TestDeputadoEstadual:
    def test_deputado_estadual(self) -> None:
        teto = calcular_teto("DEPUTADO ESTADUAL", "GO", 2022, 500_000.0)
        assert teto is not None
        assert teto.valor_limite == 1_050_000.0
        assert teto.classificacao == "ok"


class TestDegradacaoSilenciosa:
    def test_cargo_none(self) -> None:
        assert calcular_teto(None, "GO", 2022, 1_000_000.0) is None

    def test_cargo_vazio(self) -> None:
        assert calcular_teto("", "GO", 2022, 1_000_000.0) is None

    def test_cargo_desconhecido(self) -> None:
        """Cargo não-eleitoral (ex.: servidor público) → None."""
        assert calcular_teto(
            "SECRETARIO DE FAZENDA", "GO", 2022, 1_000_000.0,
        ) is None

    def test_prefeito_sem_mapeamento(self) -> None:
        """Prefeito tem teto municipal — fora do MVP, degrada pra None."""
        assert calcular_teto("PREFEITO", "GO", 2022, 500_000.0) is None

    def test_vereador_sem_mapeamento(self) -> None:
        assert calcular_teto("VEREADOR", "GO", 2022, 100_000.0) is None

    def test_despesas_zero_retorna_none(self) -> None:
        """Candidato sem gasto declarado → 0% seria enganoso, omite."""
        assert calcular_teto("DEPUTADO FEDERAL", "GO", 2022, 0.0) is None

    def test_despesas_negativas_retorna_none(self) -> None:
        assert calcular_teto("DEPUTADO FEDERAL", "GO", 2022, -100.0) is None

    def test_ano_nao_coberto_retorna_none(self) -> None:
        """MVP só cobre 2022 — outros anos devolvem None até calibrarmos."""
        assert calcular_teto(
            "DEPUTADO FEDERAL", "GO", 2026, 1_000_000.0,
        ) is None
        assert calcular_teto(
            "DEPUTADO FEDERAL", "GO", 2018, 1_000_000.0,
        ) is None


class TestNormalizacao:
    def test_cargo_com_acento(self) -> None:
        # "DEPUTADO FEDERAL" acentuado não existe mas testa normalização.
        teto = calcular_teto("Deputádo Fédéral", "GO", 2022, 1_000_000.0)
        assert teto is not None
        assert teto.valor_limite == 2_100_000.0

    def test_cargo_lowercase(self) -> None:
        teto = calcular_teto("deputado federal", "GO", 2022, 1_000_000.0)
        assert teto is not None

    def test_uf_lowercase(self) -> None:
        teto = calcular_teto("GOVERNADOR", "go", 2022, 10_000_000.0)
        assert teto is not None
        assert teto.valor_limite == 21_000_000.0


class TestFormatacao:
    def test_valores_formatados_em_brl(self) -> None:
        teto = calcular_teto("DEPUTADO FEDERAL", "GO", 2022, 1_800_000.0)
        assert teto is not None
        assert "R$" in teto.valor_limite_fmt
        assert "R$" in teto.valor_gasto_fmt
        assert teto.pct_usado_fmt.endswith("%")

    def test_pct_usado_fmt_sem_decimais(self) -> None:
        """Formato '87%' (inteiro) — não '87.3%'."""
        teto = calcular_teto("DEPUTADO FEDERAL", "GO", 2022, 1_830_000.0)
        assert teto is not None
        # 1.83M / 2.1M = 87.14% → arredondado pra "87%"
        assert teto.pct_usado_fmt == "87%"

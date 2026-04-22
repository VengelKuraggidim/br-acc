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
            total_despesas_declaradas=1_500_000.0,
        )
        assert teto is not None
        assert teto.valor_limite == 3_176_572.53
        # 1.5M / 3.176M ≈ 47.2%
        assert teto.pct_usado == pytest.approx(47.2, abs=0.2)
        assert teto.classificacao == "ok"
        assert teto.ano_eleicao == 2022
        assert "23.607/2019" in teto.fonte_legal

    def test_alto_entre_70_e_90(self) -> None:
        # 2.7M / 3.176M ≈ 85%
        teto = calcular_teto(
            cargo="deputado federal",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=2_700_000.0,
        )
        assert teto is not None
        assert teto.classificacao == "alto"

    def test_limite_entre_90_e_100(self) -> None:
        # 3.0M / 3.176M ≈ 94.4%
        teto = calcular_teto(
            cargo="Deputado Federal",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=3_000_000.0,
        )
        assert teto is not None
        assert teto.classificacao == "limite"

    def test_ultrapassou_acima_100(self) -> None:
        # 3.8M / 3.176M ≈ 119.6%
        teto = calcular_teto(
            cargo="DEPUTADO FEDERAL",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=3_800_000.0,
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
        # 13.66M / 11.48M ≈ 119%
        teto = calcular_teto(
            cargo="GOVERNADOR",
            uf="GO",
            ano_eleicao=2022,
            total_despesas_declaradas=13_660_000.0,
        )
        assert teto is not None
        assert teto.valor_limite == 11_480_000.0
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
            total_despesas_declaradas=5_000_000.0,
        )
        assert teto is not None
        assert teto.valor_limite == 11_480_000.0


class TestSenador:
    def test_senador_go(self) -> None:
        # GO: R$ 4.4M (Portaria TSE 647/2022).
        teto = calcular_teto("SENADOR", "GO", 2022, 2_000_000.0)
        assert teto is not None
        assert teto.valor_limite == 4_400_000.0
        assert teto.classificacao == "ok"

    def test_senador_sem_uf_retorna_none(self) -> None:
        """Senador varia por UF; sem UF, degrada pra None."""
        assert calcular_teto("SENADOR", None, 2022, 2_000_000.0) is None

    def test_senador_uf_nao_mapeada_retorna_none(self) -> None:
        assert calcular_teto("SENADOR", "AP", 2022, 2_000_000.0) is None


class TestDeputadoEstadual:
    def test_deputado_estadual_go(self) -> None:
        # GO: R$ 1.26M (Portaria TSE 647/2022).
        teto = calcular_teto("DEPUTADO ESTADUAL", "GO", 2022, 500_000.0)
        assert teto is not None
        assert teto.valor_limite == 1_260_000.0
        assert teto.classificacao == "ok"

    def test_deputado_estadual_sem_uf_retorna_none(self) -> None:
        assert calcular_teto("DEPUTADO ESTADUAL", None, 2022, 500_000.0) is None

    def test_deputado_estadual_uf_nao_mapeada_retorna_none(self) -> None:
        assert calcular_teto("DEPUTADO ESTADUAL", "AP", 2022, 500_000.0) is None


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

    def test_ano_nao_coberto_loga_warning_com_year_explicito(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Ano fora da tabela deve deixar trilha no log.

        Rationale: em 2026, quando o pipeline começar a emitir
        ``ano_eleicao=2026`` o card "teto" vai sumir silenciosamente do
        PWA. O warning aqui é o canary — o operador vê no log antes da
        primeira request real. Teste-fonte pra garantir que a mensagem
        cita o ano explicitamente (não um genérico "ano nao mapeado").
        """
        with caplog.at_level("WARNING", logger="bracc.services.teto_service"):
            assert (
                calcular_teto("DEPUTADO FEDERAL", "GO", 2026, 1_000_000.0)
                is None
            )
        assert any(
            "2026" in rec.getMessage() and "TETOS" in rec.getMessage()
            for rec in caplog.records
        ), f"esperava warning citando 2026 + TETOS; vi: {[r.getMessage() for r in caplog.records]}"


class TestNormalizacao:
    def test_cargo_com_acento(self) -> None:
        # "DEPUTADO FEDERAL" acentuado não existe mas testa normalização.
        teto = calcular_teto("Deputádo Fédéral", "GO", 2022, 1_000_000.0)
        assert teto is not None
        assert teto.valor_limite == 3_176_572.53

    def test_cargo_lowercase(self) -> None:
        teto = calcular_teto("deputado federal", "GO", 2022, 1_000_000.0)
        assert teto is not None

    def test_uf_lowercase(self) -> None:
        teto = calcular_teto("GOVERNADOR", "go", 2022, 5_000_000.0)
        assert teto is not None
        assert teto.valor_limite == 11_480_000.0


class TestFormatacao:
    def test_valores_formatados_em_brl(self) -> None:
        teto = calcular_teto("DEPUTADO FEDERAL", "GO", 2022, 1_800_000.0)
        assert teto is not None
        assert "R$" in teto.valor_limite_fmt
        assert "R$" in teto.valor_gasto_fmt
        assert teto.pct_usado_fmt.endswith("%")

    def test_pct_usado_fmt_sem_decimais(self) -> None:
        """Formato '87%' (inteiro) — não '87.3%'."""
        # 2.77M / 3.176M ≈ 87.2% → arredondado pra "87%"
        teto = calcular_teto("DEPUTADO FEDERAL", "GO", 2022, 2_770_000.0)
        assert teto is not None
        assert teto.pct_usado_fmt == "87%"

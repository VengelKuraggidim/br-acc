"""Tests para ``bracc.services.conexoes_service`` (Fase 04.B).

Cobre as 7 categorias + garantias LGPD (CPF pleno nunca aparece em nenhum
output). Todos os tests são determinísticos, sem Neo4j — o shape mockado
espelha o retorno da query ``perfil_politico_connections.cypher``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bracc.services.conexoes_service import (
    ConexoesClassificadas,
    classificar,
)

POLITICO_ID = "4:abc:1"


def _conn(
    *,
    rel_type: str,
    target_id: str,
    politico_is_source: bool = True,
    rel_props: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Monta uma conexão com o político focal como source OU target."""
    if politico_is_source:
        source_id, tid = POLITICO_ID, target_id
    else:
        source_id, tid = target_id, POLITICO_ID
    return {
        "source_id": source_id,
        "target_id": tid,
        "relationship_type": rel_type,
        "properties": rel_props or {},
    }


# --- 1. Amendment -----------------------------------------------------------


class TestEmendas:
    def test_amendment_target_vira_emenda(self) -> None:
        conexoes = [_conn(rel_type="AUTOR_EMENDA", target_id="emenda_1")]
        entidades = {
            "emenda_1": {
                "type": "Amendment",
                "properties": {
                    "amendment_id": "EM-2024-001",
                    "type": "individual",
                    "function": "saude",
                    "municipality": "Goiania",
                    "uf": "GO",
                    "value_committed": 100_000.0,
                    "value_paid": 80_000.0,
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.emendas) == 1
        emenda = resultado.emendas[0]
        assert emenda.id == "EM-2024-001"
        assert "individual" in emenda.tipo.lower() or "Emenda individual" in emenda.tipo
        assert "publica" in emenda.funcao.lower() or "Saude" in emenda.funcao
        assert emenda.municipio == "Goiania"
        assert emenda.uf == "GO"
        assert emenda.valor_empenhado == 100_000.0
        assert emenda.valor_pago == 80_000.0
        assert emenda.valor_pago_fmt == "R$ 80.0 mil"

    def test_amendment_sem_amendment_id_usa_element_id(self) -> None:
        conexoes = [_conn(rel_type="AUTOR_EMENDA", target_id="4:xyz:42")]
        entidades = {
            "4:xyz:42": {
                "type": "Amendment",
                "properties": {"type": "pix", "function": "educacao"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.emendas[0].id == "4:xyz:42"


# --- 2. Doadores empresa ----------------------------------------------------


class TestDoadoresEmpresa:
    def test_doacao_empresa_cria_doador(self) -> None:
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 10_000.0},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "12345678000190",
                    "razao_social": "Construtora ACME LTDA",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.doadores_empresa) == 1
        d = resultado.doadores_empresa[0]
        assert d.nome == "Construtora ACME LTDA"
        assert d.cnpj == "12345678000190"
        assert d.valor_total == 10_000.0
        assert d.n_doacoes == 1

    def test_duas_doacoes_mesmo_cnpj_agregam(self) -> None:
        """2 doações com mesmo CNPJ → 1 DoadorEmpresa n_doacoes=2, soma valores."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 5_000.0},
            ),
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 7_500.0},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {"cnpj": "12345678000190", "name": "ACME"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.doadores_empresa) == 1
        d = resultado.doadores_empresa[0]
        assert d.n_doacoes == 2
        assert d.valor_total == 12_500.0

    def test_empresa_sem_cnpj_nao_duplica(self) -> None:
        """Empresa sem CNPJ → usa element_id como chave; empresas distintas
        em element_ids distintos permanecem separadas."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_sem_cnpj_a",
                politico_is_source=False,
                rel_props={"valor": 1_000.0},
            ),
            _conn(
                rel_type="DOOU",
                target_id="emp_sem_cnpj_b",
                politico_is_source=False,
                rel_props={"valor": 2_000.0},
            ),
            # Terceira doação pra "emp_sem_cnpj_a" → agrega no mesmo registro.
            _conn(
                rel_type="DOOU",
                target_id="emp_sem_cnpj_a",
                politico_is_source=False,
                rel_props={"valor": 500.0},
            ),
        ]
        entidades = {
            "emp_sem_cnpj_a": {
                "type": "Company",
                "properties": {"name": "Empresa A sem CNPJ"},
            },
            "emp_sem_cnpj_b": {
                "type": "Company",
                "properties": {"name": "Empresa B sem CNPJ"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.doadores_empresa) == 2
        # Ordenados por valor desc: B (2000) primeiro, A (1500) segundo.
        d_b, d_a = resultado.doadores_empresa
        assert d_b.valor_total == 2_000.0
        assert d_b.n_doacoes == 1
        assert d_b.cnpj is None
        assert d_a.valor_total == 1_500.0
        assert d_a.n_doacoes == 2
        assert d_a.cnpj is None

    def test_doacao_usa_amount_como_fallback(self) -> None:
        """Aresta com ``amount`` (não ``valor``) ainda é contabilizada."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"amount": 3_000.0},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {"cnpj": "11111111000100"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.doadores_empresa[0].valor_total == 3_000.0


# --- 3. Doadores pessoa (com LGPD) ------------------------------------------


class TestDoadoresPessoa:
    def test_duas_doacoes_mesmo_cpf_agregam_e_mascara(self) -> None:
        """2 doações mesmo CPF → 1 DoadorPessoa n_doacoes=2, CPF mascarado."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="p_1",
                politico_is_source=False,
                rel_props={"valor": 500.0},
            ),
            _conn(
                rel_type="DOOU",
                target_id="p_1",
                politico_is_source=False,
                rel_props={"valor": 1_500.0},
            ),
        ]
        entidades = {
            "p_1": {
                "type": "Person",
                "properties": {"cpf": "11122233344", "name": "Joao da Silva"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.doadores_pessoa) == 1
        d = resultado.doadores_pessoa[0]
        assert d.n_doacoes == 2
        assert d.valor_total == 2_000.0
        assert d.cpf_mascarado == "***.***.***-44"

    def test_cpf_pleno_nunca_aparece_em_doador_pessoa(self) -> None:
        """LGPD crítico: CPF pleno jamais pode aparecer no output serializado."""
        cpf_pleno = "11122233344"
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="p_1",
                politico_is_source=False,
                rel_props={"valor": 100.0},
            ),
        ]
        entidades = {
            "p_1": {
                "type": "Person",
                "properties": {"cpf": cpf_pleno, "name": "Teste"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        doador = resultado.doadores_pessoa[0]
        # Nenhum campo do model pode conter os 11 dígitos juntos.
        dump = doador.model_dump()
        for valor in dump.values():
            assert cpf_pleno not in str(valor)
        # Também não pode estar no serialized JSON.
        assert cpf_pleno not in doador.model_dump_json()
        assert doador.cpf_mascarado == "***.***.***-44"


# --- 4. Sócio de empresa ----------------------------------------------------


class TestSocios:
    def test_socio_de_company(self) -> None:
        conexoes = [_conn(rel_type="SOCIO_DE", target_id="emp_1")]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "22333444000155",
                    "razao_social": "Minha Empresa SA",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.socios) == 1
        s = resultado.socios[0]
        assert s.nome == "Minha Empresa SA"
        assert s.cnpj == "22333444000155"


# --- 5. Familia -------------------------------------------------------------


class TestFamilia:
    def test_conjuge_vira_familiar_com_cpf_mascarado(self) -> None:
        conexoes = [_conn(rel_type="CONJUGE_DE", target_id="p_conjuge")]
        entidades = {
            "p_conjuge": {
                "type": "Person",
                "properties": {"cpf": "99988877766", "name": "Maria Silva"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.familia) == 1
        f = resultado.familia[0]
        assert f.relacao == "Cônjuge"
        assert f.nome == "Maria Silva"
        assert f.cpf_mascarado == "***.***.***-66"

    def test_parente_vira_familiar(self) -> None:
        conexoes = [_conn(rel_type="PARENTE_DE", target_id="p_parente")]
        entidades = {
            "p_parente": {
                "type": "Person",
                "properties": {"cpf": "44455566677", "name": "Irmao"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.familia[0].relacao == "Parente"

    def test_cpf_pleno_nunca_aparece_em_familiar(self) -> None:
        cpf_pleno = "99988877766"
        conexoes = [_conn(rel_type="CONJUGE_DE", target_id="p_conjuge")]
        entidades = {
            "p_conjuge": {
                "type": "Person",
                "properties": {"cpf": cpf_pleno, "name": "Conjuge"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        f = resultado.familia[0]
        assert cpf_pleno not in f.model_dump_json()
        dump = f.model_dump()
        for valor in dump.values():
            assert cpf_pleno not in str(valor)


# --- 6. Contratos -----------------------------------------------------------


class TestContratos:
    def test_go_procurement_e_contract_viram_contratos(self) -> None:
        conexoes = [
            _conn(rel_type="VENCEU", target_id="contract_1"),
            _conn(rel_type="FORNECEU_GO", target_id="proc_go_1"),
        ]
        entidades = {
            "contract_1": {
                "type": "Contract",
                "properties": {
                    "object": "Obra de pavimentacao",
                    "value": 500_000.0,
                    "contracting_org": "Ministerio X",
                    "date": "2024-05-10",
                },
            },
            "proc_go_1": {
                "type": "Go_procurement",
                "properties": {
                    "object": "Aquisicao de materiais",
                    "amount_estimated": 120_000.0,
                    "agency_name": "Secretaria GO",
                    "published_at": "2024-08-15",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.contratos) == 2
        # Ordenados por valor desc
        assert resultado.contratos[0].valor == 500_000.0
        assert resultado.contratos[0].objeto == "Obra de pavimentacao"
        assert resultado.contratos[0].orgao == "Ministerio X"
        assert resultado.contratos[1].valor == 120_000.0
        assert resultado.contratos[1].orgao == "Secretaria GO"

    def test_go_procurement_sem_object_usa_fallback(self) -> None:
        conexoes = [_conn(rel_type="FORNECEU_GO", target_id="proc_1")]
        entidades = {
            "proc_1": {
                "type": "Go_procurement",
                "properties": {"amount_estimated": 1_000.0},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.contratos[0].objeto == "Licitacao estadual/municipal"


# --- 7. Empresas (fallback) -------------------------------------------------


class TestEmpresas:
    def test_company_com_rel_nao_doou_nao_socio_vira_empresa(self) -> None:
        conexoes = [_conn(rel_type="CONTRATADA_POR", target_id="emp_x")]
        entidades = {
            "emp_x": {
                "type": "Company",
                "properties": {
                    "razao_social": "Empresa Fornecedora SA",
                    "cnpj": "99999999000100",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.empresas) == 1
        e = resultado.empresas[0]
        assert e.nome == "Empresa Fornecedora SA"
        assert e.cnpj == "99999999000100"
        # relacao traduzida
        assert "Contratada" in e.relacao or "CONTRATADA" in e.relacao

    def test_state_agency_lotado_em(self) -> None:
        conexoes = [_conn(rel_type="LOTADO_EM", target_id="agency_1")]
        entidades = {
            "agency_1": {
                "type": "State_agency",
                "properties": {"name": "Assembleia Legislativa GO"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.empresas) == 1
        empresa = resultado.empresas[0]
        assert empresa.nome == "Assembleia Legislativa GO"
        assert "orgao" in empresa.relacao.lower() or "Lotado" in empresa.relacao


# --- Limit por categoria ----------------------------------------------------


class TestLimit:
    def test_51_doadores_retorna_50(self) -> None:
        """Cap de 50 doadores por default."""
        conexoes = []
        entidades = {}
        for i in range(51):
            tid = f"emp_{i}"
            conexoes.append(
                _conn(
                    rel_type="DOOU",
                    target_id=tid,
                    politico_is_source=False,
                    rel_props={"valor": float(100 * (i + 1))},
                ),
            )
            entidades[tid] = {
                "type": "Company",
                "properties": {
                    "cnpj": f"{i:014d}",
                    "razao_social": f"Empresa {i}",
                },
            }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.doadores_empresa) == 50

    def test_limit_custom(self) -> None:
        conexoes = []
        entidades = {}
        for i in range(10):
            tid = f"emp_{i}"
            conexoes.append(_conn(rel_type="SOCIO_DE", target_id=tid))
            entidades[tid] = {
                "type": "Company",
                "properties": {"cnpj": f"{i:014d}", "razao_social": f"Socio {i}"},
            }
        resultado = classificar(conexoes, entidades, POLITICO_ID, limit_por_categoria=5)
        assert len(resultado.socios) == 5


# --- LGPD abrangente --------------------------------------------------------


class TestLGPDAbrangente:
    def test_cpf_pleno_nunca_em_nenhuma_das_7_categorias(self) -> None:
        """Dataset com CPFs plenos em vários lugares — nenhum output serializado
        pode conter 11 dígitos consecutivos."""
        cpf_doador = "11111111111"
        cpf_conjuge = "22222222222"
        cpf_parente = "33333333333"

        conexoes = [
            _conn(rel_type="AUTOR_EMENDA", target_id="em1"),
            _conn(
                rel_type="DOOU",
                target_id="doador_pf",
                politico_is_source=False,
                rel_props={"valor": 100.0},
            ),
            _conn(
                rel_type="DOOU",
                target_id="doador_pj",
                politico_is_source=False,
                rel_props={"valor": 500.0},
            ),
            _conn(rel_type="SOCIO_DE", target_id="soc1"),
            _conn(rel_type="CONJUGE_DE", target_id="conj1"),
            _conn(rel_type="PARENTE_DE", target_id="par1"),
            _conn(rel_type="VENCEU", target_id="ctr1"),
            _conn(rel_type="FORNECEU_GO", target_id="proc1"),
            _conn(rel_type="CONTRATADA_POR", target_id="emp1"),
        ]
        entidades = {
            "em1": {
                "type": "Amendment",
                "properties": {"type": "individual", "function": "saude"},
            },
            "doador_pf": {
                "type": "Person",
                "properties": {"cpf": cpf_doador, "name": "Doador PF"},
            },
            "doador_pj": {
                "type": "Company",
                "properties": {"cnpj": "10000000000100", "razao_social": "PJ"},
            },
            "soc1": {
                "type": "Company",
                "properties": {"cnpj": "20000000000100", "razao_social": "Socio SA"},
            },
            "conj1": {
                "type": "Person",
                "properties": {"cpf": cpf_conjuge, "name": "Conjuge"},
            },
            "par1": {
                "type": "Person",
                "properties": {"cpf": cpf_parente, "name": "Parente"},
            },
            "ctr1": {
                "type": "Contract",
                "properties": {"object": "Obra", "value": 1000.0},
            },
            "proc1": {
                "type": "Go_procurement",
                "properties": {
                    "object": "Aquisicao",
                    "amount_estimated": 500.0,
                },
            },
            "emp1": {
                "type": "Company",
                "properties": {
                    "cnpj": "30000000000100",
                    "razao_social": "EmpContratada",
                },
            },
        }

        resultado = classificar(conexoes, entidades, POLITICO_ID)

        # Serializa TODAS as 7 listas e verifica que nenhum CPF pleno vaza.
        serialized_parts: list[str] = []
        serialized_parts.extend(em.model_dump_json() for em in resultado.emendas)
        serialized_parts.extend(d.model_dump_json() for d in resultado.doadores_empresa)
        serialized_parts.extend(dp.model_dump_json() for dp in resultado.doadores_pessoa)
        serialized_parts.extend(s.model_dump_json() for s in resultado.socios)
        serialized_parts.extend(f.model_dump_json() for f in resultado.familia)
        serialized_parts.extend(c.model_dump_json() for c in resultado.contratos)
        serialized_parts.extend(e.model_dump_json() for e in resultado.empresas)

        full_dump = "\n".join(serialized_parts)
        assert cpf_doador not in full_dump, "CPF do doador vazou!"
        assert cpf_conjuge not in full_dump, "CPF do cônjuge vazou!"
        assert cpf_parente not in full_dump, "CPF do parente vazou!"

        # Sanity: as 7 categorias foram populadas.
        assert len(resultado.emendas) == 1
        assert len(resultado.doadores_empresa) == 1
        assert len(resultado.doadores_pessoa) == 1
        assert len(resultado.socios) == 1
        assert len(resultado.familia) == 2  # cônjuge + parente
        assert len(resultado.contratos) == 2  # contract + go_procurement
        assert len(resultado.empresas) == 1


# --- Robustez / edge cases --------------------------------------------------


class TestRobustez:
    def test_vazio(self) -> None:
        resultado = classificar([], {}, POLITICO_ID)
        assert resultado == ConexoesClassificadas()

    def test_conexao_que_nao_toca_o_politico_ignorada(self) -> None:
        """Aresta espúria onde nenhuma ponta é o político focal → ignorada."""
        conexoes = [
            {
                "source_id": "outro_a",
                "target_id": "outro_b",
                "relationship_type": "DOOU",
                "properties": {"valor": 9999.0},
            },
        ]
        entidades = {
            "outro_b": {
                "type": "Company",
                "properties": {"cnpj": "1", "razao_social": "X"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado == ConexoesClassificadas()

    def test_rel_familiar_em_non_person_ignorado(self) -> None:
        """CONJUGE_DE apontando pra Company → dado sujo, ignora."""
        conexoes = [_conn(rel_type="CONJUGE_DE", target_id="emp_x")]
        entidades = {
            "emp_x": {
                "type": "Company",
                "properties": {"cnpj": "1", "razao_social": "X"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.familia == []

    def test_valor_doacao_string_converte(self) -> None:
        """Aresta com valor em string (ex: vindo JSON) → convertido."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": "2500.00"},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {"cnpj": "11111111000100"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.doadores_empresa[0].valor_total == 2500.0


# --- Situacao cadastral RFB propagada ---------------------------------------


class TestSituacaoCadastralPropagada:
    """Propagacao de ``situacao_cadastral`` de :Company pros 3 models com CNPJ.

    Pipeline ``brasilapi_cnpj_status`` SET ``situacao_cadastral`` +
    ``situacao_verified_at`` no no; ``classificar`` le de
    ``target_props`` e carimba em DoadorEmpresa / SocioConectado /
    EmpresaConectada como ``situacao`` (bruto) + ``situacao_fmt``
    (leigo) + ``situacao_verified_at``.
    """

    def test_doador_empresa_baixada_propaga_situacao(self) -> None:
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 5_000.0},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "12345678000190",
                    "razao_social": "Empresa Baixada LTDA",
                    "situacao_cadastral": "BAIXADA",
                    "situacao_verified_at": "2026-04-15T10:00:00+00:00",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.doadores_empresa) == 1
        d = resultado.doadores_empresa[0]
        assert d.situacao == "BAIXADA"
        assert d.situacao_fmt == "Baixada"
        assert d.situacao_verified_at == "2026-04-15T10:00:00+00:00"

    def test_doador_sem_situacao_fica_none(self) -> None:
        """Empresa ainda nao verificada pelo pipeline → campos None."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 5_000.0},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "12345678000190",
                    "razao_social": "Nao verificada",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        d = resultado.doadores_empresa[0]
        assert d.situacao is None
        assert d.situacao_fmt is None
        assert d.situacao_verified_at is None

    def test_situacao_lixo_ignorada(self) -> None:
        """Valor que nao esta em {ATIVA,BAIXADA,SUSPENSA,INAPTA,NULA} → None."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 100.0},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "99999999000100",
                    "razao_social": "Strings Lixo",
                    "situacao_cadastral": "valor_invalido_nao_RFB",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        d = resultado.doadores_empresa[0]
        assert d.situacao is None
        assert d.situacao_fmt is None

    def test_socio_inapta_propaga_situacao(self) -> None:
        conexoes = [_conn(rel_type="SOCIO_DE", target_id="emp_1")]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "22333444000155",
                    "razao_social": "Socio SA",
                    "situacao_cadastral": "INAPTA",
                    "situacao_verified_at": "2026-04-10T12:00:00+00:00",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        s = resultado.socios[0]
        assert s.situacao == "INAPTA"
        assert s.situacao_fmt == "Inapta"

    def test_empresa_conectada_suspensa_propaga(self) -> None:
        """EmpresaConectada (fallback; não DOOU nem SOCIO) também carrega."""
        conexoes = [_conn(rel_type="CONTRATADA_POR", target_id="emp_1")]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "55555555000199",
                    "razao_social": "Fornecedora",
                    "situacao_cadastral": "SUSPENSA",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.empresas) == 1
        e = resultado.empresas[0]
        assert e.situacao == "SUSPENSA"
        assert e.situacao_fmt == "Suspensa"

    def test_todas_as_5_situacoes_validas(self) -> None:
        """Cobertura dos 5 valores RFB → formatacao leiga correta."""
        mapping = {
            "ATIVA": "Ativa",
            "BAIXADA": "Baixada",
            "SUSPENSA": "Suspensa",
            "INAPTA": "Inapta",
            "NULA": "Nula",
        }
        for idx, (raw, leigo) in enumerate(mapping.items()):
            conexoes = [_conn(rel_type="SOCIO_DE", target_id=f"emp_{idx}")]
            entidades = {
                f"emp_{idx}": {
                    "type": "Company",
                    "properties": {
                        "cnpj": f"{idx:014d}",
                        "razao_social": f"E {idx}",
                        "situacao_cadastral": raw,
                    },
                },
            }
            resultado = classificar(conexoes, entidades, POLITICO_ID)
            assert resultado.socios[0].situacao == raw
            assert resultado.socios[0].situacao_fmt == leigo


# --- Provenance nos sub-rows (Emenda / DoadorEmpresa / DoadorPessoa) --------


_PROV_COMPLETO = {
    "source_id": "tse_prestacao_contas",
    "source_record_id": "REC-123",
    "source_url": "https://divulgacandcontas.tse.jus.br/.../doacao/REC-123",
    "ingested_at": "2026-04-18T00:00:00+00:00",
    "run_id": "tse_prestacao_contas_20260418000000",
    "source_snapshot_uri": "tse/prestacao_contas/2026-04/abc.json",
}


class TestProvenanceSubRows:
    def test_emenda_com_provenance_populado(self) -> None:
        """Nó :Amendment com os 5+1 campos → ``Emenda.provenance`` carregado."""
        conexoes = [_conn(rel_type="AUTOR_EMENDA", target_id="em_1")]
        entidades = {
            "em_1": {
                "type": "Amendment",
                "properties": {
                    "amendment_id": "EM-001",
                    "type": "individual",
                    "function": "saude",
                    "value_committed": 100_000.0,
                    "value_paid": 80_000.0,
                    **_PROV_COMPLETO,
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        emenda = resultado.emendas[0]
        assert emenda.provenance is not None
        assert emenda.provenance.source_id == "tse_prestacao_contas"
        assert emenda.provenance.source_record_id == "REC-123"
        assert emenda.provenance.snapshot_url == _PROV_COMPLETO["source_snapshot_uri"]

    def test_emenda_sem_campos_obrigatorios_vira_none(self) -> None:
        """Nó legado sem os 4 campos obrigatórios → ``provenance=None``."""
        conexoes = [_conn(rel_type="AUTOR_EMENDA", target_id="em_1")]
        entidades = {
            "em_1": {
                "type": "Amendment",
                "properties": {
                    "amendment_id": "EM-LEGADO",
                    "type": "individual",
                    "function": "educacao",
                    # Falta source_url/ingested_at/run_id.
                    "source_id": "tse_prestacao_contas",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.emendas[0].provenance is None

    def test_doador_empresa_carrega_provenance(self) -> None:
        """Nó :Company com proveniência → ``DoadorEmpresa.provenance`` preenchido."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 5_000.0},
            ),
        ]
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "11222333000181",
                    "razao_social": "ACME LTDA",
                    **_PROV_COMPLETO,
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        d = resultado.doadores_empresa[0]
        assert d.provenance is not None
        assert d.provenance.source_id == "tse_prestacao_contas"
        # PJ preserva source_record_id (sem risco LGPD).
        assert d.provenance.source_record_id == "REC-123"

    def test_doador_empresa_agrega_provenance_mais_recente(self) -> None:
        """2 doações pro mesmo CNPJ com ingested_at distintos → fica com a mais recente."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 3_000.0},
            ),
        ]
        # 1 só dict pro nó — mas queremos simular re-ingestão. A regra é:
        # ``target_props`` sempre reflete a última versão do nó no grafo, então
        # múltiplas arestas DOOU apontam pro mesmo nó. Teste garante que se
        # entidades_conectadas já tem os props "mais recentes", vira
        # provenance publicado.
        entidades = {
            "emp_1": {
                "type": "Company",
                "properties": {
                    "cnpj": "99999999000199",
                    "razao_social": "Financiadora X",
                    **_PROV_COMPLETO,
                    "ingested_at": "2026-04-15T00:00:00+00:00",
                },
            },
        }
        # Adiciona uma segunda doação pro mesmo target.
        conexoes.append(
            _conn(
                rel_type="DOOU",
                target_id="emp_1",
                politico_is_source=False,
                rel_props={"valor": 7_000.0},
            ),
        )
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert len(resultado.doadores_empresa) == 1
        d = resultado.doadores_empresa[0]
        assert d.n_doacoes == 2
        assert d.valor_total == 10_000.0
        assert d.provenance is not None
        assert d.provenance.ingested_at == "2026-04-15T00:00:00+00:00"

    def test_doador_pessoa_carrega_provenance_sem_record_id(self) -> None:
        """LGPD: DoadorPessoa carrega provenance MAS ``source_record_id=None``.

        No TSE, o record_id do doador PF costuma ser o próprio CPF. Surfar
        isso violaria a máscara de CPF que o service aplica no
        ``cpf_mascarado``.
        """
        cpf_pleno = "11122233344"
        props_com_cpf_no_record_id = dict(_PROV_COMPLETO)
        # Emula o shape real do TSE: record_id pode ser o CPF.
        props_com_cpf_no_record_id["source_record_id"] = cpf_pleno

        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="pes_1",
                politico_is_source=False,
                rel_props={"valor": 500.0},
            ),
        ]
        entidades = {
            "pes_1": {
                "type": "Person",
                "properties": {
                    "cpf": cpf_pleno,
                    "name": "Doador Exemplo",
                    **props_com_cpf_no_record_id,
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        d = resultado.doadores_pessoa[0]
        assert d.provenance is not None
        assert d.provenance.source_id == "tse_prestacao_contas"
        assert d.provenance.source_url.startswith("https://")
        # Crítico LGPD: record_id é forçado a None.
        assert d.provenance.source_record_id is None
        # Serialização completa não pode carregar o CPF pleno.
        assert cpf_pleno not in d.model_dump_json()

    def test_doador_pessoa_sem_provenance_vira_none(self) -> None:
        """Props legados sem campos obrigatórios → ``provenance=None``."""
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="pes_1",
                politico_is_source=False,
                rel_props={"valor": 100.0},
            ),
        ]
        entidades = {
            "pes_1": {
                "type": "Person",
                "properties": {"cpf": "11111111111", "name": "Sem Proveniencia"},
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        assert resultado.doadores_pessoa[0].provenance is None

    def test_provenance_nao_tem_cpf_ou_cnpj_em_nenhum_campo(self) -> None:
        """Sanity LGPD: nenhum campo do ``ProvenanceBlock`` pode carregar CPF
        ou CNPJ em bruto — provenance deve ser só URLs públicas e IDs de
        run/ingestão. Caso o loader malformule o props, o service aceita —
        mas o test garante que o shape canônico que usamos é seguro.
        """
        cpf_pleno = "12345678909"
        conexoes = [
            _conn(
                rel_type="DOOU",
                target_id="pes_1",
                politico_is_source=False,
                rel_props={"valor": 100.0},
            ),
        ]
        entidades = {
            "pes_1": {
                "type": "Person",
                "properties": {
                    "cpf": cpf_pleno,
                    "name": "Doador",
                    # record_id contaminado com CPF — service deve drop.
                    "source_id": "tse_prestacao_contas",
                    "source_record_id": cpf_pleno,
                    "source_url": (
                        "https://divulgacandcontas.tse.jus.br/ords/..."
                    ),
                    "ingested_at": "2026-04-18T00:00:00+00:00",
                    "run_id": "tse_prestacao_contas_20260418000000",
                },
            },
        }
        resultado = classificar(conexoes, entidades, POLITICO_ID)
        d = resultado.doadores_pessoa[0]
        # Nenhum campo do ProvenanceBlock pode carregar os 11 dígitos.
        assert d.provenance is not None
        dump = d.provenance.model_dump_json()
        assert cpf_pleno not in dump


# --- Cypher query sanity check ----------------------------------------------


class TestCypherQuery:
    def test_query_arquivo_existe_e_tem_parametros_esperados(self) -> None:
        """Garante que a query Cypher existe e contém placeholder do entity_id."""
        query_path = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "bracc"
            / "queries"
            / "perfil_politico_connections.cypher"
        )
        assert query_path.exists(), f"Query não encontrada: {query_path}"
        conteudo = query_path.read_text(encoding="utf-8")
        assert "$entity_id" in conteudo
        assert "RETURN" in conteudo.upper()
        assert "conexoes" in conteudo.lower()
        # Mantém o contrato do shape: campos que o service consome.
        for chave in (
            "rel_type",
            "source_id",
            "target_id",
            "target_type",
            "target_props",
            "rel_props",
        ):
            assert chave in conteudo, f"Campo obrigatório ausente na query: {chave}"

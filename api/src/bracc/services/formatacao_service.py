"""Formatação de valores e mascaramento LGPD.

Portado do Flask (`backend/app.py::fmt_brl` linhas 244-251,
`backend/analise.py::_nome_mes` linhas 683-688) como parte da fase 04.A.

`mascarar_cpf` é obrigatório para LGPD. Toda exposição de CPF em response
pública deve passar por este helper — CPF pleno nunca pode vazar. O
middleware `bracc.middleware.cpf_masking` é a última linha de defesa;
este helper é a primeira.
"""

from __future__ import annotations

_MESES = {
    1: "Jan",
    2: "Fev",
    3: "Mar",
    4: "Abr",
    5: "Mai",
    6: "Jun",
    7: "Jul",
    8: "Ago",
    9: "Set",
    10: "Out",
    11: "Nov",
    12: "Dez",
}


def fmt_brl(valor: float | None) -> str:
    """Formata valor em BRL com sufixos (bi/mi/mil) ou decimais.

    Espelha `backend/app.py::fmt_brl` e `backend/analise.py::_fmt` (idênticos).
    Valor `None` é tratado como `0.00` — mantém compatibilidade com o Flask
    que chama `fmt_brl(0)` como fallback em várias situações.
    """
    if valor is None:
        valor = 0.0
    if valor >= 1_000_000_000:
        return f"R$ {valor / 1_000_000_000:.2f} bi"
    if valor >= 1_000_000:
        return f"R$ {valor / 1_000_000:.2f} mi"
    if valor >= 1_000:
        return f"R$ {valor / 1_000:.1f} mil"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def mascarar_cpf(cpf: str | None) -> str | None:
    """Mascara CPF para LGPD: apenas os 2 últimos dígitos visíveis.

    Entrada pode vir com/sem pontuação (111.222.333-44 ou 11122233344).
    Retorna `***.***.***-44` se CPF tiver exatamente 11 dígitos; caso
    contrário retorna `None` (evita vazamento de CPFs inválidos ou parciais).

    Nunca retorna o CPF pleno. Nunca usa regex que possa vazar dígitos.
    """
    if not cpf:
        return None
    # char-by-char para evitar regex que possa capturar além dos 11 dígitos
    limpo = "".join(c for c in cpf if c.isdigit())
    if len(limpo) != 11:
        return None
    return f"***.***.***-{limpo[-2:]}"


def nomear_mes(mes: int | None) -> str:
    """Abreviação de 3 letras do mês (Jan, Fev, ...). Fallback: str(mes)."""
    if mes is None:
        return ""
    return _MESES.get(mes, str(mes))

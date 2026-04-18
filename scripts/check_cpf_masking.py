#!/usr/bin/env python3
"""Valida LGPD CPF masking em models response do perfil.

Previne regressão: Pydantic model que expõe campo `cpf` (sem sufixo
`_mascarado`) em response pode vazar CPF pleno pro cliente. A convenção
do projeto é:

- Campos de CPF em models response: sempre `cpf_mascarado: str | None`.
- `cpf: str | None` é aceitável apenas em `PoliticoResumo` porque o
  valor já passa por `FormatacaoService.mascarar_cpf` no grafo (CPF de
  deputado é público por diário oficial mas exibido mascarado na UI).

Ver `docs/provenance.md`, memória
`feedback_parallel_agents_isolation.md`, e o refactor de
`middleware/cpf_masking.py` delegando pro service.

Exit 0 on pass, 1 on violation.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Models autorizados a ter campo `cpf` cru (já mascarado upstream).
AUTHORIZED: frozenset[str] = frozenset({"PoliticoResumo"})

# Regex heurístico: dentro de bloco `class X(BaseModel):`, linha tipo
# `cpf: ...` (não `cpf_mascarado`, não `cpf_partial`, etc.).
_CLASS_RE = re.compile(r"^class\s+(\w+)\s*\([^)]*BaseModel[^)]*\)\s*:", re.MULTILINE)
_CPF_FIELD_RE = re.compile(r"^\s+cpf\s*:\s*", re.MULTILINE)


def _models_in_file(path: Path) -> list[tuple[str, int]]:
    """Extract (class_name, line_of_class) pairs from a Pydantic models file."""
    text = path.read_text(encoding="utf-8")
    return [(m.group(1), text[: m.start()].count("\n") + 1) for m in _CLASS_RE.finditer(text)]


def _cpf_fields_in_class(source: str, class_name: str) -> list[int]:
    """Return line numbers where `cpf: ...` appears inside the given class body.

    Heurístico: usa `_CPF_FIELD_RE` e filtra pelo bloco textual entre
    a linha da classe-alvo e a próxima classe (ou fim do arquivo).
    """
    matches = list(_CLASS_RE.finditer(source))
    for i, m in enumerate(matches):
        if m.group(1) != class_name:
            continue
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(source)
        body = source[start:end]
        return [
            start + field.start() for field in _CPF_FIELD_RE.finditer(body)
        ]  # positions in full source
    return []


def _line_of(source: str, pos: int) -> int:
    return source[:pos].count("\n") + 1


def _scan_models_file(path: Path) -> list[str]:
    """Retorna violações (strings humanas) encontradas no arquivo."""
    source = path.read_text(encoding="utf-8")
    violations: list[str] = []
    for class_name, _class_line in _models_in_file(path):
        if class_name in AUTHORIZED:
            continue
        for pos in _cpf_fields_in_class(source, class_name):
            line = _line_of(source, pos)
            violations.append(
                f"  {path}:{line} — {class_name}.cpf deveria ser 'cpf_mascarado' "
                f"(ou adicione '{class_name}' em AUTHORIZED se já mascarado upstream)"
            )
    return violations


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    targets = list((root / "api" / "src" / "bracc" / "models").rglob("*.py"))
    all_violations: list[str] = []
    for path in sorted(targets):
        if path.name == "__init__.py":
            continue
        all_violations.extend(_scan_models_file(path))
    if all_violations:
        print("FAIL: LGPD CPF masking violations encontrados:", file=sys.stderr)
        for v in all_violations:
            print(v, file=sys.stderr)
        return 1
    print(
        "ok: models Pydantic seguem convencao LGPD CPF "
        f"(authorized={sorted(AUTHORIZED)}) — ver docs/provenance.md"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

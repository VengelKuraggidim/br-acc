"""Shared helpers for extracting CPF/CNPJ mentions from free text.

Multiple gazette/act pipelines (DOU, Camara inquiries, Senado CPIs,
Querido Diario) scan unstructured text for document mentions. This
module centralizes the regexes and dedup logic they all share.
"""

from __future__ import annotations

import re

from bracc_etl.transforms.document_formatting import (
    format_cnpj,
    format_cpf,
    strip_document,
)

_CPF_FORMATTED_RE = re.compile(r"\d{3}\.\d{3}\.\d{3}-\d{2}")
_CNPJ_FORMATTED_RE = re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}")
_CNPJ_RAW_RE = re.compile(r"\d{14}")
_CNPJ_COMBINED_RE = re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}|\d{14}")


def extract_cpfs(text: str) -> list[str]:
    """Return formatted CPFs found in text, deduped by digit-only key."""
    seen: set[str] = set()
    out: list[str] = []
    for match in _CPF_FORMATTED_RE.findall(text):
        digits = strip_document(match)
        if len(digits) == 11 and digits not in seen:
            seen.add(digits)
            out.append(format_cpf(match))
    return out


def extract_cnpjs(text: str) -> list[str]:
    """Return formatted CNPJs found in text (formatted + raw 14-digit), deduped."""
    seen: set[str] = set()
    out: list[str] = []

    for match in _CNPJ_FORMATTED_RE.findall(text):
        digits = strip_document(match)
        if len(digits) == 14 and digits not in seen:
            seen.add(digits)
            out.append(format_cnpj(match))

    for match in _CNPJ_RAW_RE.findall(text):
        if len(match) == 14 and match not in seen:
            seen.add(match)
            out.append(format_cnpj(match))

    return out


def extract_cnpjs_with_spans(text: str) -> list[tuple[str, str]]:
    """Return (formatted_cnpj, "start:end") tuples for each CNPJ mention, deduped."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in _CNPJ_COMBINED_RE.finditer(text):
        raw = match.group(0)
        digits = strip_document(raw)
        if len(digits) != 14 or digits in seen:
            continue
        seen.add(digits)
        out.append((format_cnpj(digits), f"{match.start()}:{match.end()}"))
    return out

import re


def strip_document(doc: str | None) -> str:
    if not doc:
        return ""
    return re.sub(r"[^0-9]", "", doc)


def format_cpf(cpf: str | None) -> str:
    digits = strip_document(cpf)
    if len(digits) != 11:
        return digits
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"


def format_cnpj(cnpj: str | None) -> str:
    digits = strip_document(cnpj)
    if len(digits) != 14:
        return digits
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"


def mask_cpf(cpf: str | None) -> str:
    """Mask a CPF for LGPD-compliant public display, showing only the last 4 digits."""
    digits = strip_document(cpf)
    if len(digits) != 11:
        return "***.***.***-**"
    return f"***.***.*{digits[7]}{digits[8]}-{digits[9]}{digits[10]}"


def _cpf_check_digits(digits: str) -> bool:
    if len(digits) != 11 or len(set(digits)) == 1:
        return False
    total = sum(int(digits[i]) * (10 - i) for i in range(9))
    d1 = 11 - (total % 11)
    d1 = 0 if d1 >= 10 else d1
    if int(digits[9]) != d1:
        return False
    total = sum(int(digits[i]) * (11 - i) for i in range(10))
    d2 = 11 - (total % 11)
    d2 = 0 if d2 >= 10 else d2
    return int(digits[10]) == d2


def validate_cpf(cpf: str | None) -> bool:
    digits = strip_document(cpf)
    return _cpf_check_digits(digits)


def _cnpj_check_digits(digits: str) -> bool:
    if len(digits) != 14 or len(set(digits)) == 1:
        return False
    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(int(digits[i]) * weights1[i] for i in range(12))
    d1 = 11 - (total % 11)
    d1 = 0 if d1 >= 10 else d1
    if int(digits[12]) != d1:
        return False
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(int(digits[i]) * weights2[i] for i in range(13))
    d2 = 11 - (total % 11)
    d2 = 0 if d2 >= 10 else d2
    return int(digits[13]) == d2


def validate_cnpj(cnpj: str | None) -> bool:
    digits = strip_document(cnpj)
    return _cnpj_check_digits(digits)


def classify_document(doc: str | None) -> str:
    """Classify a Brazilian document string for identity handling.

    Returns one of:
    - cpf_valid: 11-digit CPF-like document (masked not allowed)
    - cpf_partial: masked/partial CPF (LGPD style, 6 visible digits)
    - cnpj_valid: 14-digit CNPJ-like document
    - invalid: anything else
    """
    raw = (doc or "").strip()
    digits = strip_document(raw)
    has_mask = "*" in raw

    if has_mask and len(digits) == 6:
        return "cpf_partial"
    if len(digits) == 11:
        return "cpf_valid"
    if len(digits) == 14:
        return "cnpj_valid"
    return "invalid"

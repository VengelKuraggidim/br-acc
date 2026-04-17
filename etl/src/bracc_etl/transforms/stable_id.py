import hashlib


def stable_id(*parts: str, length: int = 24) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]

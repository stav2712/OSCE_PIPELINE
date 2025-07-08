import re
_SANITIZE = re.compile(r"[^A-Za-z0-9_]")

def clean_identifier(s: str) -> str:
    s = _SANITIZE.sub("_", s).lower()
    return re.sub("_+", "_", s).strip("_")
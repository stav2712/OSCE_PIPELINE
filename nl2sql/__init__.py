try:
    from importlib.metadata import version as _v
    __version__ = _v(__package__ or "nl2sql")
except Exception:
    __version__ = "0.1.0"
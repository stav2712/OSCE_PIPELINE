"""
Genera las VIEW s DuckDB a partir de los *.parquet* garantizando:

1.  Columnas legibles ⇒ se eliminan tokens irrelevantes y se usan snake_case.
2.  Desambiguación automática de nombres comunes como id, date, title…
3.  Colisiones resueltas con sufijos _1, _2, …
4.  Year y month siempre salen como INTEGER.
"""

from __future__ import annotations
import re, textwrap, duckdb, pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List
from .utils import clean_identifier

# ---------------------------------------------------------------------
_SKIP = {"compiledrelease", "releases", "records"}
_AMBIGUOUS = {
    "id", "date", "title", "description", "status",
    "amount", "currency", "name"
}
_PREFIX_MAP = {
    "awards":            "award",
    "awa_items":         "award_item",
    "awa_suppliers":     "supplier",
    "contracts":         "contract",
    "con_items":         "contract_item",
    "parties":           "party",
    "releases":          "release",
    "records":           "record",
    "ten_items":         "tender_item",
    "ten_tenderers":     "tenderer",
}
_SANITIZE = re.compile(r"[^A-Za-z0-9_]")

def _tokenize(path: str) -> List[str]:
    """Divide ‘compiledRelease/awards/0/value/amount’ → ['awards','value','amount']."""
    out = []
    for tok in path.split("/"):
        tok = tok.strip().lower()
        if not tok or tok.isdigit() or tok in _SKIP:
            continue
        out.append(tok)
    return out

def _colname(path: str, tbl_prefix: str) -> str:
    toks = _tokenize(path)
    if not toks:
        toks = ["col"]

    name = clean_identifier("_".join(toks))
    if name in _AMBIGUOUS and not name.startswith(f"{tbl_prefix}_"):
        name = f"{tbl_prefix}_{name}"

    return name or f"{tbl_prefix}_col"

# ---------------------------------------------------------------------
def build_views(con: duckdb.DuckDBPyConnection,
                parquet_dir: Path) -> Dict[str, List[str]]:
    """
    Recorre el directorio, crea/actualiza VIEW s y devuelve
    {tabla: [lista_columnas_limpias]} para el prompt.
    """
    schema: Dict[str, List[str]] = {}

    for pq_file in parquet_dir.glob("*.parquet"):
        table_raw = pq_file.stem.lower().replace("_latest", "")
        table     = clean_identifier(table_raw)
        if table.startswith("com_"):
            table = table[4:]
        tbl_prefix = _PREFIX_MAP.get(table, table.split("_", 1)[0])

        meta      = pq.read_metadata(pq_file)
        raw_cols  = meta.schema.names

        select_parts, clean_cols = [], []

        for raw in raw_cols:
            clean = _colname(raw, tbl_prefix)

            # si ya existe ⇒ añade sufijo incremental
            base, k = clean, 1
            while clean in clean_cols:
                clean = f"{base}_{k}"
                k += 1

            clean_cols.append(clean)

            # 🔄  Casteo automático de year / month a INTEGER
            if clean in {"year", "month"}:
                select_parts.append(f'CAST("{raw}" AS INTEGER) AS "{clean}"')
            else:
                select_parts.append(f'"{raw}" AS "{clean}"')

        view_sql = f"""
        CREATE OR REPLACE VIEW "{table}" AS
        SELECT {", ".join(select_parts)}
        FROM read_parquet('{pq_file.as_posix()}');
        """
        con.sql(textwrap.dedent(view_sql))
        schema[table] = clean_cols

    return schema

# ---------------------------------------------------------------------
def schema_markdown(schema: Dict[str, List[str]],
                    con: duckdb.DuckDBPyConnection,
                    max_tables: int = 30,
                    max_cols: int = 60) -> str:
    """
    Devuelve una versión compacta del esquema incluyendo un
    abreviado del tipo de dato (int, vch, dbl…).
    """
    out: List[str] = []
    for tbl, cols in sorted(schema.items())[:max_tables]:
        info = con.sql(f"PRAGMA table_info('{tbl}')").df()
        type_map = dict(zip(info["name"].str.lower(), info["type"].str.lower()))

        preview_cols = []
        for c in cols[:max_cols]:
            t = type_map.get(c.lower(), "")
            abbrev = t[:3] if t else ""
            preview_cols.append(f"{c}:{abbrev}" if abbrev else c)

        etc = "…" if len(cols) > max_cols else ""
        out.append(f"- **{tbl}**({', '.join(preview_cols)}{etc})")

    if len(schema) > max_tables:
        out.append(f"… ({len(schema) - max_tables} tablas más)")
    return "\n".join(out)
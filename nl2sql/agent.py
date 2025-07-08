import re, textwrap, duckdb, yaml, pandas as pd, hashlib, enum
from pathlib import Path
from typing import Dict, List

from .sql_schema   import build_views, schema_markdown
from .llm_backend  import get_backend
from .templates    import PROMPT_TEMPLATE, SUMMARY_TEMPLATE

_SQL_RE = re.compile(r"select\b.*?;", flags=re.I | re.S)

# Clasificación de errores
class ErrKind(enum.StrEnum):
    MISSING_NAME   = "missing_name"
    TYPE_MISMATCH  = "type_mismatch"
    DATE_FUNC      = "date_func"        # strftime / EXTRACT mal usado
    OTHER          = "other"

def _classify_error(msg: str) -> ErrKind:
    if "does not exist" in msg:
        return ErrKind.MISSING_NAME
    if "Binder Error" in msg and "VARCHAR" in msg and (
        "INTEGER" in msg or "BIGINT" in msg or "DECIMAL" in msg
    ):
        return ErrKind.TYPE_MISMATCH
    if "strftime" in msg and "Candidate functions" in msg:
        return ErrKind.DATE_FUNC
    return ErrKind.OTHER

# Extraer bloque SQL del LLM
def _extract_sql(text: str) -> str:
    candidates = []
    if "```" in text:
        blocks = re.findall(r"```[^\n]*\n(.*?)```", text, flags=re.S)
        candidates.extend(blocks)
    else:
        candidates.append(text)

    for chunk in candidates:
        m = _SQL_RE.search(chunk)
        if m:
            sql = m.group(0).strip()
            if not sql.endswith(";"):
                sql += ";"
            return sql
    return ""

# Agente NL→SQL
class NL2SQLAgent:
    def __init__(self, config_path: Path | str, *, verbose: bool = False):
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        self.cfg      = cfg
        self.con      = duckdb.connect()
        self.schema   = build_views(self.con, Path(cfg["parquet_dir"]))
        self.backend  = get_backend(cfg)
        self.verbose  = verbose

        self.schema_md   = schema_markdown(self.schema, self.con)
        self.schema_hash = hashlib.md5(self.schema_md.encode()).hexdigest()[:8]

    def _get_hint(self, error: str, kind: ErrKind) -> str:
        """
        Devuelve la porción de esquema o consejo más útil
        para el siguiente intento del LLM.
        """
        if kind is ErrKind.TYPE_MISMATCH:
            return (
                "Las columnas `year` y/o `month` son de tipo VARCHAR; "
                "castea con `CAST(year AS INTEGER)` si vas a restar o comparar.\n\n"
                f"(Esquema completo: {self.schema_hash})"
            )

        if kind is ErrKind.DATE_FUNC:
            return (
                "Está prohibido usar `strftime`, `extract`, `now()`, etc.  "
                "Para “últimos N años” usa:\n"
                "`WHERE year >= (SELECT MAX(year) FROM records) - (N-1)`\n\n"
                f"(Esquema completo: {self.schema_hash})"
            )

        if kind is ErrKind.MISSING_NAME:
            relevant_tables: set[str] = set()
            for name in re.findall(r'"(.*?)"', error):
                for tbl, cols in self.schema.items():
                    if name in cols or name == tbl:
                        relevant_tables.add(tbl)
            if relevant_tables:
                frag = []
                for tbl in relevant_tables:
                    cols = ", ".join(self.schema[tbl][:10])
                    frag.append(f"- **{tbl}**({cols}...)")
                return "\n".join(frag) + f"\n\n(Esquema completo: {self.schema_hash})"

        return f"⚠️ Usa esquema completo (hash: {self.schema_hash})"

    def query(self, question: str, max_retries: int | None = None):
        if max_retries is None:
            max_retries = 2 if "openai" in self.cfg.get("model_type", "") else 3

        error, sql, resp = None, "", ""
        for attempt in range(1, max_retries + 1):

            if attempt == 1:
                prompt = PROMPT_TEMPLATE.format(
                    schema   = self.schema_md,
                    question = question
                )
            else:
                kind   = _classify_error(error or "")
                prompt = (
                    f"### Contexto previo (hash: {self.schema_hash})\n"
                    f"### Error anterior\n{error}\n"
                    f"### Pista\n{self._get_hint(error, kind)}\n\n"
                    f"### Pregunta original\n{question}\n\n"
                    "Corrige SOLO la sentencia SQL:"
                )

            resp = self.backend.generate(prompt)
            sql  = _extract_sql(resp)

            if self.verbose:
                print(f"\n🟡 Intento {attempt}")
                print("Prompt enviado ↓↓↓\n", prompt[:1200], "…" if len(prompt) > 1200 else "")
                print("\nRespuesta LLM ↓↓↓\n", resp[:800], "…" if len(resp) > 800 else "")
                print("\nSQL extraído:\n", sql or "(vacío)")

            if not sql:
                error = "La LLM no devolvió SQL."
                continue

            try:
                df = self.con.sql(sql.replace("`", '"')).df()
                break          # ✔️ éxito
            except Exception as e:
                error = str(e) + f"\nSQL fallido:\n{sql}"
        else:
            raise RuntimeError(f"SQL inválido tras {max_retries} intentos.\n{error}")

        # ---------- Resumen para el usuario ----------
        if df.empty:
            resumen = "⚠️ La consulta devolvió 0 filas."
        else:
            head_md = df.head(15).to_markdown(index=False)
            resumen = self.backend.generate(SUMMARY_TEMPLATE.format(question=question, table_md=head_md))
        return df, resumen, sql
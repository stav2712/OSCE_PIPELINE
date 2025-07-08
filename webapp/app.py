from __future__ import annotations

import threading
import uuid
import tempfile
import pandas as pd

from pathlib import Path

from flask import Flask, jsonify, render_template, send_file, url_for, request
from flask_socketio import SocketIO, join_room

# ────────────────────────────────────
#  Agente NL2SQL  (carga diferida)
# ────────────────────────────────────
from nl2sql.agent import NL2SQLAgent

AGENT: NL2SQLAgent | None = None
_RELOADING = False

def _create_agent() -> NL2SQLAgent:
    """Construye y devuelve el agente NL2SQL (puede tardar)."""
    cfg = Path(__file__).resolve().parents[1] / "nl2sql" / "config.yaml"
    return NL2SQLAgent(cfg, verbose=False)

def _warm_up() -> None:
    """Carga el agente en segundo plano."""
    global AGENT
    try:
        print("⏳  [warm-up] Cargando vistas y modelo NL2SQL…")
        AGENT = _create_agent()
        print("✅  [warm-up] Motor NL2SQL listo")
    except Exception as exc:
        import traceback, sys
        print(f"❌  [warm-up] Error al precargar: {exc!r}", file=sys.stderr)
        traceback.print_exc()

def get_agent() -> NL2SQLAgent:
    if AGENT is None:
        raise RuntimeError("El motor NL2SQL todavía no está listo")
    return AGENT

def reset_agent() -> None:
    """Descarta el agente y vuelve a cargarlo en un hilo."""
    global AGENT, _RELOADING

    AGENT = None
    if _RELOADING:        # ya hay un hilo creando el agente
        return

    _RELOADING = True
    def _load():
        global AGENT, _RELOADING
        try:
            print("⏳  [reload] reconstruyendo vistas NL2SQL…")
            AGENT = _create_agent()
            print("✅  [reload] nuevo modelo listo")
        finally:
            _RELOADING = False

    threading.Thread(target=_load, daemon=True).start()

# ────────────────────────────────────
#  Flask / SocketIO
# ────────────────────────────────────
app = Flask(__name__)
socketio = SocketIO(app, async_mode="threading")          # instancia única

# Lanza el warm-up nada más definir la app
threading.Thread(target=_warm_up, daemon=True).start()

# -------- SocketIO events ----------
@socketio.on("join")
def handle_join(job_id: str):
    """El cliente se une a la sala = job_id para recibir progreso."""
    join_room(job_id)


# -------- Rutas HTML ----------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/etl")
def etl_page():
    return render_template("etl.html")


# -------- Rutas API ----------
@app.route("/start_etl", methods=["POST"])
def start_etl():
    data   = request.get_json(silent=True) or {}
    window = data.get("window_days", 120)

    job_id = str(uuid.uuid4())
    from .tasks import run_etl
    threading.Thread(
        target=run_etl,
        args=(socketio, job_id, window),
        daemon=True
    ).start()
    return jsonify({"job_id": job_id})

@app.route("/ask", methods=["POST"])
def ask():
    # 1. Recuperar la pregunta
    question = request.json["question"].strip()

    # 2. Ejecutar el agente NL2SQL
    agent = get_agent()
    df, resumen, sql = agent.query(question)

    # 3. Renderizar la tabla HTML (sin <style>)
    table_html = df.to_html(classes="tbl", index=False, border=0)

    # 4. Generar archivo Excel temporal
    file_id   = uuid.uuid4().hex
    xlsx_path = Path(tempfile.gettempdir()) / f"{file_id}.xlsx"

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Datos")
        worksheet = writer.sheets["Datos"]

        # Autofit: longitud máxima de cada columna (datos y cabecera)
        for idx, col in enumerate(df.columns):
            # Convertimos a str y medimos cada celda
            col_str = df[col].astype(str)
            if not col_str.empty:
                max_data_len = col_str.str.len().max()
            else:
                max_data_len = 0
            # Longitud del encabezado
            header_len = len(str(col))
            # Ancho final + un pequeño margen
            width = max(int(max_data_len), header_len) + 2

            worksheet.set_column(idx, idx, width)

    excel_url = url_for("download_excel", file_id=file_id)

    # 5. Responder al front-end
    return jsonify({
        "sql": sql,
        "resumen": resumen,
        "table": table_html,
        "excel": excel_url
    })

@app.route("/ready")
def ready():
    """Devuelve 200 cuando el agente está listo; 503 en caso contrario."""
    return ("starting", 503) if AGENT is None else ("ok", 200)

@app.route("/download/<file_id>")
def download_excel(file_id: str):
    path = Path(tempfile.gettempdir()) / f"{file_id}.xlsx"
    if not path.exists():
        return "Archivo no encontrado", 404
    return send_file(path, as_attachment=True, download_name="resultado.xlsx")

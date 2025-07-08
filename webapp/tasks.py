# tasks.py

from __future__ import annotations

from .app import reset_agent

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flask_socketio import SocketIO

from ..flow import run_flow

import logging


class SocketIOHandler(logging.Handler):
    """Cada registro .info() se envía al cliente como evento 'detail'."""
    def __init__(self, sio, room):
        super().__init__(level=logging.INFO)
        self.sio = sio
        self.room = room

    def emit(self, record):
        try:
            msg = self.format(record)
            self.sio.emit("detail", {"line": msg}, room=self.room)
        except Exception:
            # evita que un fallo de red tumbe el ETL
            pass


# Configuración base de logging (stdout)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run_etl(socketio, job_id: str, window_days: int | None = None):
    logger = logging.getLogger()  # root logger
    handler = SocketIOHandler(socketio, job_id)
    handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
    logger.addHandler(handler)

    try:
        logging.info("ETL %s — INICIO", job_id)
        socketio.emit("progress", {"msg": "Ejecutando ETL…", "pct": 0}, room=job_id)

        # Lanza todo el flujo y crea flow_YYYY-MM-DD.log atrás
        run_flow(
            window_days,
            progress=lambda pct, msg: socketio.emit(
                "progress", {"msg": msg, "pct": pct}, room=job_id
            )
        )

        # Una vez consolidado, recarga el agente con datos frescos
        socketio.emit("progress", {"msg": "Recargando modelo…", "pct": 90}, room=job_id)
        reset_agent()

        socketio.emit("progress", {"msg": "✔️ ETL terminado", "pct": 100}, room=job_id)
        logging.info("ETL %s — FIN OK", job_id)

    except Exception as exc:
        socketio.emit("progress", {"msg": f"⚠️ Error: {exc}", "pct": -1}, room=job_id)
        logging.exception("Fallo inesperado en ETL %s", job_id)

    finally:
        logger.removeHandler(handler)

from __future__ import annotations
import argparse
import io
import locale
import os
import sys
import traceback
from pathlib import Path
from .agent import NL2SQLAgent

# 1)  Salida UTF-8 en cualquier terminal
def _force_utf8_windows() -> None:
    """Fuerza CodePage 65001 y reconfigura stdout/stderr en Windows."""
    try:
        import ctypes
        import msvcrt

        # Cambia Input CP y Output CP de la consola a UTF-8 (65001)
        ctypes.windll.kernel32.SetConsoleCP(65001)
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)

        # Asegura modo binario para evitar conversión implícita
        for stream in (sys.stdout, sys.stderr):
            msvcrt.setmode(stream.fileno(), os.O_BINARY)
    except Exception:
        # Si algo falla, continuamos: mejor algo que nada
        pass


def configure_encoding() -> None:
    """
    — Linux/macOS: no suele requerir nada extra.
    — Windows: activa UTF-8, y en *cualquier* SO reconfigura los streams
      a 'utf-8' si aún no lo están.
    """
    if sys.platform == "win32":
        # Forzar configuración regional a UTF-8 para funciones dependientes de locale
        try:
            locale.setlocale(locale.LC_ALL, "es_ES.UTF-8")
        except locale.Error:
            locale.setlocale(locale.LC_ALL, "")  # la que haya instalada
        _force_utf8_windows()

    # Reenvuelve stdout/stderr si siguen sin UTF-8
    for name in ("stdout", "stderr"):
        stream: io.TextIOWrapper = getattr(sys, name)
        enc = (stream.encoding or "").lower()
        if enc != "utf-8":
            wrapper = io.TextIOWrapper(
                stream.buffer,
                encoding="utf-8",
                errors="replace",
                line_buffering=True,
            )
            setattr(sys, name, wrapper)

# 2)  Utilidad de impresión segura
def safe_print(text: str) -> None:
    """Imprime siempre, sustituyendo caracteres problemáticos si hiciera falta."""
    try:
        print(text)
    except UnicodeEncodeError:
        backup = text.encode("utf-8", "backslashreplace").decode("utf-8")
        print(backup)

# 3)  CLI principal
def main() -> None:
    configure_encoding()

    ap = argparse.ArgumentParser(
        description="Chat NL→SQL sobre licitaciones públicas (DuckDB + LLM)"
    )
    ap.add_argument(
        "-c",
        "--config",
        default="nl2sql/config.yaml",
        help="Ruta al archivo config.yaml",
    )
    ap.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Muestra trazas completas y SQL generado en cada paso",
    )
    args = ap.parse_args()

    # Selecciona emojis solo si la salida los soporta
    UTF = (sys.stdout.encoding or "").lower().startswith("utf")
    ICON_INIT = "🔄" if UTF else "[*]"
    ICON_READY = "🟢" if UTF else "[OK]"
    ICON_PROMPT = "❓" if UTF else "?"

    safe_print(f"{ICON_INIT} Inicializando agente…")
    agent = NL2SQLAgent(Path(args.config), verbose=args.debug)
    safe_print(f"{ICON_READY} Listo. Escribe tu pregunta en español (‘salir’ para terminar).")

    # Conversación interactiva
    while True:
        try:
            q = input(f"{ICON_PROMPT} ")
        except (KeyboardInterrupt, EOFError):
            break
        except Exception:
            safe_print("⚠️  Error leyendo la entrada:")
            safe_print(traceback.format_exc())
            continue

        if q.lower().strip() in {"salir", "exit", "quit"}:
            break

        try:
            # Normaliza posibles rarezas de teclado
            q_clean = q.encode("utf-8", "ignore").decode("utf-8")

            # Consulta al agente
            df, resumen, sql = agent.query(q_clean)

            safe_print("\nSQL final ejecutado:")
            safe_print(sql)

            safe_print("\nResultado (primeras filas):")
            safe_print(df.head(15).to_markdown(index=False))

            safe_print("\nResumen:")
            safe_print(resumen)

        except Exception as e:
            safe_print(f"⚠️  Error en procesamiento: {e}")

            # Con -d/--debug mostramos todo el traceback
            if args.debug:
                safe_print("\n🔎 Traceback completo ↓↓↓")
                safe_print(traceback.format_exc())

if __name__ == "__main__":
    main()
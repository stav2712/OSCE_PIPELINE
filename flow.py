import argparse
import datetime
import logging
import yaml
from pathlib import Path
from typing import Callable, Optional

from downloader import run_download
from normalizer import run_normalization
from consolidator import run_consolidation


# ─────────────────────────────────────────────────────────────
def run_flow(
    window_days: int | None = None,
    progress: Optional[Callable[[int, str], None]] = None
) -> None:
    """
    Ejecuta todo el pipeline.  
    Si se pasa `progress(pct:int, msg:str)` se irá llamando para
    actualizar la barra en la web.
    """
    if progress is None:
        progress = lambda *_: None

    # 1) Carga config.yaml
    cfg_path = Path(__file__).parent / "config.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    window = window_days if window_days is not None else cfg["window_days"]

    # 2) Prepara logging (igual que en __main__)
    log_dir = Path(cfg["root_dir"]) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"flow_{datetime.date.today()}.log"

    logger = logging.getLogger("flow")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(sh)

    # ── enlazar handlers al root ──────────────────────────────
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    root.addHandler(sh)
    logger.propagate = False
    # ──────────────────────────────────────────────────────────

    # 3) Pipeline con checkpoints de progreso
    progress(0,  "Descargando archivos…")
    logger.info("ETL OSCE — INICIO")

    new_ids = run_download(window)
    logger.info("TOTAL nuevos/cambiados: %d", len(new_ids))
    progress(40, f"Descarga lista ({len(new_ids)} nuevos). Normalizando…")

    run_normalization(new_ids)
    progress(70, "Normalización completa. Consolidando…")

    run_consolidation()
    progress(90, "Consolidación terminada.")
    logger.info("ETL OSCE — TERMINADO")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--window-days",
        type=int,
        default=None,
        help="Override del window_days de config.yaml"
    )
    args = parser.parse_args()
    run_flow(args.window_days)

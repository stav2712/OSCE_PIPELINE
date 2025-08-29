import pandas as pd, logging
from pathlib import Path
import yaml, datetime   # solo para logging con fecha

def _unite(dir_with_parquets: Path):
    pieces = []
    for pq in dir_with_parquets.rglob("*.parquet"):
        try:
            pieces.append(pd.read_parquet(pq))
        except Exception as e:
            logging.warning(f"Falló {pq}: {e}")
    if not pieces:
        return None
    return pd.concat(pieces, ignore_index=True)

def run_consolidation():
    from pathlib import Path
    CFG = Path(__file__).resolve().parent / "config.yaml"
    with open(CFG, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    proc_dir  = Path(cfg["root_dir"]) / "processed" / "signatures"
    if not proc_dir.is_dir():
        logging.info(f"No existen datos en {proc_dir}; consolidación omitida.")
        return

    final_dir = Path(cfg["root_dir"]) / "final"
    final_dir.mkdir(exist_ok=True)

    for file_name_dir in proc_dir.iterdir():
        if not file_name_dir.is_dir():
            continue

        dfs = []
        for sig_dir in file_name_dir.iterdir():
            df = _unite(sig_dir)
            if df is not None:
                dfs.append(df)
        if not dfs:
            continue

        # Alinear columnas
        all_cols = set()
        for d in dfs:
            all_cols.update(d.columns)
        dfs = [d.reindex(columns=sorted(all_cols)) for d in dfs]

        combined = pd.concat(dfs, ignore_index=True)

        # “Seguro” extra: elimina filas 100 % idénticas
        antes = len(combined)
        combined = combined.drop_duplicates()
        logging.info(f"{file_name_dir.name}: eliminadas {antes - len(combined):,} filas duplicadas estrictas")

        # Convertir object→string para no perder NAs
        for col, dt in combined.dtypes.items():
            if dt == "object":
                combined[col] = combined[col].astype("string")

        out = final_dir / f"{file_name_dir.name}_latest.parquet"
        combined.to_parquet(out, index=False)
        logging.info(f"Consolidado -> {out}  ({len(combined):,} filas)")

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    run_consolidation()
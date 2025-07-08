import pandas as pd, hashlib, yaml, logging
from pathlib import Path

EXPECTED = [
  "com_awa_ite_additionalClassific.csv","com_awa_ite_tot_exchangeRates.csv","com_awa_items.csv",
  "com_awa_suppliers.csv","com_awa_val_exchangeRates.csv","com_awards.csv","com_con_documents.csv",
  "com_con_ite_additionalClassific.csv","com_con_ite_tot_exchangeRates.csv","com_con_items.csv",
  "com_con_val_exchangeRates.csv","com_contracts.csv","com_par_additionalIdentifiers.csv",
  "com_parties.csv","com_sources.csv","com_ten_documents.csv","com_ten_ite_additionalClassific.csv",
  "com_ten_ite_tot_exchangeRates.csv","com_ten_items.csv","com_ten_tenderers.csv","records.csv","releases.csv"
]

def signature(cols:list) -> str:
    return hashlib.md5("|".join(cols).encode()).hexdigest()

def run_normalization(file_ids=None):
    from pathlib import Path
    CFG = Path(__file__).resolve().parent / "config.yaml"   # mismo directorio que el .py
    with open(CFG, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    root = Path(cfg["root_dir"])
    proc = root / "processed" / "signatures"
    proc.mkdir(parents=True, exist_ok=True)

    if file_ids is None:
        file_ids = [p.name for p in (root / "extracted_csv").iterdir() if p.is_dir()]

    if not file_ids:
        logging.info("No hay IDs para normalizar.")
        return

    for file_id in file_ids:
        folder = root / "extracted_csv" / file_id
        if not folder.exists():
            logging.warning(f"Sin carpeta {folder}, se omite.")
            continue

        parts = file_id.split("-")                 # ej. seace_v3-2025-03
        if len(parts) != 3:
            logging.warning(f"Formato de id desconocido: {file_id}")
            continue
        version, year, month = parts

        for csv_name in EXPECTED:
            path = folder / csv_name
            if not path.exists():
                continue

            try:
                df = pd.read_csv(path, low_memory=False)
            except Exception as e:
                logging.error(f"Error leyendo {path}: {e}")
                continue

            df["version"], df["year"], df["month"] = version, year, month
            base_cols = sorted(c for c in df.columns if c not in ("version", "year", "month"))
            sig = signature(base_cols)

            out_dir  = proc / csv_name.replace(".csv", "") / f"signature_{sig}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{file_id}.parquet"

            try:
                out_file.unlink(missing_ok=True)
                df.to_parquet(out_file, index=False)
                logging.info(f"OK  {out_file}")
            except Exception as e:
                logging.error(f"No se grabó {out_file}: {e}")

if __name__ == "__main__":
    import json, sys, logging
    logging.basicConfig(level=logging.INFO)
    ids = json.loads(sys.stdin.read() or "[]")
    run_normalization(ids if ids else None)
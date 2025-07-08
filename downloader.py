import os, sqlite3, requests, hashlib, zipfile, argparse, datetime, logging, concurrent.futures, time, yaml, threading, shutil
from pathlib import Path

def md5sum(path, chunk=8192):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for piece in iter(lambda: f.read(chunk), b""):
            h.update(piece)
    return h.hexdigest()

class Manifest:
    def __init__(self, db_path):
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.lock = threading.Lock()
        self.conn.execute("""CREATE TABLE IF NOT EXISTS files(
            file_id TEXT PRIMARY KEY,
            source  TEXT,
            zip_md5 TEXT,
            updated_at_api TEXT,
            last_download TEXT
        )""")
        self.conn.commit()

    def get(self, file_id):
        with self.lock:
            c = self.conn.execute(
                "SELECT zip_md5, updated_at_api FROM files WHERE file_id=?", (file_id,)
            )
            return c.fetchone()

    def upsert(self, file_id, source, md5, updated_at):
        now = datetime.datetime.utcnow().isoformat(timespec="seconds")
        with self.lock:
            self.conn.execute(
                """INSERT INTO files(file_id, source, zip_md5, updated_at_api, last_download)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(file_id) DO UPDATE SET
                     zip_md5        = excluded.zip_md5,
                     updated_at_api = excluded.updated_at_api,
                     last_download  = excluded.last_download""",
                (file_id, source, md5, updated_at, now)
            )
            self.conn.commit()

def download_zip(url, out_path, retries=3):
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            if zipfile.is_zipfile(out_path):
                return True
            raise zipfile.BadZipFile
        except Exception as e:
            logging.warning(f"Intento {attempt}/{retries} falló al descargar {url}: {e}")
            time.sleep(5)
    logging.error(f"No se pudo descargar {url}")
    return False

def crawl_source(source, cfg, manifest, window_days):
    logging.info(f"{source}: iniciando crawl_source")
    endpoint   = cfg["api_endpoint"]
    page       = 1
    new_ids    = []
    threshold  = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=window_days)

    raw_dir     = Path(cfg["root_dir"]) / "raw_zips"      / source
    extract_dir = Path(cfg["root_dir"]) / "extracted_csv"      # común a todos
    raw_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(exist_ok=True)

    while True:
        logging.info(f"{source}: solicitando página {page}")
        params = {"page": page, "source": source}
        data   = requests.get(endpoint, params=params, timeout=60).json()

        for item in data.get("results", []):
            file_id  = item["id"]
            zip_url  = item["files"].get("csv")
            updated_at = item.get("timestamp") or item.get("updated_at") or item.get("created_at")
            if not updated_at:
                logging.warning(f"Sin timestamp en item {item.get('id')}, se omite.")
                continue

            # normalizamos la fecha
            try:
                ts = updated_at.rstrip("Z")
                if updated_at.endswith("Z"):
                    ts = updated_at[:-1] + "+00:00"
                updated_dt = datetime.datetime.fromisoformat(ts)
            except Exception as e:
                logging.warning(f"Timestamp inválido '{updated_at}' en {item.get('id')}: {e}")
                continue

            if updated_dt < threshold:
                continue

            db_row = manifest.get(file_id)
            needs_download = False
            if db_row is None:
                needs_download = True
            else:
                _, db_updated = db_row
                if db_updated is None or db_updated < updated_at:
                    needs_download = True

            if not needs_download:
                continue

            zip_path = raw_dir / f"{file_id}.zip"
            if not download_zip(zip_url, zip_path):
                continue

            md5_local = md5sum(zip_path)
            target_folder = extract_dir / file_id

            # Si ya existía el folder, lo eliminamos por completo para evitar residuos
            if target_folder.exists():
                shutil.rmtree(target_folder)
            target_folder.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(zip_path) as z:
                z.extractall(target_folder)
                logging.info(f"{source}: extraído {file_id}")
            zip_path.unlink()

            manifest.upsert(file_id, source, md5_local, updated_at)
            new_ids.append(file_id)

        if not data.get("pagination", {}).get("has_next"):
            break
        page = data["pagination"]["next_page_number"]

    return new_ids

def run_download(window_days):
    from pathlib import Path
    CONFIG = Path(__file__).resolve().with_suffix(".yaml")     # downloader.yaml → config.yaml?
    # si el YAML está al lado de downloader.py:
    CONFIG = Path(__file__).resolve().parent / "config.yaml"

    with open(CONFIG, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    log_file = Path(cfg["root_dir"]) / "logs" / f'download_{datetime.date.today()}.log'
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )

    manifest     = Manifest(Path(cfg["root_dir"]) / "state" / "manifest.sqlite")
    all_new_ids  = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg["max_workers"]) as pool:
        futures = {pool.submit(crawl_source, s, cfg, manifest, window_days): s for s in cfg["sources"]}
        for fut in concurrent.futures.as_completed(futures):
            new_ids = fut.result()
            all_new_ids.extend(new_ids)
            logging.info(f"{futures[fut]} -> {len(new_ids)} archivos nuevos/cambiados")

    logging.info(f"TOTAL nuevos/cambiados: {len(all_new_ids)}")
    return all_new_ids

if __name__ == "__main__":
    argp = argparse.ArgumentParser()
    argp.add_argument("--window-days", type=int, default=120)
    args = argp.parse_args()
    run_download(args.window_days)
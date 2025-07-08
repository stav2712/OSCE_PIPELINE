#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd

def listar_columnas_parquet(ruta: Path, recursive: bool = False):
    pattern = "**/*.parquet" if recursive else "*.parquet"
    archivos = list(ruta.glob(pattern))

    if not archivos:
        print(f"No se encontraron archivos .parquet en {ruta.resolve()}")
        return

    for i, archivo in enumerate(archivos, start=1):
        try:
            df = pd.read_parquet(archivo, engine='auto')
            columnas = df.columns.tolist()
            columnas_str = ", ".join(columnas)
            print(f"{i}. Archivo: {archivo.name}\n   Columnas: {columnas_str}\n")
        except Exception as e:
            print(f"{i}. Archivo: {archivo.name}\n   Error al leer el archivo: {e}\n")

def main():
    parser = argparse.ArgumentParser(
        description="Lista las etiquetas de columnas de todos los archivos .parquet en una carpeta."
    )
    parser.add_argument(
        "carpeta",
        type=Path,
        help="Ruta a la carpeta que contiene archivos .parquet"
    )
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Buscar también en subdirectorios"
    )

    args = parser.parse_args()
    if not args.carpeta.is_dir():
        parser.error(f"La ruta {args.carpeta} no es una carpeta válida.")
    listar_columnas_parquet(args.carpeta, args.recursive)

if __name__ == "__main__":
    main()
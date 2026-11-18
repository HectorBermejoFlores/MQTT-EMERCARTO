import csv
from datetime import datetime
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo


ZONA_LOCAL = ZoneInfo("Europe/Madrid")
INICIO_EJECUCION = datetime.now(ZONA_LOCAL)
ARCHIVO_CSV: Path | None = None

_CABECERAS = (
    "from",
    "sender",
    "latitud",
    "longitud",
    "id",
    "ruta",
    "SNR",
    "RSSI",
    "hora_creación",
    "hora_llegada",
)


def crear_archivo_csv():
    global ARCHIVO_CSV

    if ARCHIVO_CSV is None:
        carpeta_csv = Path("csv")
        carpeta_csv.mkdir(parents=True, exist_ok=True)
        nombre_csv = (
            f"{INICIO_EJECUCION.strftime('%d-%m-%Y_%H-%M-%S-%f')}.csv"
        )
        ARCHIVO_CSV = carpeta_csv / nombre_csv

        with ARCHIVO_CSV.open("w", newline="", encoding="utf-8") as archivo:
            csv.writer(archivo).writerow(_CABECERAS)

    return ARCHIVO_CSV


def guardar_fila_csv(fila: Sequence[object]):
    archivo_csv = crear_archivo_csv()
    with archivo_csv.open("a", newline="", encoding="utf-8") as archivo:
        csv.writer(archivo).writerow(fila)

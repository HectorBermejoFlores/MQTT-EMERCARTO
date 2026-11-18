import time

from modules.config import MARGEN_FUTURO_SEGUNDOS, VENTANA_SEGUNDOS


PAQUETES_PROCESADOS = {}


def limpiar_procesados():
    ahora = time.time()
    paquetes_antiguos = [
        clave
        for clave, instante in PAQUETES_PROCESADOS.items()
        if ahora - instante > VENTANA_SEGUNDOS
    ]

    for clave in paquetes_antiguos:
        del PAQUETES_PROCESADOS[clave]


def esta_procesado(mensaje):
    limpiar_procesados()
    clave = (mensaje["from"], mensaje["id"])
    return clave in PAQUETES_PROCESADOS


def marcar_procesado(mensaje):
    clave = (mensaje["from"], mensaje["id"])
    PAQUETES_PROCESADOS[clave] = time.time()


def posicion_valida(mensaje):
    timestamp = mensaje["payload"]["receivedTime"]
    edad = time.time() - timestamp
    return -MARGEN_FUTURO_SEGUNDOS < edad <= VENTANA_SEGUNDOS


def respuesta_correcta(respuesta):
    return isinstance(respuesta, str) and respuesta.startswith("ok#")

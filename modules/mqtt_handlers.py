import json
from datetime import datetime

import modules.csv_file as csv_file
from modules.logger import logger
from modules.nodos import NODOS
from modules.paquetes import (
    posicion_valida,
    esta_procesado,
    marcar_procesado,
    respuesta_correcta,
)
from modules.trama import crear_trama
from modules.config import MQTT_TOPIC

# ==========================================
# MQTT
# ==========================================

def on_connect(client, userdata, flags, reason_code, properties=None):
    """
    Callback ejecutado cuando finaliza un intento de conexión MQTT.

    Si la conexión se establece correctamente, el cliente se suscribe
    al topic configurado para comenzar a recibir posiciones.

    Si el broker devuelve un código de error, se registra como ERROR.

    :param client: Cliente MQTT que ha realizado el intento de conexión.
    :param userdata: Datos adicionales asociados al cliente.
    :param flags: Información proporcionada por la conexión MQTT.
    :param reason_code: Código de resultado de la conexión.
    :param properties: Propiedades MQTT asociadas a la conexión.
    """

    if reason_code == 0:
        logger.info(
            "Conectado correctamente al servidor MQTT"
        )

        if csv_file.ARCHIVO_CSV is None:
            archivo_csv = csv_file.crear_archivo_csv()
            logger.info(
                "Archivo CSV creado: %s",
                archivo_csv
            )

        # Suscribe el cliente al topic donde se reciben las posiciones.
        client.subscribe(
            MQTT_TOPIC
        )

        logger.info(
            "Suscrito a: %s",
            MQTT_TOPIC
        )

    else:
        logger.error(
            "Error al conectar con el servidor MQTT. Código: %s",
            reason_code
        )

# ==========================================
# MQTT CALLBACK
# ==========================================

def on_message(client, userdata, msg):
    """
    Procesa cada mensaje recibido desde el broker MQTT.

    Flujo de ejecución:

    1) Decodifica el mensaje MQTT y convierte el JSON recibido.
    2) Comprueba que el mensaje corresponda a una posición.
    3) Identifica el nodo que genera la posición y el nodo que la reenvía.
    4) Comprueba que la posición siga siendo válida.
    5) Comprueba que el paquete no haya sido procesado anteriormente.
    6) Genera la trama y la envía al servicio SOAP.
    7) Si SOAP confirma el envío, registra el paquete como procesado.
    """

    try:
        # Intenta decodificar el payload MQTT y convertirlo desde JSON.
        # Si el contenido no puede interpretarse correctamente, se descarta.
        try:
            mensaje = json.loads(
                msg.payload.decode("utf-8")
            )

        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.warning(
                "Mensaje MQTT descartado: el payload no contiene un JSON válido"
            )
            return

        # Solo se procesan mensajes correspondientes a posiciones.
        if mensaje.get("type") != "position":
            logger.warning(
                "Mensaje MQTT ignorado: type=%r",
                mensaje.get("type")
            )
            return

        # Busca el identificador correspondiente al nodo que ha generado
        # la posición.
        nodo = NODOS.get(
            mensaje["from"]
        )

        # El campo sender llega representado en hexadecimal.
        # Se elimina el carácter inicial '!' y se convierte a decimal
        # para poder buscarlo posteriormente en NODOS.
        sender_decimal = int(
            mensaje["sender"].lstrip("!"),
            16
        )

        # Si el nodo origen no está registrado, la posición se descarta.
        if nodo is None:
            logger.warning(
                "Nodo desconocido: from=%s",
                mensaje["from"]
            )
            return

        try:
            trama = crear_trama(
                mensaje,
                nodo
            )
        except ValueError as error:
            logger.error(
                "Paquete descartado: no se puede generar la trama: %s",
                error
            )
            return

        # Descarta posiciones que se encuentren fuera de la ventana
        # temporal permitida.
        if not posicion_valida(mensaje):
            logger.warning(
                "Posición descartada por antigüedad: from=%s id=%s",
                NODOS.get(mensaje["from"]),
                mensaje["id"]
            )
            return

        # Evita volver a enviar una posición que ya haya sido
        # procesada correctamente.
        if esta_procesado(mensaje):
            logger.info(
                "Posición duplicada ignorada: "
                "from=%s id=%s desde nodo=%s",
                NODOS.get(mensaje["from"]),
                mensaje["id"],
                NODOS.get(sender_decimal)
            )
            return

        logger.info("=" * 60)
        logger.info(
            "Posición recibida desde nodo=%s from=%s id=%s",
            NODOS.get(sender_decimal),
            NODOS.get(mensaje["from"]),
            mensaje["id"]
        )

        payload = mensaje["payload"]
        received = payload.get("receivedTime")
        sent = payload.get("sentTime")

        received_hms = (
            datetime.fromtimestamp(
                received,
                csv_file.ZONA_LOCAL
            ).strftime("%H:%M:%S")
            if received is not None else "no disponible"
        )
        sent_hms = (
            datetime.fromtimestamp(
                sent,
                csv_file.ZONA_LOCAL
            ).strftime("%H:%M:%S")
            if sent is not None else "no disponible"
        )

        logger.info(
            "Tiempo de recepción: %s | tiempo de envío: %s",
            received_hms,
            sent_hms
        )

        ruta = mensaje.get("route")
        ruta_formateada = ""
        if ruta is None or ruta == []:
            logger.info(
                "Ruta del paquete: (no disponible)"
            )
        elif (
            not isinstance(ruta, list)
            or len(ruta) > 8
            or any(type(hop) is not int for hop in ruta)
        ):
            logger.warning(
                "Ruta del paquete no válida: %r",
                ruta
            )
        else:
            ruta_formateada = " --> ".join(
                NODOS.get(hop, str(hop))
                for hop in ruta
            )
            logger.info(
                "Ruta del paquete: %s",
                ruta_formateada
            )

        csv_file.guardar_fila_csv([
            nodo,
            NODOS.get(sender_decimal, str(sender_decimal)),
            mensaje["payload"]["latitude_i"] / 10_000_000,
            mensaje["payload"]["longitude_i"] / 10_000_000,
            mensaje["id"],
            ruta_formateada if ruta else "",
            payload.get("SNR"),
            payload.get("RSSI"),
            sent_hms,
            received_hms,
        ])

        logger.info(
            "SNR: %s, RSSI: %s",
            mensaje["payload"].get("SNR"),
            mensaje["payload"].get("RSSI")
        )

        logger.info(
            "Trama generada: %s",
            trama
        )

        # Envía la trama al servicio SOAP mediante CargarPosiciones().
        respuesta = userdata.service.CargarPosiciones(
            trama,
            "MESHTASTIC"
        )

        logger.info(
            "Respuesta SOAP: %r",
            respuesta
        )

        # El paquete solo se marca como procesado cuando la respuesta
        # del servicio SOAP indica que la operación ha sido correcta.
        if respuesta_correcta(respuesta):
            marcar_procesado(mensaje)

            logger.info(
                "Posición procesada correctamente: from=%s id=%s",
                NODOS.get(mensaje["from"], str(mensaje["from"])),
                mensaje["id"]
            )
            logger.info("=" * 60)

        else:
            logger.warning(
                "Respuesta SOAP no válida. "
                "La posición no se marca como procesada: "
                "from=%s id=%s respuesta=%r",
                NODOS.get(mensaje["from"], str(mensaje["from"])),
                mensaje["id"],
                respuesta
            )
            logger.info("=" * 60)

    # Captura errores no controlados durante el procesamiento del mensaje
    # para evitar que el consumidor MQTT deje de funcionar.
    except Exception:
        # logger.exception registra el error y también el traceback.
        logger.exception(
            "Error procesando mensaje MQTT"
        )

    # Código auxiliar que puede habilitarse para mostrar directamente
    # los mensajes recibidos durante tareas de depuración.
    #
    # logger.info("--- MENSAJE RECIBIDO ---")
    # logger.info("TOPIC: %s", msg.topic)
    #
    # try:
    #     logger.info("PAYLOAD: %s", msg.payload.decode())
    # except UnicodeDecodeError:
    #     logger.info("PAYLOAD: %r", msg.payload)
    #
    # logger.info("------------------------")

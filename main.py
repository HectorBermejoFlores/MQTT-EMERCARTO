import json
import time
import logging
from datetime import datetime, UTC

import paho.mqtt.client as mqtt

from requests import Session
from requests.auth import HTTPBasicAuth

from zeep import Client
from zeep.transports import Transport


# ==========================================
# CONFIGURACIÓN
# ==========================================

# Importa desde config.py los parámetros necesarios para las conexiones
# MQTT y SOAP, además de los valores utilizados para validar las posiciones.
from config import (
    WSDL_URL,
    USUARIO_EMERCARTO,
    PASSWORD_EMERCARTO,
    USUARIO_MQTT,
    PASSWORD_MQTT,
    MQTT_HOST,
    MQTT_PORT,
    MQTT_TOPIC,
    VENTANA_SEGUNDOS,
    MARGEN_FUTURO_SEGUNDOS,
)


# ==========================================
# NODOS Y PAQUETES PROCESADOS
# ==========================================

# Diccionario que relaciona el identificador numérico de cada nodo
# Meshtastic con el identificador utilizado en la trama enviada por SOAP.
NODOS = {
    1002466242: "TAG1",
    1187009625: "TAG2",
    708490749: "TAG3",
    3897471794: "TAG4",
    1970286148: "GATEWAY"
}

# Diccionario que almacena temporalmente los paquetes procesados
# correctamente para evitar enviar posiciones duplicadas.
#
# La clave está formada por:
# (from, id)
#
# El valor corresponde al instante en el que el paquete fue procesado.
PAQUETES_PROCESADOS = {}


# ==========================================
# LOGGING
# ==========================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

file_handler = logging.FileHandler(
    "mqtt_positions.log",
    encoding="utf-8",
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Añade una cabecera al log para diferenciar cada nueva ejecución.
logger.info("")
logger.info("=" * 60)
logger.info("NUEVA EJECUCIÓN MESHTASTIC-EMERCARTO")
logger.info("=" * 60)


# ==========================================
# CONEXIÓN SOAP
# ==========================================

try:
    # Crea una sesión HTTP que será utilizada por el cliente SOAP.
    session = Session()

    # Configura la autenticación HTTP básica necesaria para acceder
    # al servicio SOAP.
    session.auth = HTTPBasicAuth(
        USUARIO_EMERCARTO,
        PASSWORD_EMERCARTO
    )

    # Crea el transporte que utilizará Zeep para realizar las peticiones
    # utilizando la sesión HTTP configurada anteriormente.
    transport = Transport(session=session)

    # Crea el cliente SOAP utilizando el WSDL y el transporte configurado.
    soap_client = Client(
        WSDL_URL,
        transport=transport
    )

    logger.info(
        "SOAP conectado"
    )

except Exception:
    # Si falla la creación del cliente SOAP se registra como error
    # junto con el traceback de la excepción.
    logger.exception(
        "Error al configurar la conexión SOAP"
    )
    raise


# ==========================================
# FUNCIÓN QUE GENERA LA TRAMA
# ==========================================

def crear_trama(mensaje, identificador):
    """
    Genera la trama que se enviará al servicio SOAP a partir
    de una posición recibida mediante MQTT.

    Los distintos campos se separan mediante el carácter '*'.

    :param mensaje: Mensaje MQTT recibido y convertido desde JSON.
    :param identificador: Identificador asociado al nodo que genera
                          la posición.
    :return: Trama preparada para ser enviada al servicio SOAP.
    """

    # Meshtastic representa latitud y longitud multiplicadas por 10^7.
    # Se dividen para recuperar sus valores en grados.
    lat = mensaje["payload"]["latitude_i"] / 10000000
    lon = mensaje["payload"]["longitude_i"] / 10000000

    # Convierte el timestamp recibido a la representación de fecha UTC
    # utilizada en la trama enviada al servicio SOAP.
    fecha = datetime.fromtimestamp(
        mensaje["payload"]["time"],
        UTC
    ).strftime("%Y%m%d%H%M%S")

    # Construye los campos que componen la trama.
    campos = [
        identificador,   # Identificador (IMEI)
        str(lat),        # Latitud
        str(lon),        # Longitud
        "0",             # Altitud
        fecha,           # Fecha UTC
        "0",             # Satélites
        "",              # PDOP
        "0",             # Velocidad
        "0",             # Alarma
        "",              # Evento
        "",              # Orientación
        "",              # Temperatura
        "0",             # Batería
        "",              # Cobertura GPRS
        "",              # Error horizontal
        "",              # Error vertical
        "",              # HDOP
        "",              # VDOP
        "0",             # Tipo posicionamiento
        ""               # Datos adjuntos
    ]

    # Une todos los campos utilizando '*' como separador.
    return "*".join(campos)


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

        # Descarta posiciones que se encuentren fuera de la ventana
        # temporal permitida.
        if not posicion_valida(mensaje):
            logger.warning(
                "Posición descartada por antigüedad: from=%s id=%s",
                mensaje["from"],
                mensaje["id"]
            )
            return

        # Evita volver a enviar una posición que ya haya sido
        # procesada correctamente.
        if esta_procesado(mensaje):
            logger.info(
                "Posición duplicada ignorada: "
                "from=%s id=%s desde nodo=%s",
                mensaje["from"],
                mensaje["id"],
                NODOS.get(sender_decimal)
            )
            return

        # Convierte la posición recibida al formato requerido
        # por el servicio SOAP.
        trama = crear_trama(
            mensaje,
            nodo
        )

        logger.info("=" * 60)
        logger.info(
            "Posición recibida desde nodo=%s from=%s id=%s",
            NODOS.get(sender_decimal),
            mensaje["from"],
            mensaje["id"]
        )

        logger.info(
            "Trama generada: %s",
            trama
        )

        # Envía la trama al servicio SOAP mediante CargarPosiciones().
        respuesta = soap_client.service.CargarPosiciones(
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
                mensaje["from"],
                mensaje["id"]
            )
            logger.info("=" * 60)

        else:
            logger.warning(
                "Respuesta SOAP no válida. "
                "La posición no se marca como procesada: "
                "from=%s id=%s respuesta=%r",
                mensaje["from"],
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


# ==========================================
# FUNCIONES DE GESTIÓN DE PROCESADOS
# ==========================================

def limpiar_procesados():
    """
    Elimina del registro los paquetes cuyo tiempo de almacenamiento
    haya superado la ventana establecida.

    Esto evita mantener indefinidamente información de paquetes antiguos
    dentro de PAQUETES_PROCESADOS.
    """

    ahora = time.time()

    # Se utiliza una copia de los elementos del diccionario para poder
    # eliminar registros mientras se realiza el recorrido.
    for clave, instante in list(PAQUETES_PROCESADOS.items()):
        if ahora - instante > VENTANA_SEGUNDOS:
            del PAQUETES_PROCESADOS[clave]


def esta_procesado(mensaje):
    """
    Comprueba si un paquete ya fue procesado correctamente.

    Cada paquete se identifica mediante la combinación de su nodo
    origen (from) y su identificador (id).

    :param mensaje: Mensaje que se quiere comprobar.
    :return: True si el paquete ya está registrado, False en caso contrario.
    """

    # Antes de realizar la comprobación se eliminan registros antiguos.
    limpiar_procesados()

    clave = (
        mensaje["from"],
        mensaje["id"],
    )

    return clave in PAQUETES_PROCESADOS


def marcar_procesado(mensaje):
    """
    Registra un paquete como procesado correctamente.

    Guarda como clave la combinación (from, id) y como valor
    el instante en el que se ha realizado el registro.

    :param mensaje: Mensaje que se quiere registrar como procesado.
    """

    clave = (
        mensaje["from"],
        mensaje["id"],
    )

    PAQUETES_PROCESADOS[clave] = time.time()


def posicion_valida(mensaje):
    """
    Comprueba si una posición se encuentra dentro de la ventana
    temporal permitida.

    También se admite un pequeño margen para posiciones futuras
    debido a posibles diferencias entre los relojes de los dispositivos.

    :param mensaje: Mensaje cuya posición se quiere validar.
    :return: True si la posición es válida, False en caso contrario.
    """

    timestamp = mensaje["payload"]["time"]

    # Calcula la antigüedad de la posición en segundos.
    edad = time.time() - timestamp

    # Código auxiliar para comprobar los valores temporales durante
    # las tareas de depuración.
    #
    # logger.info(
    #     "DEBUG TIEMPO: timestamp=%s ahora=%s edad=%s edad_min=%s",
    #     timestamp,
    #     time.time(),
    #     edad,
    #     edad / 60
    # )

    return (
        -MARGEN_FUTURO_SEGUNDOS
        < edad
        <= VENTANA_SEGUNDOS
    )


def respuesta_correcta(respuesta):
    """
    Comprueba si la respuesta recibida desde el servicio SOAP
    corresponde a una operación correcta.

    :param respuesta: Respuesta devuelta por el servicio SOAP.
    :return: True si la respuesta es un texto que comienza por "ok#",
             False en caso contrario.
    """

    return (
        isinstance(respuesta, str)
        and respuesta.startswith("ok#")
    )


# ==========================================
# MQTT
# ==========================================

def on_connect(client, userdata, flags, rc):
    """
    Callback ejecutado cuando finaliza un intento de conexión MQTT.

    Si la conexión se establece correctamente, el cliente se suscribe
    al topic configurado para comenzar a recibir posiciones.

    Si el broker devuelve un código de error, se registra como ERROR.

    :param client: Cliente MQTT que ha realizado el intento de conexión.
    :param userdata: Datos adicionales asociados al cliente.
    :param flags: Información proporcionada por la conexión MQTT.
    :param rc: Código de resultado de la conexión.
    """

    if rc == 0:
        logger.info(
            "Conectado correctamente al servidor MQTT"
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
            rc
        )


# ==========================================
# CLIENTE MQTT
# ==========================================

# Crea el cliente que mantendrá la conexión con el broker MQTT.
mqtt_client = mqtt.Client()

# Muestra los principales datos de conexión para facilitar
# la comprobación de la configuración.
#
# La contraseña no se muestra para evitar exponerla en los logs.
logger.info(
    "Usuario MQTT: %r",
    USUARIO_MQTT
)

logger.info(
    "Host MQTT: %r",
    MQTT_HOST
)

logger.info(
    "Puerto MQTT: %s",
    MQTT_PORT
)

logger.info(
    "Topic MQTT: %s",
    MQTT_TOPIC
)

# Configura las credenciales utilizadas para autenticarse
# contra el broker MQTT.
mqtt_client.username_pw_set(
    USUARIO_MQTT,
    PASSWORD_MQTT
)

# Asocia las funciones callback que deben ejecutarse cuando
# se establece la conexión o se recibe un mensaje MQTT.
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Habilita TLS para proteger la conexión con el endpoint MQTT.
mqtt_client.tls_set()

# Activa los logs internos de Paho MQTT para facilitar
# el diagnóstico de problemas de conexión.
mqtt_client.enable_logger()


# ==========================================
# INICIO DEL CLIENTE MQTT
# ==========================================

try:
    # Intenta establecer la conexión inicial con el broker MQTT
    # utilizando el host y puerto definidos en config.py.
    mqtt_client.connect(
        MQTT_HOST,
        MQTT_PORT
    )

    logger.info(
        "Escuchando MQTT..."
    )

    logger.info(
        "Topic: %s",
        MQTT_TOPIC
    )

    # Mantiene activo el bucle de red de MQTT.
    #
    # Desde este punto Paho procesa las comunicaciones con el broker
    # y ejecuta los callbacks on_connect() y on_message() cuando corresponde.
    mqtt_client.loop_forever()

except KeyboardInterrupt:
    # Ctrl+C es una parada voluntaria y no se considera un error.
    logger.info(
        "Servicio detenido manualmente"
    )

except Exception:
    # Registra cualquier error no controlado producido durante
    # la ejecución del cliente MQTT junto con su traceback.
    logger.exception(
        "Error general en la conexión o ejecución del cliente MQTT"
    )

finally:
    # Deja constancia del final de la ejecución actual.
    logger.info("=" * 60)
    logger.info(
        "Fin de la ejecución MESHTASTIC-EMERCARTO"
    )

    logger.info(
        "=" * 60
    )
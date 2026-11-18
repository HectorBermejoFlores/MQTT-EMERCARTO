import paho.mqtt.client as mqtt

from modules.config import (
    MQTT_HOST,
    MQTT_PORT,
    MQTT_TOPIC,
    PASSWORD_MQTT,
    USUARIO_MQTT,
)
from modules.logger import configurar_logging, logger
from modules.mqtt_handlers import on_connect, on_message
import modules.soap as soap


def main():
    configurar_logging()
    soap_client = soap.conectar_soap()

    callback_api = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api is None:
        mqtt_client = mqtt.Client()
        logger.warning(
            "Paho MQTT antiguo detectado; usando la API de callbacks heredada"
        )
    else:
        mqtt_client = mqtt.Client(
            callback_api_version=callback_api.VERSION2
        )
    mqtt_client.user_data_set(soap_client)

    logger.info("Usuario MQTT: %r", USUARIO_MQTT)
    logger.info("Host MQTT: %r", MQTT_HOST)
    logger.info("Puerto MQTT: %s", MQTT_PORT)
    logger.info("Topic MQTT: %s", MQTT_TOPIC)

    mqtt_client.username_pw_set(
        USUARIO_MQTT,
        PASSWORD_MQTT,
    )
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.tls_set()
    mqtt_client.enable_logger(logger)

    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT)
        logger.info("Escuchando MQTT...")
        logger.info("Topic: %s", MQTT_TOPIC)
        mqtt_client.loop_forever()

    except KeyboardInterrupt:
        logger.info("Servicio detenido manualmente")

    except Exception:
        logger.exception(
            "Error general en la conexión o ejecución del cliente MQTT"
        )

    finally:
        logger.info("=" * 60)
        logger.info("Fin de la ejecución MESHTASTIC-EMERCARTO")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()

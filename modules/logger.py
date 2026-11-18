import logging


logger = logging.getLogger("mqtt_emercarto")
logger.setLevel(logging.INFO)
logger.propagate = False


def configurar_logging():
    if logger.handlers:
        return

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

    logger.info("")
    logger.info("=" * 60)
    logger.info("NUEVA EJECUCIÓN MESHTASTIC-EMERCARTO")
    logger.info("=" * 60)

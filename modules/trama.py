from datetime import UTC, datetime


def crear_trama(mensaje, identificador):
    """
    Genera la trama SOAP a partir de la posición recibida por MQTT.
    """
    payload = mensaje.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("falta el objeto 'payload'")

    campos_requeridos = ("latitude_i", "longitude_i", "sentTime")
    campos_ausentes = [
        campo
        for campo in campos_requeridos
        if payload.get(campo) is None
    ]
    if campos_ausentes:
        raise ValueError(
            "faltan campos obligatorios en payload: "
            + ", ".join(campos_ausentes)
        )

    latitud = payload["latitude_i"]
    longitud = payload["longitude_i"]
    timestamp = payload["sentTime"]
    for nombre, valor in (
        ("latitude_i", latitud),
        ("longitude_i", longitud),
        ("sentTime", timestamp),
    ):
        if isinstance(valor, bool) or not isinstance(valor, (int, float)):
            raise ValueError(f"el campo '{nombre}' debe ser numérico")

    lat = latitud / 10_000_000
    lon = longitud / 10_000_000

    try:
        fecha = datetime.fromtimestamp(timestamp, UTC).strftime(
            "%Y%m%d%H%M%S"
        )
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError(
            "el campo 'sentTime' no contiene una fecha válida"
        ) from error

    campos = [
        identificador,
        str(lat),
        str(lon),
        "0",
        fecha,
        "0",
        "",
        "0",
        "0",
        "",
        "",
        "",
        "0",
        "",
        "",
        "",
        "",
        "",
        "0",
        "",
    ]
    return "*".join(campos)

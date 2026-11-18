from modules.logger import logger
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport

from modules.config import (
    WSDL_URL,
    USUARIO_EMERCARTO,
    PASSWORD_EMERCARTO,
)

# ==========================================
# CONEXIÓN SOAP
# ==========================================
def conectar_soap():
    """
    Crea y devuelve un cliente SOAP configurado para conectarse
    al servicio de Emercarto.

    :return: Cliente SOAP configurado.
    """
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

        return soap_client

    except Exception:
        # Si falla la creación del cliente SOAP se registra como error
        # junto con el traceback de la excepción.
        logger.exception(
            "Error al configurar la conexión SOAP"
        )
        raise
# MQTT-EMERCARTO

Este script recibe mensajes de posición por MQTT, los valida, los registra y
envía las posiciones válidas al servicio EMERCARTO mediante SOAP.

## Requisitos

- Python 3.11 o posterior.
- Acceso de red al broker MQTT y al servicio SOAP.
- Credenciales válidas para ambos servicios.

## Instalación

Las dependencias externas están listadas en `requirements.txt` (`zeep`,
`requests`, `paho-mqtt` 2.0 o posterior y `tzdata`). `csv`, `pathlib`,
`datetime` y `zoneinfo` forman parte de Python y no se instalan por separado.

Abre una terminal en la carpeta del proyecto e instala las dependencias:

```powershell
python -m pip install -r requirements.txt
```

## Configuración

1. Copia o cambia el nombre de `configExample.py` a `config.py`. El script
   importa la configuración desde `config.py`.
2. Abre `config.py` y rellena los valores necesarios:

| Campo | Qué debes indicar |
|---|---|
| `USUARIO_EMERCARTO` | Usuario para autenticarte en el servicio SOAP. |
| `PASSWORD_EMERCARTO` | Contraseña de ese usuario. |
| `MQTT_HOST` | Nombre de host o dirección del broker MQTT. |
| `MQTT_PORT` | Puerto del broker MQTT, como número entero. |
| `MQTT_TOPIC` | Topic al que se suscribirá el cliente. |
| `USUARIO_MQTT` | Usuario para autenticarte en el broker. |
| `PASSWORD_MQTT` | Contraseña de ese usuario. |
| `VENTANA_SEGUNDOS` | Antigüedad máxima admitida para una posición, en segundos. |

El ejemplo trae `VENTANA_SEGUNDOS = 1 * 60` (un minuto) y
`MARGEN_FUTURO_SEGUNDOS = 60` (un minuto). Ajusta esos valores a tus necesidades.

El diccionario `NODOS` de `main.py` relaciona el valor numérico `from` de cada
nodo con el nombre que usa el proyecto (por ejemplo, `TAG1`). Actualízalo si
cambian los identificadores de los dispositivos. Los nombres se utilizan en
los logs, la ruta y el CSV.

No compartas ni subas a un repositorio público el archivo `config.py`: contiene
credenciales.

## Ejecución

Desde la carpeta del proyecto:

```powershell
python main.py
```

El script configura el cliente SOAP al arrancar y después intenta conectarse al
broker MQTT. Cuando MQTT confirma la conexión, se suscribe al topic configurado
y comienza a procesar mensajes. Para detenerlo manualmente, pulsa `Ctrl+C`.

Se procesan mensajes cuyo campo `type` sea `"position"`. El mensaje debe
contener un nodo conocido y una posición con `payload.latitude_i`,
`payload.longitude_i` y `payload.receivedTime`. La latitud y longitud de
Meshtastic se convierten dividiéndolas por `10 000 000`; el timestamp se usa
como fecha de la posición. Los mensajes incompletos, antiguos, duplicados o de
nodos desconocidos se descartan. La ruta `route` es opcional; cuando está
presente, se espera una lista de hasta ocho identificadores enteros de nodo.

## Logs

El script muestra logs en la terminal y también los guarda en
`mqtt_positions.log`, en la carpeta desde la que se ejecuta. El archivo se
mantiene entre ejecuciones y cada inicio añade una cabecera para distinguir la
nueva sesión.

Los logs incluyen el estado de las conexiones, las posiciones recibidas, las
horas de envío y recepción, la ruta, los valores de señal, la trama generada y
la respuesta SOAP. También registran advertencias y errores, por ejemplo,
mensajes no válidos, posiciones descartadas o errores de conexión.

## Archivos CSV

Tras una conexión MQTT correcta, el script crea un archivo CSV nuevo dentro de
la carpeta `csv`. El nombre incluye la fecha y hora de inicio del script en la
zona horaria `Europe/Madrid`, por ejemplo:

```text
csv/05-11-2026_09-39-02-169067.csv
```

El archivo se crea con una cabecera y se van añadiendo filas a medida que se
procesan posiciones válidas, antes de enviarlas a SOAP. Una reconexión MQTT
dentro de la misma ejecución continúa usando el mismo archivo. Otra ejecución
creará uno nuevo.

Las columnas actuales son:

| Columna | Contenido |
|---|---|
| `from` | Nombre del nodo que originó la posición. |
| `sender` | Nombre del nodo que envió o retransmitió el mensaje, si está registrado. |
| `latitud`, `longitud` | Coordenadas convertidas a grados. |
| `id` | Identificador del mensaje MQTT. |
| `ruta` | Nombres de los nodos de la ruta separados por `-->`; puede estar vacía si no hay ruta. |
| `SNR`, `RSSI` | Valores de señal recibidos, si están disponibles. |
| `hora_creación` | Hora de envío indicada por `payload.sentTime`, en hora local. |
| `hora_llegada` | Hora de recepción indicada por `payload.receivedTime`, en hora local. |

La carpeta `csv` está excluida de Git mediante `.gitignore`; los archivos
permanecen en tu equipo.

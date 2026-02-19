"""
ATICA WhatsApp Bridge v2.1
Conecta WhatsApp Cloud API con la API SICETAC (sicetac-mcp-atiemppo).

API repo: https://github.com/imatjuanmatiz/sicetac-mcp-atiemppo
API URL:  https://sicetac-api-mcp.onrender.com

Endpoints disponibles en la API:
  POST /consulta         → JSON con totales H2/H4/H8 (puede incluir variantes)
  POST /consulta_resumen → Siempre resumen
  POST /consulta_texto   → Devuelve { "texto": "..." } ya formateado
  GET  /health           → Health check
  POST /refresh          → Refresca cache de datos
"""

from fastapi import FastAPI, Request, Response
import requests
import os
import re
import logging
from datetime import datetime

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("atica-whatsapp")

app = FastAPI(title="ATICA WhatsApp Bridge", version="2.1.0")

# ====== CONFIGURACIÓN DESDE VARIABLES DE ENTORNO ======
VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN", "aticatoken123")
WHATSAPP_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")
WHATSAPP_PHONE_ID = os.environ.get("WHATSAPP_PHONE_ID")

# Base URL de la API SICETAC (sin trailing slash)
SICETAC_API_BASE = os.environ.get(
    "SICETAC_API_URL",
    "https://sicetac-api-mcp.onrender.com",
).rstrip("/")

# Si la variable tiene /consulta al final, quitarlo para tener la base
if SICETAC_API_BASE.endswith("/consulta"):
    SICETAC_API_BASE = SICETAC_API_BASE.replace("/consulta", "")

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT_MS", "30000")) / 1000

# ====== CONFIGURACIONES VEHICULARES VÁLIDAS ======
# Ref: ConsultaInput en sicetac_service.py — vehiculo default: "C3S3"
VEHICULOS_VALIDOS = [
    "C2S1", "C2S2", "C3S1", "C3S2", "C3S3",
    "C2S1S1", "C2S2S2", "C3S2S1", "C3S2S2", "C3S3S3",
]

CARROCERIAS_VALIDAS = [
    "GENERAL", "CONTENEDOR", "GRANEL", "LIQUIDOS",
    "REFRIGERADO", "PLATAFORMA",
]


# ====== PARSEO DE MENSAJES ======
def normalizar_ciudad(texto: str) -> str:
    """Normaliza nombres de ciudades colombianas comunes."""
    texto = texto.strip().title()
    alias = {
        "Bogota": "Bogotá",
        "Medellin": "Medellín",
        "Barranquilla": "Barranquilla",
        "Cartagena": "Cartagena",
        "Bucaramanga": "Bucaramanga",
        "Cali": "Cali",
        "Santa Marta": "Santa Marta",
        "Santamarta": "Santa Marta",
        "Bquilla": "Barranquilla",
        "Baq": "Barranquilla",
        "Ctg": "Cartagena",
        "Ctgna": "Cartagena",
        "Bga": "Bucaramanga",
        "Sta Marta": "Santa Marta",
        "Sta. Marta": "Santa Marta",
        "Pereira": "Pereira",
        "Manizales": "Manizales",
        "Ibague": "Ibagué",
        "Cucuta": "Cúcuta",
        "Villavicencio": "Villavicencio",
        "Buenaventura": "Buenaventura",
        "Tunja": "Tunja",
        "Neiva": "Neiva",
        "Popayan": "Popayán",
        "Pasto": "Pasto",
        "Monteria": "Montería",
        "Sincelejo": "Sincelejo",
        "Valledupar": "Valledupar",
        "Riohacha": "Riohacha",
        "Quibdo": "Quibdó",
        "Florencia": "Florencia",
        "Yopal": "Yopal",
        "Arauca": "Arauca",
        "Leticia": "Leticia",
        "Sogamoso": "Sogamoso",
        "Duitama": "Duitama",
        "Zipaquira": "Zipaquirá",
        "Girardot": "Girardot",
        "Turbo": "Turbo",
        "Apartado": "Apartadó",
        "Honda": "Honda",
        "Ipiales": "Ipiales",
        "Tumaco": "Tumaco",
        "Barrancabermeja": "Barrancabermeja",
    }
    return alias.get(texto, texto)


def parsear_ruta(texto: str) -> dict | None:
    """
    Extrae origen y destino del texto libre.
    Patrones soportados:
      - "bogota a barranquilla"
      - "de bogota a barranquilla"
      - "bogota - barranquilla"
      - "bogota → barranquilla"
      - "bogota hasta barranquilla"
    """
    texto = texto.strip().lower()

    # Limpiar prefijos comunes
    texto = re.sub(
        r"^(hola|buenos días|buenas tardes|buenas noches|buenas|consulta|consultar|"
        r"ruta|flete|tarifa|cuanto cuesta|cuánto cuesta|precio|calcular|calcular ruta|"
        r"valor|costo)\s*[,.:;]?\s*",
        "",
        texto,
    )

    # Patrón: "de X a Y" o "X a Y" o "X - Y" o "X → Y" o "X hasta Y"
    match = re.search(
        r"(?:de\s+)?(.+?)\s+(?:a|hasta|->|→|–|-)\s+(.+)",
        texto,
    )
    if match:
        origen_raw = match.group(1).strip()
        destino_raw = match.group(2).strip()

        # Limpiar posibles opciones del destino (ej: "barranquilla c3s3 contenedor vacio")
        PALABRAS_OPCION = {"VACIO", "VACÍO", "CARGADO"}
        destino_parts = destino_raw.split()
        destino_clean = []
        for part in destino_parts:
            if part.upper() in VEHICULOS_VALIDOS or part.upper() in CARROCERIAS_VALIDAS or part.upper() in PALABRAS_OPCION:
                break
            destino_clean.append(part)

        origen = normalizar_ciudad(" ".join(origen_raw.split()))
        destino = normalizar_ciudad(" ".join(destino_clean))

        if origen and destino:
            return {"origen": origen, "destino": destino}

    return None


def parsear_vehiculo(texto: str) -> str | None:
    """Extrae configuración vehicular si se menciona."""
    texto_upper = texto.upper()
    for v in VEHICULOS_VALIDOS:
        if v in texto_upper:
            return v
    return None


def parsear_carroceria(texto: str) -> str | None:
    """Extrae tipo de carrocería si se menciona."""
    texto_upper = texto.upper()
    for c in CARROCERIAS_VALIDAS:
        if c in texto_upper:
            return c
    return None


def parsear_modo_viaje(texto: str) -> str | None:
    """Detecta si el usuario pide modo vacío."""
    texto_lower = texto.lower()
    if "vacio" in texto_lower or "vacío" in texto_lower:
        return "VACIO"
    return None


# ====== CONSULTA SICETAC ======
def consultar_sicetac(
    origen: str,
    destino: str,
    vehiculo: str = None,
    carroceria: str = None,
    modo_viaje: str = None,
) -> dict | None:
    """
    Llama al API SICETAC POST /consulta con resumen=true.
    Ref: ConsultaInput en sicetac_service.py
    """
    payload = {
        "origen": origen,
        "destino": destino,
        "resumen": True,
    }
    if vehiculo:
        payload["vehiculo"] = vehiculo
    if carroceria:
        payload["carroceria"] = carroceria
    if modo_viaje:
        payload["modo_viaje"] = modo_viaje

    url = f"{SICETAC_API_BASE}/consulta"
    logger.info(f"SICETAC [{url}] payload: {payload}")

    try:
        resp = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        logger.info(f"SICETAC response: status={resp.status_code}")

        # La API lanza HTTPException con status 400/404/500
        if resp.status_code >= 400:
            try:
                err_data = resp.json()
                detail = err_data.get("detail", str(err_data))
            except Exception:
                detail = resp.text
            logger.error(f"SICETAC error {resp.status_code}: {detail}")
            return {"_error": True, "_status": resp.status_code, "_detail": detail}

        data = resp.json()
        return data

    except requests.exceptions.Timeout:
        logger.error("SICETAC timeout")
        return None
    except Exception as e:
        logger.error(f"SICETAC exception: {e}")
        return None


# ====== FORMATO DE RESPUESTA ======
def fmt_cop(valor) -> str:
    """Formatea un número como pesos colombianos."""
    try:
        v = float(valor)
        return f"${v:,.0f}".replace(",", ".")
    except Exception:
        return str(valor)


def formatear_respuesta(data: dict) -> str:
    """
    Formatea la respuesta SICETAC para WhatsApp.
    Maneja tanto respuesta simple (totales) como con variantes.
    """
    try:
        origen = data.get("origen", "?")
        destino = data.get("destino", "?")
        config = data.get("configuracion", "C3S3")
        carroceria = data.get("carroceria", "GENERAL")
        mes = data.get("mes", "")
        modo = data.get("modo_viaje", "CARGADO")

        lineas = [
            f"📦 *Ruta:* {origen} → {destino}",
            f"🚛 *Vehículo:* {config} | *Carrocería:* {carroceria}",
        ]

        if modo != "CARGADO":
            lineas.append(f"📋 *Modo:* {modo}")

        lineas.append(f"📅 *Periodo:* {mes}")

        # ====== CASO: VARIANTES (múltiples rutas) ======
        if "variantes" in data:
            variantes = data["variantes"]
            lineas.append("")
            lineas.append(f"*Se encontraron {len(variantes)} ruta(s):*")

            for i, var in enumerate(variantes, 1):
                nombre = var.get("NOMBRE_SICE", f"Ruta {i}")
                id_sice = var.get("ID_SICE", "")
                tot = var.get("totales", {})

                lineas.append("")
                lineas.append(f"*{i}. {nombre}* (ID: {id_sice})")

                h2 = tot.get("H2")
                h4 = tot.get("H4")
                h8 = tot.get("H8")
                if h2 is not None:
                    lineas.append(f"   H2: {fmt_cop(h2)}")
                if h4 is not None:
                    lineas.append(f"   H4: {fmt_cop(h4)}")
                if h8 is not None:
                    lineas.append(f"   H8: {fmt_cop(h8)}")

        # ====== CASO: RUTA ÚNICA ======
        else:
            totales = data.get("totales", {})
            h2 = totales.get("H2")
            h4 = totales.get("H4")
            h8 = totales.get("H8")

            lineas.append("")
            lineas.append("*Valores de referencia SICETAC:*")
            if h2 is not None:
                lineas.append(f"• H2: {fmt_cop(h2)} COP")
            if h4 is not None:
                lineas.append(f"• H4: {fmt_cop(h4)} COP")
            if h8 is not None:
                lineas.append(f"• H8: {fmt_cop(h8)} COP")

        lineas.append("")
        lineas.append("_Fuente: SICETAC - Min. Transporte_")

        return "\n".join(lineas)

    except Exception as e:
        logger.error(f"Error formateando: {e}")
        return "Error al formatear la respuesta. Intenta de nuevo."


def mensaje_ayuda() -> str:
    """Retorna mensaje de ayuda/bienvenida."""
    return (
        "👋 *¡Hola! Soy ATICA*, tu asistente de consulta SICETAC.\n\n"
        "Escríbeme una ruta y te doy los valores de referencia del Ministerio de Transporte.\n\n"
        "*Ejemplos:*\n"
        "• _Bogotá a Barranquilla_\n"
        "• _De Medellín a Cartagena_\n"
        "• _Cali - Buenaventura_\n\n"
        "*Opciones avanzadas:*\n"
        "• Vehículo: _Bogotá a Cali C2S2_\n"
        "• Carrocería: _Bogotá a Cali contenedor_\n"
        "• Vacío: _Bogotá a Cali vacío_\n\n"
        "Escribe *ayuda* en cualquier momento."
    )


# ====== HEALTH CHECK ======
@app.get("/")
async def health():
    return {
        "service": "atica-whatsapp-bridge",
        "version": "2.1.0",
        "status": "running",
        "sicetac_api": SICETAC_API_BASE,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ====== WEBHOOK DE VERIFICACIÓN (GET) ======
@app.get("/webhook")
async def verify(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    logger.info(f"Webhook verify: mode={mode}, token_match={token == VERIFY_TOKEN}")

    if mode == "subscribe" and token == VERIFY_TOKEN and challenge:
        # Meta espera el challenge como texto plano
        return Response(content=challenge, media_type="text/plain")
    return {"status": "forbidden"}


# ====== MENSAJES ENTRANTES (POST) ======
@app.post("/webhook")
async def receive_message(request: Request):
    data = await request.json()

    try:
        entry = data["entry"][0]
        change = entry["changes"][0]
        value = change["value"]
        messages = value.get("messages")

        if not messages:
            return {"status": "no messages"}

        message = messages[0]
        from_number = message["from"]

        # Solo texto por ahora
        if "text" not in message:
            send_whatsapp_message(
                to=from_number,
                body="Por ahora solo proceso mensajes de texto. Escribe una ruta como: _Bogotá a Barranquilla_",
            )
            return {"status": "non-text"}

        user_text = message["text"]["body"].strip()
        logger.info(f"MSG [{from_number}]: {user_text}")

    except Exception as e:
        logger.error(f"Parse error: {e}")
        return {"status": "parse error", "detail": str(e)}

    # ====== COMANDOS ESPECIALES ======
    texto_lower = user_text.lower().strip()
    if texto_lower in (
        "hola", "hi", "hello", "ayuda", "help",
        "menu", "menú", "inicio", "start", "?",
    ):
        send_whatsapp_message(to=from_number, body=mensaje_ayuda())
        return {"status": "help sent"}

    # ====== PARSEAR RUTA ======
    ruta = parsear_ruta(user_text)

    if not ruta:
        send_whatsapp_message(
            to=from_number,
            body=(
                "No pude identificar la ruta. 🤔\n\n"
                "Escríbela así:\n"
                "• _Bogotá a Barranquilla_\n"
                "• _De Medellín a Cartagena_\n\n"
                "Escribe *ayuda* para más opciones."
            ),
        )
        return {"status": "no route parsed"}

    # Opciones adicionales del texto
    vehiculo = parsear_vehiculo(user_text)
    carroceria = parsear_carroceria(user_text)
    modo_viaje = parsear_modo_viaje(user_text)

    # ====== CONSULTAR SICETAC ======
    resultado = consultar_sicetac(
        origen=ruta["origen"],
        destino=ruta["destino"],
        vehiculo=vehiculo,
        carroceria=carroceria,
        modo_viaje=modo_viaje,
    )

    if resultado is None:
        send_whatsapp_message(
            to=from_number,
            body="⚠️ No pude conectar con SICETAC en este momento. Intenta de nuevo en 1 minuto.",
        )
        return {"status": "sicetac timeout/error"}

    # Verificar si la API devolvió un error HTTP (400/404/500)
    if resultado.get("_error"):
        detail = resultado.get("_detail", "Error desconocido")
        status = resultado.get("_status", 500)

        if status == 404:
            msg = (
                f"⚠️ No encontré la ruta *{ruta['origen']}* → *{ruta['destino']}*.\n\n"
                "Verifica que los nombres de las ciudades/municipios sean correctos.\n"
                "Escribe *ayuda* para ver ejemplos."
            )
        elif status == 400:
            msg = f"⚠️ Datos inválidos: {detail}"
        else:
            msg = "⚠️ Error en el servidor SICETAC. Intenta de nuevo en unos minutos."

        send_whatsapp_message(to=from_number, body=msg)
        return {"status": "sicetac api error", "code": status, "detail": detail}

    # Verificar si hay "error" en el body (respuesta 200 pero con error)
    if "error" in resultado and not resultado.get("totales") and not resultado.get("variantes"):
        error_msg = resultado.get("error", "Error desconocido")
        send_whatsapp_message(
            to=from_number,
            body=f"⚠️ {error_msg}",
        )
        return {"status": "sicetac body error", "detail": error_msg}

    # ====== FORMATEAR Y ENVIAR ======
    respuesta = formatear_respuesta(resultado)
    send_whatsapp_message(to=from_number, body=respuesta)

    logger.info(f"OK [{from_number}]: {ruta['origen']} -> {ruta['destino']}")
    return {"status": "ok"}


# ====== ENVÍO DE MENSAJES WHATSAPP ======
def send_whatsapp_message(to: str, body: str):
    """Envía un mensaje de texto por WhatsApp Cloud API (Graph API v22.0)."""
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_ID:
        logger.warning("Missing WhatsApp credentials — skipping send")
        return

    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_ID}/messages"

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": body[:4096],
        },
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        logger.info(f"WA send [{to}]: status={resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"WA error: {resp.text}")
    except Exception as e:
        logger.error(f"WA send error: {e}")

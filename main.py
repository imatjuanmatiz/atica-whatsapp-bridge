"""
ATICA WhatsApp Bridge v3.0
Conecta WhatsApp Cloud API con la API SICETAC y, de forma opcional,
con OpenAI para dar respuestas conversacionales.
"""

from datetime import datetime, timezone
import json
import logging
import os
import re

from fastapi import FastAPI, Request, Response
import requests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("atica-whatsapp")

app = FastAPI(title="ATICA WhatsApp Bridge", version="3.0.0")


VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN", "aticatoken123")
WHATSAPP_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")
WHATSAPP_PHONE_ID = os.environ.get("WHATSAPP_PHONE_ID")

SICETAC_API_BASE = os.environ.get(
    "SICETAC_API_URL",
    "https://sicetac-api-mcp.onrender.com",
).rstrip("/")
if SICETAC_API_BASE.endswith("/consulta"):
    SICETAC_API_BASE = SICETAC_API_BASE.replace("/consulta", "")

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT_MS", "30000")) / 1000

OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-5-mini").strip()
OPENAI_API_URL = (os.environ.get("OPENAI_API_URL") or "https://api.openai.com/v1/responses").strip()

LEAD_CAPTURE_WEBHOOK_URL = (os.environ.get("LEAD_CAPTURE_WEBHOOK_URL") or "").strip()
CAPTURE_WEBHOOK_SECRET = (os.environ.get("CAPTURE_WEBHOOK_SECRET") or "").strip()
LEAD_CAPTURE_AUTH_TOKEN = (os.environ.get("LEAD_CAPTURE_AUTH_TOKEN") or "").strip()
LEAD_CAPTURE_APIKEY = (os.environ.get("LEAD_CAPTURE_APIKEY") or LEAD_CAPTURE_AUTH_TOKEN).strip()

VEHICULOS_VALIDOS = [
    "C278",
    "C289",
    "C2910",
    "C2M10",
    "C3",
    "C2S2",
    "C2S3",
    "C3S2",
    "C3S3",
    "V3",
]

CARROCERIA_ALIASES = {
    "GENERAL": "General - Estacas",
    "GENERAL ESTACAS": "General - Estacas",
    "GENERAL - ESTACAS": "General - Estacas",
    "GENERAL ESTIBA": "General - Estacas",
    "GENERAL - ESTIBA": "General - Estacas",
    "ESTIBA": "General - Estibas",
    "ESTIBAS": "General - Estibas",
    "GENERAL ESTIBAS": "General - Estibas",
    "GENERAL - ESTIBAS": "General - Estibas",
    "PLATAFORMA": "General - Plataforma",
    "GENERAL PLATAFORMA": "General - Plataforma",
    "GENERAL - PLATAFORMA": "General - Plataforma",
    "FURGON GENERAL": "General - Furgon",
    "GENERAL FURGON": "General - Furgon",
    "GENERAL - FURGON": "General - Furgon",
    "PORTACONTENEDORES": "Portacontenedores",
    "PORTA CONTENEDORES": "Portacontenedores",
    "CONTENEDOR PORTACONTENEDORES": "Portacontenedores",
    "FURGON REFRIGERADO": "Furgon Refrigerado",
    "CARGA REFRIGERADA": "Furgon Refrigerado",
    "REFRIGERADO": "Furgon Refrigerado",
    "ESTACAS GRANEL SOLIDO": "Granel Solido - Estacas",
    "GRANEL SOLIDO ESTACAS": "Granel Solido - Estacas",
    "GRANEL SOLIDO - ESTACAS": "Granel Solido - Estacas",
    "FURGON GRANEL SOLIDO": "Granel Solido - Furgon",
    "GRANEL SOLIDO FURGON": "Granel Solido - Furgon",
    "GRANEL SOLIDO - FURGON": "Granel Solido - Furgon",
    "VOLCO": "Granel Solido - Volco",
    "GRANEL SOLIDO VOLCO": "Granel Solido - Volco",
    "GRANEL SOLIDO - VOLCO": "Granel Solido - Volco",
    "ESTIBAS GRANEL SOLIDO": "Granel Solido - Estibas",
    "GRANEL SOLIDO ESTIBAS": "Granel Solido - Estibas",
    "GRANEL SOLIDO - ESTIBAS": "Granel Solido - Estibas",
    "PLATAFORMA GRANEL SOLIDO": "Granel Solido - Plataforma",
    "GRANEL SOLIDO PLATAFORMA": "Granel Solido - Plataforma",
    "GRANEL SOLIDO - PLATAFORMA": "Granel Solido - Plataforma",
    "TANQUE - GRANEL LIQUIDO": "Granel Liquido - Tanque",
    "TANQUE GRANEL LIQUIDO": "Granel Liquido - Tanque",
    "GRANEL LIQUIDO TANQUE": "Granel Liquido - Tanque",
}

CARROCERIAS_VALIDAS = list(set(CARROCERIA_ALIASES.values()))

VEHICULO_DESCRIPCIONES = {
    "C278": "camion rigido de 2 ejes",
    "C289": "camion rigido de 2 ejes de mayor capacidad",
    "C2910": "camion rigido de 2 ejes de mayor tonelaje",
    "C2M10": "configuracion para mula de 2 ejes motrices",
    "C3": "camion rigido de 3 ejes",
    "C2S2": "tractocamion de 2 ejes con semirremolque de 2 ejes",
    "C2S3": "tractocamion de 2 ejes con semirremolque de 3 ejes",
    "C3S2": "tractocamion de 3 ejes con semirremolque de 2 ejes",
    "C3S3": "tractocamion de 3 ejes con semirremolque de 3 ejes",
    "V3": "vehiculo liviano o configuracion especial segun la tabla base",
}

LEAD_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
LEAD_COMPANY_RE = re.compile(
    r"(?:empresa|compañ[ií]a|compania|transportadora|soy de|trabajo en)\s*[:\-]?\s*([A-Za-z0-9ÁÉÍÓÚÑáéíóúñ .,&-]{3,80})",
    re.IGNORECASE,
)

# Estado liviano por teléfono. Es efímero, pero mejora la conversación
# sin agregar una dependencia obligatoria de persistencia.
CONVERSATION_STATE: dict[str, dict] = {}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalizar_ciudad(texto: str) -> str:
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
    texto = texto.strip().lower()
    texto = re.sub(
        r"^(hola|buenos días|buenas tardes|buenas noches|buenas|consulta|consultar|"
        r"ruta|flete|tarifa|cuanto cuesta|cuánto cuesta|precio|calcular|calcular ruta|"
        r"valor|costo)\s*[,.:;]?\s*",
        "",
        texto,
    )

    match = re.search(
        r"(?:de\s+)?(.+?)\s+(?:a|hasta|->|→|–|-)\s+(.+)",
        texto,
    )
    if not match:
        return None

    origen_raw = match.group(1).strip()
    destino_raw = match.group(2).strip()

    palabras_opcion = {"VACIO", "VACÍO", "CARGADO"}
    destino_parts = destino_raw.split()
    destino_clean = []
    for part in destino_parts:
        if part.upper() in VEHICULOS_VALIDOS or part.upper() in CARROCERIAS_VALIDAS or part.upper() in palabras_opcion:
            break
        destino_clean.append(part)

    origen = normalizar_ciudad(" ".join(origen_raw.split()))
    destino = normalizar_ciudad(" ".join(destino_clean))
    if origen and destino:
        return {"origen": origen, "destino": destino}
    return None


def parsear_vehiculo(texto: str) -> str | None:
    texto_upper = texto.upper()
    for vehiculo in VEHICULOS_VALIDOS:
        if vehiculo in texto_upper:
            return vehiculo
    return None


def parsear_carroceria(texto: str) -> str | None:
    texto_upper = texto.upper()
    for alias, canonical in CARROCERIA_ALIASES.items():
        if alias in texto_upper:
            return canonical
    return None


def parsear_modo_viaje(texto: str) -> str | None:
    return None


def usuario_pide_vacio(texto: str) -> bool:
    texto_lower = texto.lower()
    return "vacio" in texto_lower or "vacío" in texto_lower


def detectar_pregunta_configuracion(texto: str) -> str | None:
    texto_upper = texto.upper()
    if "VEHICULO" in texto_upper or "VEHÍCULO" in texto_upper or "CONFIGURACION" in texto_upper or "CONFIGURACIÓN" in texto_upper:
        for vehiculo in VEHICULOS_VALIDOS:
            if vehiculo in texto_upper:
                return vehiculo
    return None


def mensaje_configuracion_vehiculo(vehiculo: str) -> str:
    descripcion = VEHICULO_DESCRIPCIONES.get(vehiculo, "configuración vehicular SICETAC")
    return (
        f"*{vehiculo}* corresponde a {descripcion}.\n\n"
        "Si quieres, te calculo una ruta con esa configuración. "
        "Escríbeme por ejemplo: _Bogotá a Barranquilla "
        f"{vehiculo}_"
    )


def extraer_email(texto: str) -> str | None:
    match = LEAD_EMAIL_RE.search(texto or "")
    return match.group(0).strip() if match else None


def extraer_empresa(texto: str) -> str | None:
    match = LEAD_COMPANY_RE.search(texto or "")
    if not match:
        return None
    empresa = match.group(1).strip(" .,:;-")
    return empresa[:80] if empresa else None


def fmt_cop(valor) -> str:
    try:
        v = float(valor)
        return f"${v:,.0f}".replace(",", ".")
    except Exception:
        return str(valor)


def formatear_respuesta(data: dict) -> str:
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
                if tot.get("H2") is not None:
                    lineas.append(f"   H2: {fmt_cop(tot.get('H2'))}")
                if tot.get("H4") is not None:
                    lineas.append(f"   H4: {fmt_cop(tot.get('H4'))}")
                if tot.get("H8") is not None:
                    lineas.append(f"   H8: {fmt_cop(tot.get('H8'))}")
        else:
            totales = data.get("totales", {})
            lineas.append("")
            lineas.append("*Valores de referencia SICETAC:*")
            if totales.get("H2") is not None:
                lineas.append(f"• H2: {fmt_cop(totales.get('H2'))} COP")
            if totales.get("H4") is not None:
                lineas.append(f"• H4: {fmt_cop(totales.get('H4'))} COP")
            if totales.get("H8") is not None:
                lineas.append(f"• H8: {fmt_cop(totales.get('H8'))} COP")

        lineas.append("")
        lineas.append("_Fuente: SICETAC - Min. Transporte_")
        return "\n".join(lineas)
    except Exception as e:
        logger.error(f"Error formateando: {e}")
        return "Error al formatear la respuesta. Intenta de nuevo."


def mensaje_ayuda() -> str:
    return (
        "👋 *¡Hola! Soy ATICA*, tu asistente de consulta SICETAC.\n\n"
        "Escríbeme una ruta y te doy los valores de referencia cargados del Ministerio de Transporte.\n\n"
        "*Ejemplos:*\n"
        "• _Bogotá a Barranquilla_\n"
        "• _De Medellín a Cartagena_\n"
        "• _Cali - Buenaventura_\n\n"
        "*Configuraciones disponibles:*\n"
        "• _C278, C289, C2910, C2M10, C3, C2S2, C2S3, C3S2, C3S3, V3_\n\n"
        "*Carrocerías soportadas:*\n"
        "• _General - Estacas_\n"
        "• _General - Furgon_\n"
        "• _General - Estibas_\n"
        "• _General - Plataforma_\n"
        "• _Portacontenedores_\n"
        "• _Furgon Refrigerado_\n"
        "• _Granel Solido - Estacas_\n"
        "• _Granel Solido - Furgon_\n"
        "• _Granel Solido - Volco_\n"
        "• _Granel Solido - Estibas_\n"
        "• _Granel Solido - Plataforma_\n"
        "• _Granel Liquido - Tanque_\n\n"
        "Ejemplo avanzado: _Bogotá a Cali C3S3 portacontenedores_\n\n"
        "Escribe *ayuda* en cualquier momento."
    )


def get_contact_name(value: dict) -> str | None:
    contacts = value.get("contacts") or []
    if not contacts:
        return None
    profile = contacts[0].get("profile") or {}
    name = (profile.get("name") or "").strip()
    return name or None


def get_state(phone: str) -> dict:
    return CONVERSATION_STATE.setdefault(
        phone,
        {
            "previous_response_id": None,
            "lead": {
                "phone": phone,
                "profile_name": None,
                "name": None,
                "company": None,
                "email": None,
                "first_seen_at": utcnow_iso(),
            },
            "last_route": None,
        },
    )


def merge_lead_data(state: dict, profile_name: str | None, text: str):
    lead = state["lead"]
    if profile_name and not lead.get("profile_name"):
        lead["profile_name"] = profile_name
    if profile_name and not lead.get("name"):
        lead["name"] = profile_name

    email = extraer_email(text)
    if email:
        lead["email"] = email

    empresa = extraer_empresa(text)
    if empresa:
        lead["company"] = empresa

    lead["last_message_at"] = utcnow_iso()
    lead["last_message"] = text[:500]


def consultar_sicetac(
    origen: str,
    destino: str,
    vehiculo: str = None,
    carroceria: str = None,
    modo_viaje: str = None,
) -> dict | None:
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

        if resp.status_code >= 400:
            try:
                err_data = resp.json()
                detail = err_data.get("detail", str(err_data))
            except Exception:
                detail = resp.text
            logger.error(f"SICETAC error {resp.status_code}: {detail}")
            return {"_error": True, "_status": resp.status_code, "_detail": detail}

        return resp.json()
    except requests.exceptions.Timeout:
        logger.error("SICETAC timeout")
        return None
    except Exception as e:
        logger.error(f"SICETAC exception: {e}")
        return None


def build_sicetac_snapshot(data: dict | None) -> dict | None:
    if not data:
        return None

    variantes = data.get("variantes") or []
    if variantes:
        return {
            "origen": data.get("origen"),
            "destino": data.get("destino"),
            "configuracion": data.get("configuracion"),
            "carroceria": data.get("carroceria"),
            "mes": data.get("mes"),
            "modo_viaje": data.get("modo_viaje"),
            "routes_count": len(variantes),
            "routes": [
                {
                    "nombre": item.get("NOMBRE_SICE"),
                    "id_sice": item.get("ID_SICE"),
                    "h2": (item.get("totales") or {}).get("H2"),
                    "h4": (item.get("totales") or {}).get("H4"),
                    "h8": (item.get("totales") or {}).get("H8"),
                }
                for item in variantes[:5]
            ],
        }

    totales = data.get("totales", {})
    return {
        "origen": data.get("origen"),
        "destino": data.get("destino"),
        "configuracion": data.get("configuracion"),
        "carroceria": data.get("carroceria"),
        "mes": data.get("mes"),
        "modo_viaje": data.get("modo_viaje"),
        "routes_count": 1 if totales else 0,
        "routes": [
            {
                "nombre": "Ruta principal",
                "h2": totales.get("H2"),
                "h4": totales.get("H4"),
                "h8": totales.get("H8"),
            }
        ]
        if totales
        else [],
    }


def capture_lead_event(payload: dict):
    if not LEAD_CAPTURE_WEBHOOK_URL:
        return

    headers = {"Content-Type": "application/json"}
    if CAPTURE_WEBHOOK_SECRET:
        headers["x-capture-secret"] = CAPTURE_WEBHOOK_SECRET
    if LEAD_CAPTURE_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {LEAD_CAPTURE_AUTH_TOKEN}"
    if LEAD_CAPTURE_APIKEY:
        headers["apikey"] = LEAD_CAPTURE_APIKEY

    try:
        requests.post(
            LEAD_CAPTURE_WEBHOOK_URL,
            headers=headers,
            json=payload,
            timeout=1.5,
        )
    except Exception as e:
        logger.warning(f"Lead capture skipped: {e}")


def extract_response_text(data: dict) -> str | None:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    output = data.get("output") or []
    parts = []
    for item in output:
        for content in item.get("content") or []:
            if content.get("type") == "output_text":
                text = (content.get("text") or "").strip()
                if text:
                    parts.append(text)
    return "\n".join(parts) if parts else None


def generar_respuesta_ia(
    *,
    phone: str,
    profile_name: str | None,
    user_text: str,
    state: dict,
    ruta: dict | None,
    resultado_sicetac: dict | None,
    deterministic_reply: str,
) -> str | None:
    if not OPENAI_API_KEY:
        return None

    context_payload = {
        "phone": phone,
        "profile_name": profile_name,
        "lead": state.get("lead"),
        "detected_route": ruta,
        "last_route": state.get("last_route"),
        "sicetac_result": build_sicetac_snapshot(resultado_sicetac),
        "fallback_reply": deterministic_reply,
    }

    payload = {
        "model": OPENAI_MODEL,
        "instructions": (
            "Eres ATICA, un asistente comercial y operativo por WhatsApp. "
            "Responde en español de Colombia con tono cercano, concreto y profesional. "
            "Nunca inventes valores SICETAC; usa solo lo que venga en el contexto. "
            "Si hay resultado SICETAC, explícalo y luego sugiere el siguiente paso. "
            "Si falta la ruta, pide solo el dato faltante. "
            "Si faltan datos del lead, pide máximo un dato por turno entre nombre, empresa y correo. "
            "Puedes usar fallback_reply como base, pero mejóralo para que suene conversacional. "
            "No uses bloques de código. Máximo 900 caracteres."
        ),
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_text},
                    {"type": "input_text", "text": f"CONTEXTO_JSON: {json.dumps(context_payload, ensure_ascii=False)}"},
                ],
            }
        ],
    }

    previous_response_id = state.get("previous_response_id")
    if previous_response_id:
        payload["previous_response_id"] = previous_response_id

    try:
        resp = requests.post(
            OPENAI_API_URL,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )
        if resp.status_code >= 400:
            logger.error(f"OpenAI error {resp.status_code}: {resp.text}")
            return None

        data = resp.json()
        text = extract_response_text(data)
        if text:
            state["previous_response_id"] = data.get("id")
            return text[:4096]
    except Exception as e:
        logger.error(f"OpenAI exception: {e}")

    return None


@app.get("/")
async def health():
    return {
        "service": "atica-whatsapp-bridge",
        "version": "3.0.0",
        "status": "running",
        "sicetac_api": SICETAC_API_BASE,
        "openai_enabled": bool(OPENAI_API_KEY),
        "lead_capture_enabled": bool(LEAD_CAPTURE_WEBHOOK_URL),
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/webhook")
async def verify(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    logger.info(f"Webhook verify: mode={mode}, token_match={token == VERIFY_TOKEN}")

    if mode == "subscribe" and token == VERIFY_TOKEN and challenge:
        return Response(content=challenge, media_type="text/plain")
    return {"status": "forbidden"}


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
        profile_name = get_contact_name(value)
        state = get_state(from_number)

        if "text" not in message:
            send_whatsapp_message(
                to=from_number,
                body="Por ahora solo proceso mensajes de texto. Escribe una ruta como: _Bogotá a Barranquilla_",
            )
            return {"status": "non-text"}

        user_text = message["text"]["body"].strip()
        merge_lead_data(state, profile_name, user_text)
        logger.info(f"MSG [{from_number}]: {user_text}")
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return {"status": "parse error", "detail": str(e)}

    texto_lower = user_text.lower().strip()
    if texto_lower in ("hola", "hi", "hello", "ayuda", "help", "menu", "menú", "inicio", "start", "?"):
        fallback_reply = mensaje_ayuda()
        respuesta = generar_respuesta_ia(
            phone=from_number,
            profile_name=profile_name,
            user_text=user_text,
            state=state,
            ruta=None,
            resultado_sicetac=None,
            deterministic_reply=fallback_reply,
        ) or fallback_reply
        send_whatsapp_message(to=from_number, body=respuesta)
        capture_lead_event(
            {
                "event": "help_requested",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
            }
        )
        return {"status": "help sent"}

    if usuario_pide_vacio(user_text):
        msg = (
            "Por ahora este canal solo entrega valores *cargados*.\n\n"
            "Si quieres, te calculo la ruta con una configuración y carrocería válidas. "
            "Ejemplo: _Bogotá a Barranquilla C3S3 portacontenedores_."
        )
        send_whatsapp_message(to=from_number, body=msg)
        capture_lead_event(
            {
                "event": "unsupported_vacio_request",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
                "message": user_text,
            }
        )
        return {"status": "unsupported vacio"}

    vehiculo_consultado = detectar_pregunta_configuracion(user_text)
    if vehiculo_consultado:
        send_whatsapp_message(to=from_number, body=mensaje_configuracion_vehiculo(vehiculo_consultado))
        capture_lead_event(
            {
                "event": "vehicle_info_requested",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
                "message": user_text,
            }
        )
        return {"status": "vehicle info sent"}

    ruta = parsear_ruta(user_text)
    if not ruta:
        fallback_reply = (
            "No pude identificar la ruta. 🤔\n\n"
            "Escríbela así:\n"
            "• _Bogotá a Barranquilla_\n"
            "• _De Medellín a Cartagena_\n\n"
            "Escribe *ayuda* para más opciones."
        )
        respuesta = generar_respuesta_ia(
            phone=from_number,
            profile_name=profile_name,
            user_text=user_text,
            state=state,
            ruta=None,
            resultado_sicetac=None,
            deterministic_reply=fallback_reply,
        ) or fallback_reply
        send_whatsapp_message(to=from_number, body=respuesta)
        capture_lead_event(
            {
                "event": "message_without_route",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
                "message": user_text,
            }
        )
        return {"status": "no route parsed"}

    vehiculo = parsear_vehiculo(user_text)
    carroceria = parsear_carroceria(user_text)
    modo_viaje = parsear_modo_viaje(user_text)

    resultado = consultar_sicetac(
        origen=ruta["origen"],
        destino=ruta["destino"],
        vehiculo=vehiculo,
        carroceria=carroceria,
        modo_viaje=modo_viaje,
    )

    if resultado is None:
        fallback_reply = "⚠️ No pude conectar con SICETAC en este momento. Intenta de nuevo en 1 minuto."
        respuesta = generar_respuesta_ia(
            phone=from_number,
            profile_name=profile_name,
            user_text=user_text,
            state=state,
            ruta=ruta,
            resultado_sicetac=None,
            deterministic_reply=fallback_reply,
        ) or fallback_reply
        send_whatsapp_message(to=from_number, body=respuesta)
        return {"status": "sicetac timeout/error"}

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

        respuesta = generar_respuesta_ia(
            phone=from_number,
            profile_name=profile_name,
            user_text=user_text,
            state=state,
            ruta=ruta,
            resultado_sicetac=None,
            deterministic_reply=msg,
        ) or msg
        send_whatsapp_message(to=from_number, body=respuesta)
        return {"status": "sicetac api error", "code": status, "detail": detail}

    if "error" in resultado and not resultado.get("totales") and not resultado.get("variantes"):
        fallback_reply = f"⚠️ {resultado.get('error', 'Error desconocido')}"
        respuesta = generar_respuesta_ia(
            phone=from_number,
            profile_name=profile_name,
            user_text=user_text,
            state=state,
            ruta=ruta,
            resultado_sicetac=None,
            deterministic_reply=fallback_reply,
        ) or fallback_reply
        send_whatsapp_message(to=from_number, body=respuesta)
        return {"status": "sicetac body error", "detail": resultado.get("error")}

    state["last_route"] = {
        "origen": ruta["origen"],
        "destino": ruta["destino"],
        "vehiculo": vehiculo or "C3S3",
        "carroceria": carroceria,
        "modo_viaje": modo_viaje,
        "consulted_at": utcnow_iso(),
    }

    respuesta_deterministica = formatear_respuesta(resultado)
    respuesta = generar_respuesta_ia(
        phone=from_number,
        profile_name=profile_name,
        user_text=user_text,
        state=state,
        ruta=ruta,
        resultado_sicetac=resultado,
        deterministic_reply=respuesta_deterministica,
    ) or respuesta_deterministica
    send_whatsapp_message(to=from_number, body=respuesta)

    capture_lead_event(
        {
            "event": "route_consulted",
            "ts": utcnow_iso(),
            "channel": "whatsapp",
            "lead": state["lead"],
            "route": state["last_route"],
            "sicetac": build_sicetac_snapshot(resultado),
            "message": user_text,
        }
    )

    logger.info(f"OK [{from_number}]: {ruta['origen']} -> {ruta['destino']}")
    return {"status": "ok"}


def send_whatsapp_message(to: str, body: str):
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

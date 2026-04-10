"""
ATICA WhatsApp Bridge v3.1
Conecta WhatsApp Cloud API con la API SICETAC y, de forma opcional,
con OpenAI para dar respuestas conversacionales.
"""

from datetime import datetime, timezone
import json
import logging
import os
import re
import unicodedata

from fastapi import FastAPI, Request, Response
import requests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("atica-whatsapp")

app = FastAPI(title="ATICA WhatsApp Bridge", version="3.1.0")


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
DEFAULT_VEHICULO = "C3S3"
DEFAULT_CARROCERIA = "General - Estacas"

BODY_TYPE_OPTIONS = [
    "General - Estacas",
    "General - Furgon",
    "General - Estibas",
    "General - Plataforma",
    "Portacontenedores",
    "Furgon Refrigerado",
    "Granel Solido - Estacas",
    "Granel Solido - Furgon",
    "Granel Solido - Volco",
    "Granel Solido - Estibas",
    "Granel Solido - Plataforma",
    "Granel Liquido - Tanque",
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

CARROCERIAS_VALIDAS = BODY_TYPE_OPTIONS[:]

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

TONELADAS_REFERENCIA = {
    "C3S3": 34.0,
    "C3S2": 32.0,
    "C2S3": 30.0,
    "C2S2": 28.0,
    "C3": 17.0,
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


def normalizar_lookup_texto(valor: str | None) -> str:
    texto = str(valor or "").strip().upper()
    texto = texto.replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
    texto = re.sub(r"\s+", " ", texto)
    return texto


def quitar_tildes(valor: str | None) -> str:
    texto = str(valor or "")
    texto = unicodedata.normalize("NFKD", texto)
    return "".join(ch for ch in texto if not unicodedata.combining(ch))


def normalizar_carroceria(texto: str | None) -> str | None:
    if not texto:
        return None
    cleaned = re.sub(r"\s+", " ", texto).strip()
    if not cleaned:
        return None
    normalized = normalizar_lookup_texto(cleaned)
    return CARROCERIA_ALIASES.get(normalized) or cleaned


def limpiar_fragmento_ruta(texto: str) -> str:
    return re.sub(r"^[,.;:\s]+|[,.;:\s]+$", "", texto or "").strip()


def recortar_destino(destino_raw: str) -> str:
    palabras_opcion = {"VACIO", "VACÍO", "CARGADO"}
    destino_parts = destino_raw.split()
    destino_clean = []
    for index in range(len(destino_parts)):
        remaining = " ".join(destino_parts[index:])
        part = destino_parts[index]
        if part.upper() in palabras_opcion:
            break
        if part.upper() in VEHICULOS_VALIDOS:
            break
        if re.match(r"^\d+(?:[.,]\d+)?$", part) and index + 1 < len(destino_parts):
            siguiente = destino_parts[index + 1].upper()
            if siguiente in {"HORA", "HORAS", "HR", "HRS", "H", "TON", "TONS", "TONELADA", "TONELADAS"}:
                break
        if part.upper() in {"HORA", "HORAS", "HR", "HRS", "H", "TON", "TONS", "TONELADA", "TONELADAS"}:
            break
        if normalizar_carroceria(remaining) in CARROCERIAS_VALIDAS:
            break
        destino_clean.append(part)
    return " ".join(destino_clean).strip()


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
        r"^(valor por tonelada|por tonelada|cuanto por tonelada|cuánto por tonelada|valor por ton|por ton|"
        r"hola|buenos días|buenas tardes|buenas noches|buenas|consulta|consultar|"
        r"ruta|flete|tarifa|cuanto cuesta|cuánto cuesta|precio|calcular|calcular ruta|"
        r"valor|costo|ahora|ok|bueno|entonces)\s*[,.:;]?\s*",
        "",
        texto,
    )

    patterns = [
        r"^(?:de\s+)?(.+?)\s+a\s+(.+)$",
        r"^(?:de\s+)?(.+?)\s+para\s+(.+)$",
        r"^(.+?)\s*->\s*(.+)$",
        r"^(.+?)\s*-\s*(.+)$",
        r"^entre\s+(.+?)\s+y\s+(.+)$",
        r"^origen\s+(.+?)\s+destino\s+(.+)$",
        r"^origen[:\s]+(.+?)\s+destino[:\s]+(.+)$",
    ]

    for pattern in patterns:
        match = re.search(pattern, texto, re.IGNORECASE)
        if not match:
            continue

        origen_raw = limpiar_fragmento_ruta(match.group(1))
        destino_raw = limpiar_fragmento_ruta(match.group(2))
        origen = normalizar_ciudad(" ".join(origen_raw.split()))
        destino = normalizar_ciudad(recortar_destino(destino_raw))
        if origen and destino:
            return {"origen": origen, "destino": destino}
    return None


def parsear_vehiculo(texto: str) -> str | None:
    texto_upper = texto.upper()
    for vehiculo in sorted(VEHICULOS_VALIDOS, key=len, reverse=True):
        if vehiculo in texto_upper:
            return vehiculo
    return None


def parsear_carroceria(texto: str) -> str | None:
    texto_normalizado = normalizar_lookup_texto(texto)
    for alias, canonical in CARROCERIA_ALIASES.items():
        if alias in texto_normalizado:
            return canonical
    return None


def parsear_modo_viaje(texto: str) -> str | None:
    return None


def parsear_horas_personalizadas(texto: str) -> float | None:
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:horas|hora|hrs|hr|h)\b", texto, re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except Exception:
        return None


def parsear_toneladas(texto: str) -> float | None:
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:toneladas|tonelada|tons|ton)\b", texto, re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except Exception:
        return None


def usuario_pide_valor_por_tonelada(texto: str) -> bool:
    texto_normalizado = normalizar_lookup_texto(texto)
    return "POR TONELADA" in texto_normalizado or "VALOR TONELADA" in texto_normalizado or "VALOR POR TON" in texto_normalizado


def usuario_pide_otra_hora(texto: str) -> bool:
    texto_normalizado = normalizar_lookup_texto(texto)
    return bool(parsear_horas_personalizadas(texto)) and (
        "HORA" in texto_normalizado
        or "CARGUE" in texto_normalizado
        or "DESCARGUE" in texto_normalizado
        or "PROCESO" in texto_normalizado
        or "LOGIST" in texto_normalizado
    )


def usuario_pide_vacio(texto: str) -> bool:
    texto_lower = texto.lower()
    return "vacio" in texto_lower or "vacío" in texto_lower


def detectar_pregunta_configuracion(texto: str) -> str | None:
    texto_upper = texto.upper()
    if (
        "QUE ES" in texto_upper
        or "QUÉ ES" in texto_upper
        or "QUE SIGNIFICA" in texto_upper
        or "QUÉ SIGNIFICA" in texto_upper
        or "TIPO DE VEHICULO" in texto_upper
        or "TIPO DE VEHÍCULO" in texto_upper
    ):
        for vehiculo in VEHICULOS_VALIDOS:
            if vehiculo in texto_upper:
                return vehiculo
    return None


def mensaje_configuracion_vehiculo(vehiculo: str) -> str:
    descripcion = VEHICULO_DESCRIPCIONES.get(vehiculo, "configuración vehicular SICETAC")
    return (
        f"{vehiculo} corresponde a {quitar_tildes(descripcion)}.\n\n"
        "Si quieres, te calculo una ruta con esa configuracion. "
        "Escribeme por ejemplo: Bogota a Barranquilla "
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


def fmt_decimal(valor: float | int | None) -> str | None:
    if valor is None:
        return None
    try:
        numero = float(valor)
        if numero.is_integer():
            return str(int(numero))
        return f"{numero:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return None


def extraer_totales(data: dict | None) -> dict:
    if not data:
        return {}
    if data.get("totales"):
        return data.get("totales") or {}
    variantes = data.get("variantes") or []
    if len(variantes) == 1:
        return variantes[0].get("totales") or {}
    return {}


def calcular_valor_hora_desde_totales(totales: dict | None) -> float | None:
    if not totales:
        return None
    h2 = totales.get("H2")
    h4 = totales.get("H4")
    h8 = totales.get("H8")
    try:
        if h4 is not None and h8 is not None:
            return (float(h8) - float(h4)) / 4.0
        if h2 is not None and h4 is not None:
            return (float(h4) - float(h2)) / 2.0
    except Exception:
        return None
    return None


def calcular_total_para_horas(data: dict | None, horas: float) -> float | None:
    totales = extraer_totales(data)
    if not totales:
        return None
    valor_hora = calcular_valor_hora_desde_totales(totales)
    if valor_hora is None:
        return None
    try:
        h8 = totales.get("H8")
        if h8 is not None:
            base_movilizacion = float(h8) - (8.0 * valor_hora)
            return round(base_movilizacion + (float(horas) * valor_hora), 2)
        h4 = totales.get("H4")
        if h4 is not None:
            base_movilizacion = float(h4) - (4.0 * valor_hora)
            return round(base_movilizacion + (float(horas) * valor_hora), 2)
        h2 = totales.get("H2")
        if h2 is not None:
            base_movilizacion = float(h2) - (2.0 * valor_hora)
            return round(base_movilizacion + (float(horas) * valor_hora), 2)
    except Exception:
        return None
    return None


def formatear_respuesta(data: dict, *, include_closing: bool = True) -> str:
    try:
        origen = quitar_tildes(data.get("origen", "?"))
        destino = quitar_tildes(data.get("destino", "?"))
        config = data.get("configuracion", "C3S3")
        carroceria = quitar_tildes(data.get("carroceria", DEFAULT_CARROCERIA))
        mes = data.get("mes", "")

        lineas = [
            f"Ruta: {origen} a {destino}",
            f"Configuracion: {config} | Carroceria: {carroceria}",
        ]
        if mes:
            lineas.append(f"Periodo: {mes}")

        if "variantes" in data:
            variantes = data["variantes"]
            lineas.append(f"Variantes encontradas: {len(variantes)}")

            for i, var in enumerate(variantes, 1):
                nombre = quitar_tildes(var.get("NOMBRE_SICE", f"Ruta {i}"))
                id_sice = var.get("ID_SICE", "")
                tot = var.get("totales", {})
                etiqueta = f"{i}. {nombre}"
                if id_sice:
                    etiqueta += f" (ID {id_sice})"
                lineas.append(etiqueta)
                if tot.get("H2") is not None:
                    lineas.append(f"H2: {fmt_cop(tot.get('H2'))}")
                if tot.get("H4") is not None:
                    lineas.append(f"H4: {fmt_cop(tot.get('H4'))}")
                if tot.get("H8") is not None:
                    lineas.append(f"H8: {fmt_cop(tot.get('H8'))}")
        else:
            totales = data.get("totales", {})
            lineas.append("Valores SICETAC:")
            if totales.get("H2") is not None:
                lineas.append(f"H2: {fmt_cop(totales.get('H2'))}")
            if totales.get("H4") is not None:
                lineas.append(f"H4: {fmt_cop(totales.get('H4'))}")
            if totales.get("H8") is not None:
                lineas.append(f"H8: {fmt_cop(totales.get('H8'))}")

        if include_closing:
            lineas.append("")
            lineas.append("Escribe otra ruta asi: origen a destino.")
        return "\n".join(lineas)
    except Exception as e:
        logger.error(f"Error formateando: {e}")
        return "Error al formatear la respuesta. Intenta de nuevo."


def mensaje_ayuda() -> str:
    return (
        "ATICA consulta rutas SICETAC.\n\n"
        "Escribe la ruta directo asi: origen a destino.\n"
        f"Si no indicas configuracion o carroceria, uso {DEFAULT_VEHICULO} y {quitar_tildes(DEFAULT_CARROCERIA)}.\n\n"
        "Ejemplos:\n"
        "- Bogota a Barranquilla\n"
        "- Medellin a Cartagena C3S3\n"
        "- Cali a Buenaventura portacontenedores\n\n"
        "Proyecto de Atiemppo.com. Puedes calcular otra ruta escribiendo: origen a destino."
    )


def mensaje_opciones() -> str:
    return (
        "Configuraciones: C278, C289, C2910, C2M10, C3, C2S2, C2S3, C3S2, C3S3 y V3.\n\n"
        "Carrocerias: General - Estacas, General - Furgon, General - Estibas, General - Plataforma, "
        "Portacontenedores, Furgon Refrigerado, Granel Solido - Estacas, Granel Solido - Furgon, "
        "Granel Solido - Volco, Granel Solido - Estibas, Granel Solido - Plataforma y Granel Liquido - Tanque.\n\n"
        f"Si no indicas una, uso {DEFAULT_VEHICULO} y {quitar_tildes(DEFAULT_CARROCERIA)}.\n\n"
        "Ejemplos:\n"
        "- Bogota a Barranquilla\n"
        "- Medellin a Cartagena C3S3\n"
        "- Cali a Buenaventura portacontenedores"
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
            "last_result": None,
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
    resumen: bool = True,
    horas_logisticas: float | None = None,
    tarifa_standby: float | None = None,
) -> dict | None:
    payload = {
        "origen": origen,
        "destino": destino,
        "resumen": resumen,
    }
    if vehiculo:
        payload["vehiculo"] = vehiculo
    if carroceria:
        payload["carroceria"] = carroceria
    if modo_viaje:
        payload["modo_viaje"] = modo_viaje
    if horas_logisticas is not None:
        payload["horas_logisticas"] = horas_logisticas
        payload["horas_logisticas_personalizadas"] = horas_logisticas
    if tarifa_standby is not None:
        payload["tarifa_standby"] = tarifa_standby

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


def resolver_toneladas_configuracion(vehiculo: str) -> float | None:
    return TONELADAS_REFERENCIA.get((vehiculo or "").strip().upper())


def formatear_valor_por_tonelada(
    *,
    resultado: dict,
    vehiculo: str,
    horas: float | None = None,
    toneladas: float | None = None,
) -> str:
    toneladas_base = toneladas if toneladas is not None else resolver_toneladas_configuracion(vehiculo)
    if not toneladas_base:
        return (
            "Puedo calcular el valor por tonelada, pero necesito la tonelada a usar.\n\n"
            "Escribelo por ejemplo asi: Bogota a Barranquilla 12 toneladas."
        )

    total = None
    etiqueta_horas = "H8"
    if horas is not None:
        total = calcular_total_para_horas(resultado, horas)
        etiqueta_horas = f"{fmt_decimal(horas)}h"
    if total is None:
        total = extraer_totales(resultado).get("H8")
        etiqueta_horas = "H8"
    if total is None:
        return "No pude calcular el valor por tonelada para esa ruta."

    valor_ton = float(total) / float(toneladas_base)
    origen = quitar_tildes(resultado.get("origen"))
    destino = quitar_tildes(resultado.get("destino"))
    toneladas_txt = fmt_decimal(toneladas_base) or str(toneladas_base)
    return (
        f"Ruta: {origen} a {destino}\n"
        f"Configuracion: {vehiculo}\n"
        f"Referencia usada: {etiqueta_horas} = {fmt_cop(total)}\n"
        f"Toneladas: {toneladas_txt}\n"
        f"Valor por tonelada: {fmt_cop(valor_ton)}\n\n"
        "Escribe otra ruta asi: origen a destino."
    )


def formatear_valor_personalizado_por_horas(
    *,
    resultado: dict,
    horas: float,
    vehiculo: str,
    toneladas: float | None = None,
    incluir_por_tonelada: bool = False,
) -> str:
    total = calcular_total_para_horas(resultado, horas)
    if total is None:
        return "No pude calcular ese valor por horas con la informacion disponible."

    origen = quitar_tildes(resultado.get("origen"))
    destino = quitar_tildes(resultado.get("destino"))
    horas_txt = fmt_decimal(horas) or str(horas)
    lineas = [
        f"Ruta: {origen} a {destino}",
        f"Configuracion: {vehiculo}",
        f"Valor SICETAC para {horas_txt} horas: {fmt_cop(total)}",
    ]
    if incluir_por_tonelada:
        toneladas_base = toneladas if toneladas is not None else resolver_toneladas_configuracion(vehiculo)
        if toneladas_base:
            valor_ton = total / toneladas_base
            lineas.append(f"Valor por tonelada: {fmt_cop(valor_ton)} usando {fmt_decimal(toneladas_base)} t")
    lineas.append("")
    lineas.append("Escribe otra ruta asi: origen a destino.")
    return "\n".join(lineas)


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
            "Eres ATICA, un asistente operativo por WhatsApp enfocado en calcular rutas SICETAC. "
            "Responde en espanol de Colombia, corto, puntual y profesional. "
            "Nunca inventes valores SICETAC; usa solo lo que venga en el contexto. "
            "Tu prioridad es detectar la ruta o el calculo solicitado. "
            "Si el mensaje ya contiene una ruta razonablemente identificable, asumela y avanza sin pedir confirmaciones innecesarias. "
            "Si hay resultado SICETAC, devuelvelo de forma breve y luego invita a escribir otra ruta. "
            "Si falta la ruta, pide solo el dato faltante, no varios pasos a la vez. "
            "Usa fallback_reply como base y mantenlo sintetico. "
            "No ofrezcas cotizacion formal. "
            "No uses expresiones coloquiales como 'entiendo el lio'. "
            "No uses bloques de codigo. Maximo 450 caracteres."
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


def resolver_contexto_consulta(user_text: str, state: dict) -> tuple[dict | None, bool]:
    ruta_detectada = parsear_ruta(user_text)
    if ruta_detectada:
        return ruta_detectada, True

    requiere_contexto_anterior = usuario_pide_valor_por_tonelada(user_text) or usuario_pide_otra_hora(user_text)
    if requiere_contexto_anterior and state.get("last_route"):
        last_route = state["last_route"]
        return {
            "origen": last_route.get("origen"),
            "destino": last_route.get("destino"),
        }, False
    return None, False


@app.get("/")
async def health():
    return {
        "service": "atica-whatsapp-bridge",
        "version": "3.1.0",
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
        send_whatsapp_message(to=from_number, body=mensaje_ayuda())
        capture_lead_event(
            {
                "event": "help_requested",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
            }
        )
        return {"status": "help sent"}

    if texto_lower in ("opciones", "configuraciones", "vehiculos", "vehículos", "carrocerias", "carrocerias", "menu opciones"):
        send_whatsapp_message(to=from_number, body=mensaje_opciones())
        capture_lead_event(
            {
                "event": "options_requested",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
            }
        )
        return {"status": "options sent"}

    if usuario_pide_vacio(user_text):
        msg = (
            "Por ahora este canal solo entrega valores cargados.\n\n"
            "Escribe la ruta asi: Bogota a Barranquilla C3S3."
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

    ruta, ruta_en_mensaje_actual = resolver_contexto_consulta(user_text, state)
    vehiculo_consultado = detectar_pregunta_configuracion(user_text)
    if vehiculo_consultado and not ruta:
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

    if not ruta:
        fallback_reply = (
            "No pude identificar la ruta.\n\n"
            "Escribela directo asi:\n"
            "Bogota a Barranquilla\n"
            "Medellin a Cartagena"
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

    vehiculo = parsear_vehiculo(user_text) or (state.get("last_route") or {}).get("vehiculo") or DEFAULT_VEHICULO
    carroceria = parsear_carroceria(user_text) or (state.get("last_route") or {}).get("carroceria") or DEFAULT_CARROCERIA
    modo_viaje = parsear_modo_viaje(user_text)
    horas_personalizadas = parsear_horas_personalizadas(user_text)
    toneladas_explicitas = parsear_toneladas(user_text)
    pide_valor_ton = usuario_pide_valor_por_tonelada(user_text)
    pide_horas = usuario_pide_otra_hora(user_text)

    resultado = consultar_sicetac(
        origen=ruta["origen"],
        destino=ruta["destino"],
        vehiculo=vehiculo,
        carroceria=carroceria,
        modo_viaje=modo_viaje,
    )

    if resultado is None:
        fallback_reply = "No pude conectar con SICETAC en este momento. Intenta de nuevo en 1 minuto."
        send_whatsapp_message(to=from_number, body=fallback_reply)
        return {"status": "sicetac timeout/error"}

    if resultado.get("_error"):
        detail = resultado.get("_detail", "Error desconocido")
        status = resultado.get("_status", 500)

        if status == 404:
            msg = (
                f"No encontre la ruta {quitar_tildes(ruta['origen'])} a {quitar_tildes(ruta['destino'])}.\n\n"
                "Verifica los nombres y escribela otra vez en formato origen a destino."
            )
        elif status == 400:
            msg = f"Datos invalidos: {quitar_tildes(detail)}"
        else:
            msg = "Error en el servidor SICETAC. Intenta de nuevo en unos minutos."
        send_whatsapp_message(to=from_number, body=msg)
        return {"status": "sicetac api error", "code": status, "detail": detail}

    if "error" in resultado and not resultado.get("totales") and not resultado.get("variantes"):
        fallback_reply = quitar_tildes(resultado.get("error", "Error desconocido"))
        send_whatsapp_message(to=from_number, body=fallback_reply)
        return {"status": "sicetac body error", "detail": resultado.get("error")}

    state["last_route"] = {
        "origen": ruta["origen"],
        "destino": ruta["destino"],
        "vehiculo": vehiculo or "C3S3",
        "carroceria": carroceria,
        "modo_viaje": modo_viaje,
        "consulted_at": utcnow_iso(),
    }
    state["last_result"] = resultado

    respuesta_deterministica = formatear_respuesta(resultado)
    respuesta = respuesta_deterministica
    if pide_horas and horas_personalizadas is not None:
        respuesta = formatear_valor_personalizado_por_horas(
            resultado=resultado,
            horas=horas_personalizadas,
            vehiculo=vehiculo,
            toneladas=toneladas_explicitas,
            incluir_por_tonelada=pide_valor_ton,
        )
    elif pide_valor_ton:
        respuesta = formatear_valor_por_tonelada(
            resultado=resultado,
            vehiculo=vehiculo,
            horas=horas_personalizadas if pide_horas else None,
            toneladas=toneladas_explicitas,
        )
    elif not ruta_en_mensaje_actual and (pide_horas or pide_valor_ton):
        respuesta = respuesta_deterministica
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

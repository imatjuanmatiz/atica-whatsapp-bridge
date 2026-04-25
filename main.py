"""
ATICA WhatsApp Bridge v3.3
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

app = FastAPI(title="ATICA WhatsApp Bridge", version="3.3.0")


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
MUNICIPIOS_CACHE_TTL_SECONDS = int(os.environ.get("MUNICIPIOS_CACHE_TTL_SECONDS", "3600"))

OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-5-mini").strip()
OPENAI_API_URL = (os.environ.get("OPENAI_API_URL") or "https://api.openai.com/v1/responses").strip()
OPENAI_FALLBACK_ENABLED = (
    (os.environ.get("OPENAI_FALLBACK_ENABLED") or "false").strip().lower() == "true"
)

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

MANUAL_MUNICIPIO_ALIASES = {
    "BOGOTA": {"nombre_oficial": "BOGOTÁ, D.C.", "codigo_dane": "11001000", "departamento": "BOGOTÁ, D.C."},
    "BOGOTA DC": {"nombre_oficial": "BOGOTÁ, D.C.", "codigo_dane": "11001000", "departamento": "BOGOTÁ, D.C."},
    "CALI": {"nombre_oficial": "SANTIAGO DE CALI", "codigo_dane": "76001000", "departamento": "VALLE DEL CAUCA"},
    "MEDELLIN": {"nombre_oficial": "MEDELLÍN", "codigo_dane": "05001000", "departamento": "ANTIOQUIA"},
    "BARRANQUILLA": {"nombre_oficial": "BARRANQUILLA", "codigo_dane": "08001000", "departamento": "ATLÁNTICO"},
    "CARTAGENA": {"nombre_oficial": "CARTAGENA DE INDIAS", "codigo_dane": "13001000", "departamento": "BOLÍVAR"},
    "BUCARAMANGA": {"nombre_oficial": "BUCARAMANGA", "codigo_dane": "68001000", "departamento": "SANTANDER"},
    "PEREIRA": {"nombre_oficial": "PEREIRA", "codigo_dane": "66001000", "departamento": "RISARALDA"},
    "BUENAVENTURA": {"nombre_oficial": "BUENAVENTURA", "codigo_dane": "76109000", "departamento": "VALLE DEL CAUCA"},
    "JAMUNDI": {"nombre_oficial": "JAMUNDÍ", "codigo_dane": "76364000", "departamento": "VALLE DEL CAUCA"},
    "PUERTO SALGAR": {"nombre_oficial": "PUERTO SALGAR", "codigo_dane": "25572000", "departamento": "CUNDINAMARCA"},
    "FUNZA": {"nombre_oficial": "FUNZA", "codigo_dane": "25286000", "departamento": "CUNDINAMARCA"},
    "YUMBO": {"nombre_oficial": "YUMBO", "codigo_dane": "76892000", "departamento": "VALLE DEL CAUCA"},
    "TOCANCIPA": {"nombre_oficial": "TOCANCIPÁ", "codigo_dane": "25817000", "departamento": "CUNDINAMARCA"},
    "ZIPAQUIRA": {"nombre_oficial": "ZIPAQUIRÁ", "codigo_dane": "25899000", "departamento": "CUNDINAMARCA"},
    "GIRON": {"nombre_oficial": "GIRÓN", "codigo_dane": "68307000", "departamento": "SANTANDER"},
    "ESPINAL": {"nombre_oficial": "ESPINAL", "codigo_dane": "73319000", "departamento": "TOLIMA"},
    "ENVIGADO": {"nombre_oficial": "ENVIGADO", "codigo_dane": "05266000", "departamento": "ANTIOQUIA"},
    "ITAGUI": {"nombre_oficial": "ITAGÜÍ", "codigo_dane": "05360000", "departamento": "ANTIOQUIA"},
    "ARAUCA": {"nombre_oficial": "ARAUCA", "codigo_dane": "81001000", "departamento": "ARAUCA"},
    "CARTAGO": {"nombre_oficial": "CARTAGO", "codigo_dane": "76147000", "departamento": "VALLE DEL CAUCA"},
    "CALARCA": {"nombre_oficial": "CALARCÁ", "codigo_dane": "63130000", "departamento": "QUINDÍO"},
    "LA TEBAIDA": {"nombre_oficial": "LA TEBAIDA", "codigo_dane": "63401000", "departamento": "QUINDÍO"},
    "SOGAMOSO": {"nombre_oficial": "SOGAMOSO", "codigo_dane": "15759000", "departamento": "BOYACÁ"},
}

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

BODY_TYPE_GROUPS = {
    "body_general": {
        "title": "General",
        "button_title": "General",
        "options": [
            "General - Estacas",
            "General - Furgon",
            "General - Estibas",
            "General - Plataforma",
        ],
    },
    "body_especial": {
        "title": "Especial",
        "button_title": "Especiales",
        "options": [
            "Portacontenedores",
            "Furgon Refrigerado",
            "Granel Liquido - Tanque",
        ],
    },
    "body_granel": {
        "title": "Granel",
        "button_title": "Granel",
        "options": [
            "Granel Solido - Estacas",
            "Granel Solido - Furgon",
            "Granel Solido - Volco",
            "Granel Solido - Estibas",
            "Granel Solido - Plataforma",
        ],
    },
}

CARROCERIA_ALIASES = {
    "GENERAL": "General - Estacas",
    "GENERAL ESTACAS": "General - Estacas",
    "GENERAL - ESTACAS": "General - Estacas",
    "ESTACAS": "General - Estacas",
    "GENERAL ESTIBA": "General - Estacas",
    "GENERAL - ESTIBA": "General - Estacas",
    "GENERAL ESTIBAS CORTA": "General - Estacas",
    "ESTIBA": "General - Estibas",
    "ESTIBAS": "General - Estibas",
    "GENERAL ESTIBAS": "General - Estibas",
    "GENERAL - ESTIBAS": "General - Estibas",
    "FURGON": "General - Furgon",
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
    "FRIO": "Furgon Refrigerado",
    "FRIGORIFICO": "Furgon Refrigerado",
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
    "TANQUE": "Granel Liquido - Tanque",
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

INTENT_PATTERNS = [
    "quiero que me traigas el valor del flete de",
    "quiero que me traigas el valor del flete",
    "quiero saber el valor a pagar de",
    "quiero saber el valor del viaje de",
    "quiero saber el valor del viaje",
    "quiero saber el costo de la ruta",
    "quiero saber el costo de",
    "quiero saber el precio de",
    "quiero saber el precio",
    "calcula el valor de la ruta",
    "calcula el valor del flete de",
    "calcula el valor del flete",
    "trae el valor del flete de",
    "trae del valor del flete de",
    "trae el valor del flete",
    "cual es el valor a pagar de",
    "cual es el valor del viaje de",
    "cual es el valor del viaje",
    "cual es el costo de la ruta",
    "cual es el costo de",
    "cuentame el costo de",
    "genera el costo de",
    "dime el costo de la ruta",
    "dime el costo de",
    "dime el valor de",
    "precio de",
    "valor de",
    "costo de",
    "costo",
    "precio",
    "valor",
    "necesito",
]

LEAD_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
LEAD_COMPANY_RE = re.compile(
    r"(?:empresa|compañ[ií]a|compania|transportadora|soy de|trabajo en)\s*[:\-]?\s*([A-Za-z0-9ÁÉÍÓÚÑáéíóúñ .,&-]{3,80})",
    re.IGNORECASE,
)

# Estado liviano por teléfono. Es efímero, pero mejora la conversación
# sin agregar una dependencia obligatoria de persistencia.
CONVERSATION_STATE: dict[str, dict] = {}
MUNICIPIOS_CACHE: dict[str, object] = {
    "loaded_at": None,
    "aliases": {},
    "ordered_aliases": [],
}
VEHICULOS_CACHE: dict[str, object] = {
    "loaded_at": None,
    "aliases": {},
    "details": {},
}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


def normalizar_texto_libre(valor: str | None) -> str:
    texto = quitar_tildes(str(valor or "")).upper()
    texto = re.sub(r"[^A-Z0-9\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def municipio_alias_priority(alias_original: str | None, nombre_oficial: str | None) -> tuple[int, int]:
    alias_norm = normalizar_texto_libre(alias_original)
    oficial_norm = normalizar_texto_libre(nombre_oficial)
    score = 0
    if alias_norm == oficial_norm:
        score += 100
    if " " not in oficial_norm:
        score += 10
    return (score, -len(oficial_norm))


def safe_title(texto: str, limit: int = 24) -> str:
    limpio = re.sub(r"\s+", " ", str(texto or "")).strip()
    if len(limpio) <= limit:
        return limpio
    return f"{limpio[: limit - 3].rstrip()}..."


def safe_description(texto: str | None, limit: int = 72) -> str | None:
    limpio = re.sub(r"\s+", " ", str(texto or "")).strip()
    if not limpio:
        return None
    if len(limpio) <= limit:
        return limpio
    return f"{limpio[: limit - 3].rstrip()}..."


def strip_intent_prefixes(texto: str) -> tuple[str, str | None]:
    cleaned = re.sub(r"\s+", " ", (texto or "").strip())
    lowered = quitar_tildes(cleaned).lower()
    for pattern in sorted(INTENT_PATTERNS, key=len, reverse=True):
        if lowered.startswith(pattern):
            remainder = cleaned[len(pattern):].strip(" ,.:;-")
            return remainder or cleaned, pattern
    return cleaned, None


def get_municipios_endpoint() -> str:
    return f"{SICETAC_API_BASE}/municipios"


def get_vehiculos_endpoint() -> str:
    return f"{SICETAC_API_BASE}/opciones/vehiculos"


def ensure_municipios_cache() -> None:
    loaded_at = MUNICIPIOS_CACHE.get("loaded_at")
    if isinstance(loaded_at, datetime):
        age = (utcnow() - loaded_at).total_seconds()
        if age < MUNICIPIOS_CACHE_TTL_SECONDS:
            return

    try:
        resp = requests.get(get_municipios_endpoint(), timeout=REQUEST_TIMEOUT)
        if resp.status_code >= 400:
            logger.warning(f"Municipios cache fetch failed: status={resp.status_code}")
            MUNICIPIOS_CACHE["loaded_at"] = utcnow()
            return
        data = resp.json()
        municipios = data.get("municipios") or []
        aliases: dict[str, dict] = {}
        for item in municipios:
            codigo = str(item.get("codigo_dane") or "").strip() or None
            nombre_oficial = str(item.get("nombre_oficial") or "").strip()
            departamento = str(item.get("departamento") or "").strip() or None
            posibles = [
                nombre_oficial,
                item.get("variacion_1"),
                item.get("variacion_2"),
                item.get("variacion_3"),
            ]
            for posible in posibles:
                clave = normalizar_texto_libre(posible)
                if not clave:
                    continue
                candidate = {
                    "codigo_dane": codigo,
                    "nombre_oficial": nombre_oficial,
                    "departamento": departamento,
                    "_priority": municipio_alias_priority(posible, nombre_oficial),
                }
                current = aliases.get(clave)
                if not current or candidate["_priority"] > current.get("_priority", (0, 0)):
                    aliases[clave] = candidate
        for alias_text, info in MANUAL_MUNICIPIO_ALIASES.items():
            clave = normalizar_texto_libre(alias_text)
            if not clave:
                continue
            candidate = {
                "codigo_dane": info.get("codigo_dane"),
                "nombre_oficial": info.get("nombre_oficial"),
                "departamento": info.get("departamento"),
                "_priority": (120, -len(clave)),
            }
            current = aliases.get(clave)
            if not current or candidate["_priority"] > current.get("_priority", (0, 0)):
                aliases[clave] = candidate
        ordered_aliases = sorted(aliases.keys(), key=len, reverse=True)
        for value in aliases.values():
            value.pop("_priority", None)
        MUNICIPIOS_CACHE["aliases"] = aliases
        MUNICIPIOS_CACHE["ordered_aliases"] = ordered_aliases
        MUNICIPIOS_CACHE["loaded_at"] = utcnow()
    except Exception as e:
        logger.warning(f"Municipios cache unavailable: {e}")
        MUNICIPIOS_CACHE["loaded_at"] = utcnow()


def ensure_vehiculos_cache() -> None:
    loaded_at = VEHICULOS_CACHE.get("loaded_at")
    if isinstance(loaded_at, datetime):
        age = (utcnow() - loaded_at).total_seconds()
        if age < MUNICIPIOS_CACHE_TTL_SECONDS:
            return

    try:
        resp = requests.get(get_vehiculos_endpoint(), timeout=REQUEST_TIMEOUT)
        if resp.status_code >= 400:
            logger.warning(f"Vehiculos cache fetch failed: status={resp.status_code}")
            VEHICULOS_CACHE["loaded_at"] = utcnow()
            return
        data = resp.json()
        vehiculos = data.get("vehiculos") or []
        aliases: dict[str, str] = {}
        details: dict[str, dict] = {}
        for item in vehiculos:
            tipo = str(item.get("tipo_vehiculo") or "").strip().upper()
            analisis = str(item.get("configuracion_analisis") or "").strip().upper()
            detalle = str(item.get("detalle_tipo_vehiculo") or "").strip()
            if not tipo:
                continue
            aliases[tipo] = tipo
            aliases[tipo.replace("C", "", 1)] = tipo
            if analisis:
                aliases[analisis] = tipo
            details[tipo] = {
                "tipo_vehiculo": tipo,
                "configuracion_analisis": analisis or None,
                "detalle_tipo_vehiculo": detalle or None,
            }
        VEHICULOS_CACHE["aliases"] = aliases
        VEHICULOS_CACHE["details"] = details
        VEHICULOS_CACHE["loaded_at"] = utcnow()
    except Exception as e:
        logger.warning(f"Vehiculos cache unavailable: {e}")
        VEHICULOS_CACHE["loaded_at"] = utcnow()


def resolver_vehiculo_cache(texto: str | None) -> str | None:
    ensure_vehiculos_cache()
    aliases = VEHICULOS_CACHE.get("aliases") or {}
    clave = normalizar_texto_libre(texto)
    if not clave:
        return None
    return aliases.get(clave)


def get_vehicle_detail(vehiculo: str) -> dict | None:
    ensure_vehiculos_cache()
    details = VEHICULOS_CACHE.get("details") or {}
    return details.get((vehiculo or "").strip().upper())


def resolver_municipio_cache(texto: str | None) -> dict | None:
    ensure_municipios_cache()
    aliases = MUNICIPIOS_CACHE.get("aliases") or {}
    clave = normalizar_texto_libre(texto)
    if not clave:
        return None
    return aliases.get(clave)


def extraer_municipios_en_texto(texto: str) -> list[dict]:
    ensure_municipios_cache()
    aliases = MUNICIPIOS_CACHE.get("aliases") or {}
    ordered_aliases = MUNICIPIOS_CACHE.get("ordered_aliases") or []
    texto_normalizado = f" {normalizar_texto_libre(texto)} "
    encontrados: list[dict] = []
    spans_ocupados: list[tuple[int, int]] = []

    for alias in ordered_aliases:
        patron = f" {alias} "
        start = texto_normalizado.find(patron)
        if start == -1:
            continue
        start_content = start + 1
        end_content = start_content + len(alias)
        if any(not (end_content <= s or start_content >= e) for s, e in spans_ocupados):
            continue
        info = aliases.get(alias)
        if not info:
            continue
        encontrados.append(
            {
                "match": alias,
                "start": start_content,
                "end": end_content,
                "codigo_dane": info.get("codigo_dane"),
                "nombre_oficial": info.get("nombre_oficial"),
                "departamento": info.get("departamento"),
            }
        )
        spans_ocupados.append((start_content, end_content))

    encontrados.sort(key=lambda item: item["start"])
    unicos: list[dict] = []
    vistos: set[str] = set()
    for item in encontrados:
        clave = f"{item.get('codigo_dane')}|{item.get('nombre_oficial')}"
        if clave in vistos:
            continue
        vistos.add(clave)
        unicos.append(item)
    return unicos


def inferir_ruta_con_municipios(texto: str) -> dict | None:
    municipios = extraer_municipios_en_texto(texto)
    if len(municipios) < 2:
        return None
    origen_info = municipios[0]
    destino_info = municipios[1]
    return {
        "origen": normalizar_ciudad(origen_info.get("nombre_oficial") or ""),
        "destino": normalizar_ciudad(destino_info.get("nombre_oficial") or ""),
        "codigo_dane_origen": origen_info.get("codigo_dane"),
        "codigo_dane_destino": destino_info.get("codigo_dane"),
    }


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


ROUTE_PREFIX_RE = re.compile(
    r"^(?:"
    r"valor por tonelada|por tonelada|cuanto por tonelada|cuánto por tonelada|valor por ton|por ton|"
    r"hola(?:\s+atica)?|buenos días|buenos dias|buen día|buen dia|buenas tardes|buenas noches|"
    r"buena noche|buenas|buenasndias|consulta|consultar|ruta|flete ruta|flete|tarifa|"
    r"cuanto cuesta|cuánto cuesta|precio|calcular ruta|calcular|valor|costo|ahora|ok|"
    r"bueno|entonces|atica"
    r")\b[\s,.:;\-]*",
    re.IGNORECASE,
)


def limpiar_prefijos_ruta(texto: str) -> str:
    cleaned = (texto or "").strip()
    while cleaned:
        updated = ROUTE_PREFIX_RE.sub("", cleaned, count=1).strip()
        if updated == cleaned or not updated:
            return cleaned
        cleaned = updated
    return cleaned


def limpiar_linea_estructurada(linea: str) -> str:
    cleaned = re.sub(r"^[\-\*\u2022]+\s*", "", (linea or "").strip())
    cleaned = re.sub(
        r"^(?:ruta|origen|destino|tipo de vehiculo|tipo de vehículo|tipo de carga|carroceria|carrocería|"
        r"vehiculo|vehículo|configuracion|configuración|horas de cargue y descargue|horas logisticas|"
        r"horas logísticas)[:\s-]*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned.strip()


def parsear_ruta_por_lineas(texto: str) -> dict | None:
    lineas = [linea.strip() for linea in re.split(r"[\r\n]+", texto or "") if linea.strip()]
    if len(lineas) < 2:
        return None

    origen_linea = None
    destino_linea = None
    rutas_candidatas: list[str] = []
    lineas_municipio: list[dict] = []

    for linea in lineas:
        linea_limpia = limpiar_linea_estructurada(linea)
        if not linea_limpia:
            continue
        if re.match(r"^\s*ruta[:\s-]+", linea, re.IGNORECASE):
            rutas_candidatas.append(linea_limpia)
            continue
        if re.match(r"^\s*origen[:\s-]+", linea, re.IGNORECASE):
            origen_linea = linea_limpia
            continue
        if re.match(r"^\s*destino[:\s-]+", linea, re.IGNORECASE):
            destino_linea = linea_limpia
            continue
        if parsear_vehiculo(linea_limpia) or parsear_carroceria(linea_limpia):
            continue
        if parsear_horas_personalizadas(linea_limpia) or parsear_toneladas(linea_limpia):
            continue
        rutas_candidatas.append(linea_limpia)

    if origen_linea and destino_linea:
        origen_resuelto = resolver_municipio_cache(origen_linea)
        destino_resuelto = resolver_municipio_cache(destino_linea)
        if origen_resuelto and destino_resuelto:
            return {
                "origen": normalizar_ciudad(origen_resuelto.get("nombre_oficial") or origen_linea),
                "destino": normalizar_ciudad(destino_resuelto.get("nombre_oficial") or destino_linea),
                "codigo_dane_origen": origen_resuelto.get("codigo_dane"),
                "codigo_dane_destino": destino_resuelto.get("codigo_dane"),
            }

    for candidata in rutas_candidatas:
        ruta = parsear_ruta(candidata) if candidata != texto else None
        if ruta:
            return ruta
        municipio = resolver_municipio_cache(candidata)
        if municipio:
            lineas_municipio.append(municipio)

    if len(lineas_municipio) >= 2:
        return {
            "origen": normalizar_ciudad(lineas_municipio[0].get("nombre_oficial") or ""),
            "destino": normalizar_ciudad(lineas_municipio[1].get("nombre_oficial") or ""),
            "codigo_dane_origen": lineas_municipio[0].get("codigo_dane"),
            "codigo_dane_destino": lineas_municipio[1].get("codigo_dane"),
        }
    return None


def parsear_ruta(texto: str) -> dict | None:
    texto_base, _ = strip_intent_prefixes(texto)
    ruta_por_lineas = parsear_ruta_por_lineas(texto_base)
    if ruta_por_lineas:
        return ruta_por_lineas

    texto = limpiar_prefijos_ruta(texto_base).lower()

    patterns = [
        r"^(?:(?:de|desde)\s+)?(.+?)\s+a\s+(.+)$",
        r"^(?:(?:de|desde)\s+)?(.+?)\s+para\s+(.+)$",
        r"^(?:(?:de|desde)\s+)?(.+?)\s+hasta\s+(.+)$",
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
        origen_txt = " ".join(origen_raw.split())
        destino_txt = recortar_destino(destino_raw)
        origen_resuelto = resolver_municipio_cache(origen_txt)
        destino_resuelto = resolver_municipio_cache(destino_txt)
        origen = normalizar_ciudad((origen_resuelto or {}).get("nombre_oficial") or origen_txt)
        destino = normalizar_ciudad((destino_resuelto or {}).get("nombre_oficial") or destino_txt)
        if origen and destino:
            ruta = {"origen": origen, "destino": destino}
            if origen_resuelto:
                ruta["codigo_dane_origen"] = origen_resuelto.get("codigo_dane")
            if destino_resuelto:
                ruta["codigo_dane_destino"] = destino_resuelto.get("codigo_dane")
            return ruta
    return inferir_ruta_con_municipios(texto)


def analizar_texto_busqueda(texto: str) -> dict:
    cleaned_text, matched_intent = strip_intent_prefixes(texto)
    ruta = parsear_ruta(cleaned_text)
    municipios = extraer_municipios_en_texto(cleaned_text)
    return {
        "original_text": texto,
        "cleaned_text": cleaned_text,
        "matched_intent_pattern": matched_intent,
        "municipios_detected": [
            {
                "codigo_dane": item.get("codigo_dane"),
                "nombre_oficial": item.get("nombre_oficial"),
                "departamento": item.get("departamento"),
            }
            for item in municipios[:4]
        ],
        "route_found": bool(ruta),
        "route": ruta,
    }


def parsear_vehiculo(texto: str) -> str | None:
    ensure_vehiculos_cache()
    texto_normalizado = normalizar_texto_libre(texto)
    aliases = VEHICULOS_CACHE.get("aliases") or {}
    for alias in sorted(aliases.keys(), key=len, reverse=True):
        patron = f" {alias} "
        if patron in f" {texto_normalizado} ":
            return aliases.get(alias)
    texto_upper = texto.upper()
    for vehiculo in sorted(VEHICULOS_VALIDOS, key=len, reverse=True):
        if vehiculo in texto_upper:
            return vehiculo
        if vehiculo.replace("C", "", 1) in texto_upper:
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
        vehiculo = parsear_vehiculo(texto)
        if vehiculo:
            return vehiculo
    return None


def mensaje_configuracion_vehiculo(vehiculo: str) -> str:
    detalle = get_vehicle_detail(vehiculo) or {}
    descripcion = (
        detalle.get("detalle_tipo_vehiculo")
        or VEHICULO_DESCRIPCIONES.get(vehiculo)
        or "configuración vehicular SICETAC"
    )
    configuracion_analisis = detalle.get("configuracion_analisis")
    lineas = [f"{vehiculo} corresponde a {quitar_tildes(descripcion)}."]
    if configuracion_analisis:
        lineas.append(f"En analisis o valor de plaza tambien puede aparecer como {quitar_tildes(configuracion_analisis)}.")
    lineas.append("")
    lineas.append(f"Si quieres, te calculo una ruta con esa configuracion. Escribeme por ejemplo: Bogota a Barranquilla {vehiculo}")
    return "\n".join(lineas)


SALUDO_EXACTO_NORMALIZADO = {
    "HOLA",
    "HI",
    "HELLO",
    "HOLA ATICA",
    "HOLA AMIGO",
    "HOLA COMO ESTAS",
    "HOLA CÓMO ESTÁS",
    "BUEN DIA",
    "BUEN DÍA",
    "BUENOS DIAS",
    "BUENOS DÍAS",
    "BUENAS TARDES",
    "BUENAS NOCHES",
    "BUENA NOCHE",
    "BUENAS",
    "BUENASNDIAS",
    "COMO VAS",
    "COMO VA",
    "CÓMO VAS",
    "CÓMO VA",
    "QUE TAL",
    "QUÉ TAL",
    "ESTAS",
    "ESTAS ?",
    "ESTÁS",
    "ESTÁS ?",
    "MENU",
    "MENÚ",
    "AYUDA",
    "HELP",
    "INICIO",
    "START",
    "?",
}


def es_saludo_o_ayuda_simple(texto: str) -> bool:
    texto_normalizado = normalizar_texto_libre(texto)
    if not texto_normalizado:
        return False
    if texto_normalizado in SALUDO_EXACTO_NORMALIZADO:
        return True
    palabras = texto_normalizado.split()
    if len(palabras) > 5:
        return False
    if any(token.isdigit() for token in palabras):
        return False
    pistas = ["HOLA", "BUEN", "BUENOS", "BUENAS", "COMO", "CÓMO", "QUE TAL", "QUÉ TAL", "ESTAS", "ESTÁS"]
    return any(pista in texto_normalizado for pista in pistas)


def es_saludo_simple(texto: str) -> bool:
    return es_saludo_o_ayuda_simple(texto)


def construir_respuesta_ruta_faltante(user_text: str, analisis_busqueda: dict, state: dict) -> str:
    municipios = analisis_busqueda.get("municipios_detected") or []
    vehiculo = parsear_vehiculo(user_text)
    carroceria = parsear_carroceria(user_text)

    if es_saludo_simple(user_text):
        return "Escribeme la ruta asi: Bogota a Barranquilla."

    if vehiculo or carroceria:
        if state.get("last_route"):
            return "Ya tengo ese cambio. Si quieres otra ruta, escribela asi: origen a destino."
        return "Ya tengo esa configuracion. Ahora escribeme la ruta: origen a destino."

    if len(municipios) == 1:
        ciudad = quitar_tildes(municipios[0].get("nombre_oficial") or "")
        return f"Ya tengo {ciudad}. Ahora escribeme el otro punto asi: origen a destino."

    texto_normalizado = normalizar_texto_libre(user_text)
    if any(palabra in texto_normalizado for palabra in ["EXCEL", "PROMEDIO", "AUMENTO", "MERCADO", "PLAZA"]):
        return "Hoy te ayudo con rutas SICETAC. Escribeme una asi: Bogota a Barranquilla."

    return "Te ayudo. Escribeme la ruta en una sola linea: origen a destino."


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
            lineas.append("Si quieres ver mas opciones, escribe: opciones.")
            lineas.append("Si quieres cambiar configuracion, escribe: cambiar configuracion.")
        return "\n".join(lineas)
    except Exception as e:
        logger.error(f"Error formateando: {e}")
        return "Error al formatear la respuesta. Intenta de nuevo."


def formatear_valor_plaza(data: dict) -> str | None:
    plaza = data.get("valor_plaza") or {}
    meses = plaza.get("meses") or []
    if not meses:
        return None

    tipo_carga = quitar_tildes(plaza.get("tipo_carga_label") or "Carga normal")
    promedio = plaza.get("promedio_ultimos_meses")
    lineas = [f"Valor en plaza RNDC ultimos {len(meses)} meses ({tipo_carga}):"]
    for item in meses:
        mes_label = item.get("mes_label") or item.get("mes_codigo") or "Mes"
        valor = item.get("valor")
        linea = f"- {mes_label}: {fmt_cop(valor)}"
        lineas.append(linea)
    if promedio is not None:
        lineas.append(f"Promedio: {fmt_cop(promedio)}")
    if plaza.get("fallback_to_carga_normal"):
        lineas.append("Nota: para esta carroceria no habia valor especifico y use carga normal.")
    lineas.append("Fuente: Calculos Atica - Atiemppo.")
    return "\n".join(lineas)


def mensaje_ayuda() -> str:
    return (
        "Hola Soy Atica de Atiemppo ahora en Whatsapp!, estoy aca para proporcionarte la informacion de SICETAC al instante y en tu telefono.\n\n"
        "Escribe la ruta directo asi: origen a destino.\n"
        "Si quieres ver configuraciones y carrocerias disponibles, escribe: opciones.\n"
        "Si quieres cambiar configuracion o carroceria, escribe: cambiar configuracion.\n"
        "Tambien puedes escribir configuraciones sin la C, por ejemplo 3S3 o 2S2.\n"
        f"Si no indicas configuracion o carroceria, uso {DEFAULT_VEHICULO} y {quitar_tildes(DEFAULT_CARROCERIA)}.\n\n"
        "Ejemplos:\n"
        "- Bogota a Barranquilla\n"
        "- Medellin a Cartagena C2M10\n"
        "- Cali a Buenaventura portacontenedores\n"
        "- Bogota a Barranquilla C3S3 furgon refrigerado\n"
        "- Cartagena a Bogota C2S2 estacas\n"
        "- Para cambiar una ruta ya calculada: escribe cambiar configuracion y elige C2S2\n\n"
        "Escribe la ruta que quieres que analicemos hoy."
    )


def mensaje_opciones() -> str:
    return (
        "Configuraciones: C278, C289, C2910, C2M10, C3, C2S2, C2S3, C3S2, C3S3 y V3.\n\n"
        "Tambien puedes escribirlas sin la C cuando aplique: 2S2, 2S3, 3S2, 3S3.\n\n"
        "Carrocerias:\n"
        "- General - Estacas\n"
        "- General - Furgon\n"
        "- General - Estibas\n"
        "- General - Plataforma\n"
        "- Portacontenedores\n"
        "- Furgon Refrigerado\n"
        "- Granel Solido - Estacas\n"
        "- Granel Solido - Furgon\n"
        "- Granel Solido - Volco\n"
        "- Granel Solido - Estibas\n"
        "- Granel Solido - Plataforma\n"
        "- Granel Liquido - Tanque\n\n"
        f"Si no indicas una, uso {DEFAULT_VEHICULO} y {quitar_tildes(DEFAULT_CARROCERIA)}.\n\n"
        "Ejemplos:\n"
        "- Bogota a Barranquilla\n"
        "- Medellin a Cartagena C2M10\n"
        "- Cali a Buenaventura portacontenedores\n"
        "- Bogota a Barranquilla C3S3 furgon refrigerado\n"
        "- Cartagena a Bogota C2S2 estacas\n"
        "- Si ya calculaste una ruta y quieres cambiar vehiculo o carroceria, escribe: cambiar configuracion\n\n"
        "Tambien puedes escribir ayuda o cambiar configuracion."
    )


def usuario_quiere_cambiar_configuracion(texto: str) -> bool:
    texto_normalizado = normalizar_lookup_texto(texto)
    return (
        "CAMBIAR CONFIGURACION" in texto_normalizado
        or "CAMBIAR CONFIGURACION" in texto_normalizado
        or "CAMBIAR VEHICULO" in texto_normalizado
        or "CAMBIAR CARROCERIA" in texto_normalizado
        or "CAMBIAR CARROCERIA" in texto_normalizado
        or texto_normalizado.strip() in {"CONFIGURACION", "CONFIGURACIÓN", "CARROCERIA", "CARROCERÍA"}
    )


def mensaje_configuracion_guardada(vehiculo: str | None = None, carroceria: str | None = None) -> str:
    partes = []
    if vehiculo:
        partes.append(f"vehiculo {vehiculo}")
    if carroceria:
        partes.append(f"carroceria {quitar_tildes(carroceria)}")
    detalle = " y ".join(partes) if partes else "la configuracion"
    return (
        f"Listo. Guardare {detalle} como preferida en esta conversacion.\n\n"
        "Ahora escribe la ruta asi: origen a destino."
    )


def mensaje_menu_configuracion() -> str:
    return (
        "Que quieres ajustar?\n\n"
        f"Configuracion actual: {DEFAULT_VEHICULO} y {quitar_tildes(DEFAULT_CARROCERIA)} si no has elegido otra en esta conversacion."
    )


def mensaje_seleccion_carroceria() -> str:
    return (
        "Elige el grupo de carroceria.\n\n"
        "Luego te muestro las opciones y la guardo para las siguientes rutas de esta conversacion."
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
            "preferred_vehicle": None,
            "preferred_body_type": None,
            "pending_selection": None,
        },
    )


def get_preferred_vehicle(state: dict) -> str:
    return (state.get("preferred_vehicle") or DEFAULT_VEHICULO).strip()


def get_preferred_body_type(state: dict) -> str:
    return (state.get("preferred_body_type") or DEFAULT_CARROCERIA).strip()


def set_preferred_vehicle(state: dict, vehiculo: str | None):
    if vehiculo:
        state["preferred_vehicle"] = vehiculo.strip().upper()


def set_preferred_body_type(state: dict, carroceria: str | None):
    if carroceria:
        state["preferred_body_type"] = carroceria.strip()


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
    codigo_dane_origen: str | None = None,
    codigo_dane_destino: str | None = None,
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
    if codigo_dane_origen:
        payload["codigo_dane_origen"] = codigo_dane_origen
    if codigo_dane_destino:
        payload["codigo_dane_destino"] = codigo_dane_destino
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
    if not OPENAI_API_KEY or not OPENAI_FALLBACK_ENABLED:
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

    requiere_contexto_anterior = (
        usuario_pide_valor_por_tonelada(user_text)
        or usuario_pide_otra_hora(user_text)
        or bool(parsear_vehiculo(user_text))
        or bool(parsear_carroceria(user_text))
    )
    if requiere_contexto_anterior and state.get("last_route"):
        last_route = state["last_route"]
        return {
            "origen": last_route.get("origen"),
            "destino": last_route.get("destino"),
        }, False
    return None, False


def extract_incoming_message(message: dict) -> tuple[str | None, str]:
    message_type = (message.get("type") or "").strip()
    if message_type == "text":
        body = ((message.get("text") or {}).get("body") or "").strip()
        return body or None, "text"
    if message_type == "interactive":
        interactive = message.get("interactive") or {}
        interactive_type = (interactive.get("type") or "").strip()
        if interactive_type == "button_reply":
            reply = interactive.get("button_reply") or {}
            body = (reply.get("id") or reply.get("title") or "").strip()
            return body or None, "button_reply"
        if interactive_type == "list_reply":
            reply = interactive.get("list_reply") or {}
            body = (reply.get("id") or reply.get("title") or "").strip()
            return body or None, "list_reply"
    return None, message_type or "unknown"


def send_whatsapp_payload(to: str, payload: dict):
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_ID:
        logger.warning("Missing WhatsApp credentials — skipping send")
        return

    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    full_payload = {
        "messaging_product": "whatsapp",
        "to": to,
        **payload,
    }

    try:
        resp = requests.post(url, headers=headers, json=full_payload, timeout=30)
        logger.info(f"WA send [{to}]: status={resp.status_code}, type={payload.get('type')}")
        if resp.status_code != 200:
            logger.error(f"WA error: {resp.text}")
    except Exception as e:
        logger.error(f"WA send error: {e}")


def send_whatsapp_message(to: str, body: str):
    send_whatsapp_payload(
        to,
        {
            "type": "text",
            "text": {
                "preview_url": False,
                "body": body[:4096],
            },
        },
    )


def send_whatsapp_buttons(to: str, body: str, buttons: list[dict], footer: str | None = None):
    action_buttons = []
    for button in buttons[:3]:
        action_buttons.append(
            {
                "type": "reply",
                "reply": {
                    "id": str(button.get("id") or "")[:256],
                    "title": safe_title(button.get("title") or "", limit=20),
                },
            }
        )
    if not action_buttons:
        return

    interactive = {
        "type": "button",
        "body": {"text": body[:1024]},
        "action": {"buttons": action_buttons},
    }
    if footer:
        interactive["footer"] = {"text": footer[:60]}

    send_whatsapp_payload(
        to,
        {
            "type": "interactive",
            "interactive": interactive,
        },
    )


def send_whatsapp_list(to: str, body: str, button_text: str, sections: list[dict], footer: str | None = None):
    interactive = {
        "type": "list",
        "body": {"text": body[:1024]},
        "action": {
            "button": safe_title(button_text, limit=20),
            "sections": sections,
        },
    }
    if footer:
        interactive["footer"] = {"text": footer[:60]}

    send_whatsapp_payload(
        to,
        {
            "type": "interactive",
            "interactive": interactive,
        },
    )


def build_vehicle_rows() -> list[dict]:
    rows = []
    for vehiculo in VEHICULOS_VALIDOS:
        detalle = get_vehicle_detail(vehiculo) or {}
        descripcion = detalle.get("detalle_tipo_vehiculo") or VEHICULO_DESCRIPCIONES.get(vehiculo) or ""
        row = {
            "id": f"vehicle:{vehiculo}",
            "title": vehiculo,
        }
        descripcion_corta = safe_description(quitar_tildes(descripcion))
        if descripcion_corta:
            row["description"] = descripcion_corta
        rows.append(row)
    return rows


def build_body_rows(group_key: str) -> list[dict]:
    group = BODY_TYPE_GROUPS.get(group_key) or {}
    rows = []
    for option in group.get("options") or []:
        alias = {
            "General - Estacas": "Alias: estacas o general",
            "General - Furgon": "Alias: furgon",
            "General - Estibas": "Alias: estibas",
            "General - Plataforma": "Alias: plataforma",
            "Portacontenedores": "Alias: portacontenedores",
            "Furgon Refrigerado": "Alias: frio o refrigerado",
            "Granel Solido - Volco": "Alias: volco",
            "Granel Liquido - Tanque": "Alias: tanque",
        }.get(option)
        row = {
            "id": f"body:{option}",
            "title": safe_title(option, limit=24),
        }
        alias_corto = safe_description(alias)
        if alias_corto:
            row["description"] = alias_corto
        rows.append(row)
    return rows


def send_configuration_menu(to: str):
    send_whatsapp_buttons(
        to=to,
        body=mensaje_menu_configuracion(),
        buttons=[
            {"id": "config:vehicle_menu", "title": "Vehiculo"},
            {"id": "config:body_menu", "title": "Carroceria"},
            {"id": "config:options_text", "title": "Ver opciones"},
        ],
        footer="ATICA",
    )


def send_vehicle_selector(to: str):
    send_whatsapp_list(
        to=to,
        body="Elige tu configuracion preferida. La guardo para esta conversacion y luego me escribes la ruta.",
        button_text="Elegir vehiculo",
        sections=[
            {
                "title": "Configuraciones",
                "rows": build_vehicle_rows(),
            }
        ],
        footer="Tambien puedes escribirla directo",
    )


def send_body_group_selector(to: str):
    send_whatsapp_buttons(
        to=to,
        body=mensaje_seleccion_carroceria(),
        buttons=[
            {"id": "body_group:body_general", "title": "General"},
            {"id": "body_group:body_especial", "title": "Especiales"},
            {"id": "body_group:body_granel", "title": "Granel"},
        ],
        footer="ATICA",
    )


def send_body_selector(to: str, group_key: str):
    group = BODY_TYPE_GROUPS.get(group_key)
    if not group:
        send_whatsapp_message(to=to, body=mensaje_opciones())
        return
    send_whatsapp_list(
        to=to,
        body=f"Elige la carroceria del grupo {group.get('title', 'seleccionado')}.",
        button_text="Elegir carroceria",
        sections=[
            {
                "title": group.get("title", "Carrocerias"),
                "rows": build_body_rows(group_key),
            }
        ],
        footer="Luego escribe la ruta",
    )


@app.get("/")
async def health():
    return {
        "service": "atica-whatsapp-bridge",
        "version": "3.3.0",
        "status": "running",
        "sicetac_api": SICETAC_API_BASE,
        "openai_enabled": bool(OPENAI_API_KEY and OPENAI_FALLBACK_ENABLED),
        "openai_configured": bool(OPENAI_API_KEY),
        "openai_fallback_enabled": OPENAI_FALLBACK_ENABLED,
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
        incoming_text, incoming_kind = extract_incoming_message(message)

        if not incoming_text:
            send_whatsapp_message(
                to=from_number,
                body="Por ahora proceso texto, botones y listas. Escribe una ruta como: Bogota a Barranquilla",
            )
            return {"status": "non-text"}

        user_text = incoming_text.strip()
        merge_lead_data(state, profile_name, user_text)
        logger.info(f"MSG [{from_number}] ({incoming_kind}): {user_text}")
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return {"status": "parse error", "detail": str(e)}

    if user_text.startswith("config:"):
        if user_text == "config:vehicle_menu":
            state["pending_selection"] = "vehicle"
            send_vehicle_selector(from_number)
            return {"status": "vehicle selector sent"}
        if user_text == "config:body_menu":
            state["pending_selection"] = "body_group"
            send_body_group_selector(from_number)
            return {"status": "body group selector sent"}
        if user_text == "config:options_text":
            send_whatsapp_message(to=from_number, body=mensaje_opciones())
            return {"status": "options sent"}

    if user_text.startswith("body_group:"):
        group_key = user_text.split(":", 1)[1]
        state["pending_selection"] = f"body:{group_key}"
        send_body_selector(from_number, group_key)
        return {"status": "body selector sent"}

    if user_text.startswith("vehicle:"):
        vehiculo_elegido = user_text.split(":", 1)[1].strip().upper()
        if vehiculo_elegido in VEHICULOS_VALIDOS:
            set_preferred_vehicle(state, vehiculo_elegido)
            state["pending_selection"] = None
            send_whatsapp_message(to=from_number, body=mensaje_configuracion_guardada(vehiculo=vehiculo_elegido))
            capture_lead_event(
                {
                    "event": "preferred_vehicle_updated",
                    "ts": utcnow_iso(),
                    "channel": "whatsapp",
                    "lead": state["lead"],
                    "selection": {"preferred_vehicle": vehiculo_elegido},
                }
            )
            return {"status": "preferred vehicle updated"}

    if user_text.startswith("body:"):
        carroceria_elegida = user_text.split(":", 1)[1].strip()
        carroceria_normalizada = normalizar_carroceria(carroceria_elegida)
        if carroceria_normalizada in CARROCERIAS_VALIDAS:
            set_preferred_body_type(state, carroceria_normalizada)
            state["pending_selection"] = None
            send_whatsapp_message(
                to=from_number,
                body=mensaje_configuracion_guardada(carroceria=carroceria_normalizada),
            )
            capture_lead_event(
                {
                    "event": "preferred_body_type_updated",
                    "ts": utcnow_iso(),
                    "channel": "whatsapp",
                    "lead": state["lead"],
                    "selection": {"preferred_body_type": carroceria_normalizada},
                }
            )
            return {"status": "preferred body updated"}

    texto_lower = user_text.lower().strip()
    if es_saludo_o_ayuda_simple(user_text) and not parsear_ruta(user_text):
        send_whatsapp_message(to=from_number, body=mensaje_ayuda())
        if any(token in texto_lower for token in ("ayuda", "help", "menu", "menú")):
            send_configuration_menu(from_number)
        capture_lead_event(
            {
                "event": "help_requested",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
            }
        )
        return {"status": "help sent"}

    if texto_lower in ("opciones", "configuraciones", "vehiculos", "vehículos", "carrocerias", "carrocerias", "menu opciones") or usuario_quiere_cambiar_configuracion(user_text):
        if usuario_quiere_cambiar_configuracion(user_text):
            send_configuration_menu(from_number)
        else:
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

    analisis_busqueda = analizar_texto_busqueda(user_text)
    ruta, ruta_en_mensaje_actual = resolver_contexto_consulta(analisis_busqueda.get("cleaned_text") or user_text, state)
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
        fallback_reply = construir_respuesta_ruta_faltante(user_text, analisis_busqueda, state)
        send_whatsapp_message(to=from_number, body=fallback_reply)
        capture_lead_event(
            {
                "event": "message_without_route",
                "ts": utcnow_iso(),
                "channel": "whatsapp",
                "lead": state["lead"],
                "message": user_text,
                "parse": analisis_busqueda,
            }
        )
        return {"status": "no route parsed"}

    vehiculo_detectado = parsear_vehiculo(user_text)
    carroceria_detectada = parsear_carroceria(user_text)
    if ruta_en_mensaje_actual:
        vehiculo = vehiculo_detectado or get_preferred_vehicle(state)
        carroceria = carroceria_detectada or get_preferred_body_type(state)
    else:
        vehiculo = vehiculo_detectado or (state.get("last_route") or {}).get("vehiculo") or get_preferred_vehicle(state)
        carroceria = carroceria_detectada or (state.get("last_route") or {}).get("carroceria") or get_preferred_body_type(state)
    modo_viaje = parsear_modo_viaje(user_text)
    horas_personalizadas = parsear_horas_personalizadas(user_text)
    toneladas_explicitas = parsear_toneladas(user_text)
    pide_valor_ton = usuario_pide_valor_por_tonelada(user_text)
    pide_horas = usuario_pide_otra_hora(user_text)
    uso_default_vehiculo = vehiculo_detectado is None and (
        ruta_en_mensaje_actual and not state.get("preferred_vehicle")
    )
    uso_default_carroceria = carroceria_detectada is None and (
        ruta_en_mensaje_actual and not state.get("preferred_body_type")
    )

    if vehiculo_detectado:
        set_preferred_vehicle(state, vehiculo_detectado)
    if carroceria_detectada:
        set_preferred_body_type(state, carroceria_detectada)

    resultado = consultar_sicetac(
        origen=ruta["origen"],
        destino=ruta["destino"],
        vehiculo=vehiculo,
        carroceria=carroceria,
        modo_viaje=modo_viaje,
        codigo_dane_origen=ruta.get("codigo_dane_origen"),
        codigo_dane_destino=ruta.get("codigo_dane_destino"),
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
    query_kind = "route_summary"
    total_referencia = extraer_totales(resultado).get("H8")
    total_bucket = "H8" if total_referencia is not None else None
    if pide_horas and horas_personalizadas is not None:
        respuesta = formatear_valor_personalizado_por_horas(
            resultado=resultado,
            horas=horas_personalizadas,
            vehiculo=vehiculo,
            toneladas=toneladas_explicitas,
            incluir_por_tonelada=pide_valor_ton,
        )
        query_kind = "custom_hours"
        total_referencia = calcular_total_para_horas(resultado, horas_personalizadas)
        total_bucket = f"H{fmt_decimal(horas_personalizadas) or horas_personalizadas}"
    elif pide_valor_ton:
        respuesta = formatear_valor_por_tonelada(
            resultado=resultado,
            vehiculo=vehiculo,
            horas=horas_personalizadas if pide_horas else None,
            toneladas=toneladas_explicitas,
        )
        query_kind = "value_per_ton"
        if horas_personalizadas is not None:
            total_referencia = calcular_total_para_horas(resultado, horas_personalizadas)
            total_bucket = f"H{fmt_decimal(horas_personalizadas) or horas_personalizadas}"
    elif not ruta_en_mensaje_actual and (pide_horas or pide_valor_ton):
        respuesta = respuesta_deterministica
    send_whatsapp_message(to=from_number, body=respuesta)

    mensaje_plaza = None
    if ruta_en_mensaje_actual and query_kind == "route_summary":
        mensaje_plaza = formatear_valor_plaza(resultado)
        if mensaje_plaza:
            send_whatsapp_message(to=from_number, body=mensaje_plaza)

    capture_lead_event(
        {
            "event": "route_consulted",
            "ts": utcnow_iso(),
            "channel": "whatsapp",
            "lead": state["lead"],
            "route": state["last_route"],
            "sicetac": build_sicetac_snapshot(resultado),
            "message": user_text,
            "parse": analisis_busqueda,
            "query": {
                "kind": query_kind,
                "requested_hours": horas_personalizadas,
                "requested_tons": toneladas_explicitas,
                "value_per_ton_requested": pide_valor_ton,
                "used_last_route_context": not ruta_en_mensaje_actual,
                "used_default_vehicle": uso_default_vehiculo,
                "used_default_body_type": uso_default_carroceria,
                "sicetac_reference_total": total_referencia,
                "sicetac_reference_bucket": total_bucket,
                "market_plaza_attached": bool(mensaje_plaza),
            },
        }
    )

    logger.info(f"OK [{from_number}]: {ruta['origen']} -> {ruta['destino']}")
    return {"status": "ok"}

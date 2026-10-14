"""Private tariff addon. Provider identity is verified by the portal, never by @ text."""
import base64
from contextvars import ContextVar
import logging
import os
import re
import requests
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

logger = logging.getLogger("atica-enterprise")
_proof = ContextVar("sicetac_webhook_proof", default=None)

def set_proof(raw, signature):
    return _proof.set({"webhook":base64.b64encode(raw).decode("ascii"),"signature":signature})

def reset_proof(token):
    _proof.reset(token)

def current_proof():
    return _proof.get()

def link_command_message(*, text, proof):
    """A linking command never reaches route parsing, contact capture or LLMs."""
    if not re.match(r"^VINCULAR(?:\s|$)", text.strip(), re.IGNORECASE):
        return None
    if not re.fullmatch(r"VINCULAR[ \t]+[A-Fa-f0-9]{32}", text.strip(), re.IGNORECASE):
        return "Genera tu código en el portal: Mi empresa → Usuarios, o Preferencias → Conectar mi WhatsApp. Copia y envía el mensaje completo."
    url=os.environ.get("SICETAC_ENTERPRISE_URL", "").strip().rstrip("/")
    secret=os.environ.get("CAPTURE_WEBHOOK_SECRET", "").strip()
    if not url or not secret or not proof:
        return "No se pudo verificar la vinculación. Conserva tu código e inténtalo más tarde."
    try:
        response=requests.post(url+"/vincular",headers={"x-capture-secret":secret,"Content-Type":"application/json"},json=proof,timeout=12)
        data=response.json()
        if response.status_code==200 and data.get("linked"):
            return f"WhatsApp vinculado a {data['company']}. Ya puedes consultar las rutas y tarifas autorizadas de tu empresa. Escribe, por ejemplo: Bogotá a Cali."
        if response.status_code in (403,409,410):
            return data.get("error") or "No se pudo vincular. Revisa tu acceso o genera un nuevo código en el portal."
        logger.warning("WhatsApp linking unavailable: status=%s",response.status_code)
    except (requests.RequestException,ValueError,KeyError,TypeError):
        logger.warning("WhatsApp linking unavailable")
    return "No se pudo completar la vinculación. Conserva tu código e inténtalo más tarde."

def _tonnage(value):
    if isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
        return number if number.is_finite() and number > 0 else None
    except (InvalidOperation, ValueError):
        return None


def _money(value):
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("Invalid monetary value")
    return "$ " + f"{number.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def format_company_values(data, result, toneladas=None):
    profile = data.get("business_profile") or data["company"].get("business_profile")
    allowed = [("customer", "VF", "Valor Flete")] if profile == "generadora" else [
        ("carrier", "VPT", "Valor pagado a transportador"), ("customer", "VF", "Valor Flete")]
    rows = [row for row in data.get("results", []) if any((row.get("private") or {}).get(k) for k, _, _ in allowed)]
    if not rows:
        return None
    legend = [f"{label} ({short})" for kind, short, label in allowed if any((row.get("private") or {}).get(kind) for row in rows)]
    lines = [f"*Valores de empresa · {data['company']['name']}*", " · ".join(legend)]
    if any(((row.get("private") or {}).get(k) or {}).get("is_simulated") for row in rows for k, _, _ in allowed):
        lines.append("*SIMULACIÓN · Valores de prueba*")
    for row in rows:
        private = row.get("private") or {}
        lines.append("")
        if len(rows) > 1:
            lines.append(f"Ruta ID {row['route_id']} · {row['vehicle']}")
        variant = next((v for v in result.get("variantes", []) if str(v.get("ID_SICE") or v.get("RUTASID")) == str(row["route_id"])), {})
        base = _tonnage(toneladas if toneladas is not None else variant.get("toneladas_configuracion", result.get("toneladas_configuracion")))
        converted = False
        dates = []
        for kind, short, _ in allowed:
            rate = private.get(kind)
            if not rate:
                continue
            unit = rate["unit"]
            text = f"*{short}:* {_money(rate['amount'])} {unit}"
            if unit == "COP/viaje" and base is not None:
                text += f" · {_money(Decimal(str(rate['amount'])) / base)}/tn"
                converted = True
            lines.append(text)
            if rate.get("effective_from"):
                dates.append((short, rate["effective_from"], rate.get("effective_to")))
        if converted:
            weight = format(base, "f").rstrip("0").rstrip(".") if "." in format(base, "f") else format(base, "f")
            lines.append(f"Base por tonelada: {weight.replace('.', ',')} t" + (" de referencia" if toneladas is None else " indicadas"))
        elif any((private.get(k) or {}).get("unit") == "COP/viaje" for k, _, _ in allowed):
            lines.append("Por tonelada: sin base de peso disponible.")
        if dates:
            periods = {(start, end) for _, start, end in dates}
            if len(periods) == 1:
                start, end = next(iter(periods))
                lines.append(f"Vigencia: {start}" + (f" a {end}" if end else " en adelante"))
            else:
                lines.append("Vigencia: " + " · ".join(f"{short} {start}" + (f" a {end}" if end else " en adelante") for short, start, end in dates))
    return "\n".join(lines)[:4096]


def private_rates_message(*, proof, result, route, vehicle, body, travel_mode, toneladas=None):
    url = os.environ.get("SICETAC_ENTERPRISE_URL", "").strip()
    secret = os.environ.get("CAPTURE_WEBHOOK_SECRET", "").strip()
    if not url or not secret or not proof or result.get("estimado"):
        return None
    variants = result.get("variantes") or [{"ID_SICE":(result.get("detalle_lookup") or {}).get("rutasid")}]
    route_ids = list(dict.fromkeys(str(v.get("ID_SICE") or v.get("RUTASID") or "") for v in variants))
    route_ids = [r for r in route_ids if r]
    if not route_ids or len(route_ids)>30:
        return None
    queries = [{"origin":route.get("codigo_dane_origen") or route["origen"],"destination":route.get("codigo_dane_destino") or route["destino"],"vehicle":vehicle,"body":body,"travel_mode":travel_mode,"route_id":r} for r in route_ids]
    try:
        response = requests.post(url,headers={"x-capture-secret":secret,"Content-Type":"application/json"},json={**proof,"queries":queries},timeout=12)
        if response.status_code != 200:
            logger.warning("Private tariff query unavailable: status=%s",response.status_code)
            return None
        data=response.json()
        if not data.get("authorized"):
            return None
        return format_company_values(data, result, toneladas)
    except (requests.RequestException,ValueError,KeyError,TypeError,InvalidOperation):
        logger.warning("Private tariff query unavailable")
        return None

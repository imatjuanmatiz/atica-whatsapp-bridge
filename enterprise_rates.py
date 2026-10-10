"""Private tariff addon. Provider identity is verified by the portal, never by @ text."""
import base64
from contextvars import ContextVar
import logging
import os
import re
import requests

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

def private_rates_message(*, proof, result, route, vehicle, body, travel_mode):
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
        lines=[f"{data['company']['name']} · Tarifas privadas"]
        profile=data.get("business_profile") or data["company"].get("business_profile")
        kinds=[("customer","Flete negociado")] if profile=="generadora" else [("carrier","Valor a pagar al transportador"),("customer","Flete al cliente")]
        available=False
        for row in data.get("results",[]):
            private=row.get("private") or {}
            if not any(private.get(k) for k,_ in kinds):
                continue
            available=True
            if any((private.get(k) or {}).get("is_simulated") for k,_ in kinds):
                lines.append("SIMULACIÓN DE PRUEBA · No son pagos ni facturas reales")
            lines.append(f"Ruta ID {row['route_id']} · {row['vehicle']}")
            for kind,label in kinds:
                rate=private.get(kind)
                if rate:
                    amount=f"{float(rate['amount']):,.2f}".replace(",","_").replace(".",",").replace("_",".")
                    lines.append(f"{label}: $ {amount} {rate['unit']}")
                    lines.append(f"Vigencia: {rate['effective_from']} a {rate.get('effective_to') or 'sin fin definido'}")
            margin=row.get("indicative_difference")
            if margin and profile!="generadora":
                amount=f"{float(margin['amount']):,.2f}".replace(",","_").replace(".",",").replace("_",".")
                lines.append(f"Diferencia de tarifas: $ {amount} {margin['unit']} (no es margen contable)")
        return "\n".join(lines)[:4096] if available else None
    except (requests.RequestException,ValueError,KeyError,TypeError):
        logger.warning("Private tariff query unavailable")
        return None

from fastapi import FastAPI, Request
import requests
import os

app = FastAPI()

# ====== CONFIGURACIÓN DESDE VARIABLES DE ENTORNO ======
VERIFY_TOKEN = os.environ.get("WHATSAPP_VERIFY_TOKEN", "aticatoken123")

# Token y phone_id los sacas del panel de WhatsApp Cloud API
WHATSAPP_TOKEN = os.environ.get("WHATSAPP_ACCESS_TOKEN")
WHATSAPP_PHONE_ID = os.environ.get("WHATSAPP_PHONE_ID")

# URL de Flowise: cópiala del CURL que te da Flowise
# ejemplo: https://cloud.flowiseai.com/api/v1/prediction/3f91702b-f0e8-4be3-9c7a-5cb10659524e
FLOWISE_URL = os.environ.get(
    "FLOWISE_URL",
    "https://cloud.flowiseai.com/api/v1/prediction/3f91702b-f0e8-4be3-9c7a-5cb10659524e",
)


# ====== WEBHOOK DE VERIFICACIÓN (GET) ======
@app.get("/webhook")
async def verify(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN and challenge:
        # Meta espera que devuelvas el challenge en texto plano
        return int(challenge)
    return {"status": "forbidden"}


# ====== MENSAJES ENTRANTES (POST) ======
@app.post("/webhook")
async def receive_message(request: Request):
    data = await request.json()
    # print(data)  # Para debug en logs de Render si quieres

    try:
        entry = data["entry"][0]
        change = entry["changes"][0]
        value = change["value"]
        messages = value.get("messages")

        # Si no hay mensajes (por ejemplo, solo statuses), salimos
        if not messages:
            return {"status": "no messages"}

        message = messages[0]

        from_number = message["from"]  # Ej: "573001234567"

        # Solo manejamos mensajes de texto por ahora
        if "text" in message:
            user_text = message["text"]["body"]
        else:
            # Si no es texto, respondemos algo básico
            send_whatsapp_message(
                to=from_number,
                body="Por ahora solo puedo procesar mensajes de texto.",
            )
            return {"status": "non-text message"}

    except Exception as e:
        # Si el formato no es el esperado, no rompemos
        return {"status": "parse error", "detail": str(e)}

    # ====== 1. ENVIAR TEXTO A FLOWISE ======
    try:
        payload = {
            "question": user_text,
            # Opcional: usar el número como sessionId para mantener contexto
            "overrideConfig": {"sessionId": from_number},
        }

        flowise_resp = requests.post(
            FLOWISE_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        flowise_resp.raise_for_status()
        flow_data = flowise_resp.json()

        # Flowise Cloud suele devolver algo como { "text": "..." } o { "answer": "..." }
        answer = (
            flow_data.get("text")
            or flow_data.get("answer")
            or str(flow_data)
        )

    except Exception as e:
        # Si Flowise falla, respondemos un mensaje de error amable
        send_whatsapp_message(
            to=from_number,
            body="Tuve un problema consultando el modelo ATICA, intenta de nuevo en unos minutos.",
        )
        return {"status": "flowise error", "detail": str(e)}

    # ====== 2. ENVIAR RESPUESTA A WHATSAPP ======
    send_whatsapp_message(to=from_number, body=answer)

    return {"status": "ok"}


# ====== FUNCIÓN AUXILIAR PARA ENVIAR MENSAJES A WHATSAPP ======
def send_whatsapp_message(to: str, body: str):
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_ID:
        # Si faltan variables de entorno, no intentamos enviar
        return

    url = f"https://graph.facebook.com/v19.0/{WHATSAPP_PHONE_ID}/messages"

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "text": {"body": body[:4096]},  # WhatsApp tiene límite de longitud
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        # resp.raise_for_status()  # puedes activarlo si quieres ver fallos en logs
    except Exception:
        pass

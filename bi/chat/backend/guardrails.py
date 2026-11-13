import re
import unicodedata

from bi.chat.backend.config import OPENAI_API_KEY, S3_BUCKET

RECHAZO_BI = (
    "Solo puedo responder consultas sobre las métricas de monitoreo del asistente "
    "y el comportamiento transaccional agregado del dashboard. "
    "Por políticas de seguridad no se muestran datos sensibles como CVV, "
    "números de tarjeta, correos individuales ni credenciales."
)

# Bloquear datos sensibles de tarjetas, PII individual y secretos
PATRON_SENSIBLE = re.compile(
    r"\b(cvv|cvc|cvv2|nip|pin|clabe|password|contrase[ñn]a|"
    r"card_number|numero\s+de\s+tarjeta|n[uú]mero\s+de\s+tarjeta|"
    r"secret_key|access_key|session_secret|api_key|credenciales|"
    r"curp|rfc|ssn)\b|"
    r"sk-[a-z0-9_-]{12,}|akia[a-z0-9]{12,}|"
    r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b",
    re.IGNORECASE,
)

# Bloquear intentos de inyeccion de prompt o extraccion de reglas internas
PATRON_ATAQUE = re.compile(
    r"ignora.{0,40}(reglas|instrucciones)|"
    r"(revela|muestra|imprime).{0,40}(instrucciones|prompt|configuraci[oó]n|secretos)|"
    r"(dame|muestra|busca).{0,30}(correo|email|tel[eé]fono|direcci[oó]n).{0,25}(cliente|usuario)",
    re.IGNORECASE,
)


# Normalizar texto de entrada o salida
def normalizar(texto):
    return "".join(
        c
        for c in unicodedata.normalize("NFKC", str(texto or ""))
        if unicodedata.category(c) != "Cf"
    )


# Verificar si el texto contiene datos restringidos
def contiene_dato_sensible(texto):
    normal = normalizar(texto)
    if PATRON_SENSIBLE.search(normal) or PATRON_ATAQUE.search(normal):
        return True
    secretos = [OPENAI_API_KEY, S3_BUCKET]
    return any(s and len(s) >= 8 and s.lower() in normal.lower() for s in secretos)


# Validar pregunta del usuario antes de consultar el modelo
def entrada_restringida_bi(texto):
    return contiene_dato_sensible(texto)


# Proteger respuesta generada antes de enviarla al dashboard
def proteger_respuesta_bi(texto):
    if not isinstance(texto, str) or not texto.strip():
        return "No se pudo generar el análisis en este momento. Intenta de nuevo."
    if contiene_dato_sensible(texto):
        return RECHAZO_BI
    return texto.strip()[:4000]

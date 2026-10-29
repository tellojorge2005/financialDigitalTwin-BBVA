import re
import unicodedata

from backend.config import (
    OPENAI_API_KEY, OPENAI_MODEL, SESSION_SECRET, S3_BUCKET,
    S3_CUSTOMER_PATH, S3_MONTHLY_PATH, AWS_REGION
)
from backend.data import CUSTOMER_COLUMNS


RECHAZO = (
    "Puedo ayudarte con tu información financiera y tus metas de ahorro. "
    "¿Qué deseas consultar?"
)

# Comprobación complementaria al aislamiento de datos y al contexto permitido
PATRON = re.compile(
    r"\b(xgboost|langgraph|langchain|openai|chatgpt|gpt|spark|pyspark|"
    r"airflow|postgres(?:ql)?|sqlite|parquet|docker|aws|s3|api|sdk|sql|"
    r"backend|frontend|endpoint|bucket|servidor|infraestructura|"
    r"credenciales|prompt|token|tokens|json|yaml|python|javascript|"
    r"localhost|traceback|schema|modelo|algoritmo|arquitectura|"
    r"infraestructuras|servidores|almacenamiento|framework|nube)\b|"
    r"https?://|s3a?://|[a-z]:\\|/opt/|/home/|/api/|\x60{3}|"
    r"\b[a-z]+_[a-z_]+\b|sk-[a-z0-9_-]{12,}",
    re.IGNORECASE
)
ATAQUE = re.compile(
    r"ignora.{0,40}(reglas|instrucciones)|"
    r"(revela|muestra|imprime).{0,40}(instrucciones|configuraci[oó]n|secretos)|"
    r"(otro|otros|otra|otras).{0,15}(cliente|persona)|"
    r"(cliente|usuario).{0,10}(id|identificador)|"
    r"(base64|rot13|hexadecimal|desarrollador|administrador)",
    re.IGNORECASE
)


def normalizar(texto):
    return "".join(
        c for c in unicodedata.normalize("NFKC", texto)
        if unicodedata.category(c) != "Cf"
    )


def contiene_informacion_interna(texto):
    normal = normalizar(texto)
    if PATRON.search(normal):
        return True
    compactado = re.sub(r"[^a-z0-9]", "", normal.lower())
    if any(t in compactado for t in ("xgboost", "langchain", "langgraph")):
        return True
    valores = [
        OPENAI_API_KEY, OPENAI_MODEL, SESSION_SECRET, S3_BUCKET,
        S3_CUSTOMER_PATH, S3_MONTHLY_PATH, AWS_REGION, *CUSTOMER_COLUMNS
    ]
    return any(
        len(v) >= 4 and v.lower() in normal.lower()
        for v in valores if isinstance(v, str)
    )


def entrada_restringida(texto):
    return contiene_informacion_interna(texto) or bool(ATAQUE.search(texto))


def proteger_respuesta(texto):
    if not isinstance(texto, str) or not texto.strip():
        return "No pude preparar la respuesta. Intenta formularlo de otra manera."
    if contiene_informacion_interna(texto):
        return RECHAZO
    return texto.strip()[:5000]


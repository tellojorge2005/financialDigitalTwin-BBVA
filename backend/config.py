import os
from pathlib import Path

from dotenv import load_dotenv


# Definir directorio del proyecto
PROJECT_PATH = Path(__file__).resolve().parent.parent
FRONTEND_PATH = PROJECT_PATH / "frontend"


# Cargar variables de entorno
load_dotenv(PROJECT_PATH / ".env", override=False)

AWS_REGION = os.getenv(
    "AWS_DEFAULT_REGION",
    "us-east-2"
).strip()

S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "financial-digital-twin-bbva-022950218031-us-east-2-an"
).strip()

S3_CUSTOMER_PATH = os.getenv(
    "S3_CUSTOMER_PATH",
    "gold/customer_financial_summary.parquet"
).strip("/")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

OPENAI_MODEL = os.getenv(
    "OPENAI_MODEL",
    "gpt-4o-mini"
).strip()

SESSION_SECRET = os.getenv("SESSION_SECRET", "").strip()

COOKIE_SECURE = (
    os.getenv("COOKIE_SECURE", "false").strip().lower() == "true"
)


# Validar configuración
def validar_configuracion():
    if not OPENAI_API_KEY or OPENAI_API_KEY in {
        "TU_API_KEY",
        "_TU_API_KEY"
    }:
        raise RuntimeError("Configura OPENAI_API_KEY en .env.")

    if not OPENAI_MODEL or OPENAI_MODEL == "TU_MODELO":
        raise RuntimeError("Configura OPENAI_MODEL en .env.")

    if (
        len(SESSION_SECRET) < 32
        or SESSION_SECRET == "REEMPLAZAR_CON_UN_SECRETO_ALEATORIO"
    ):
        raise RuntimeError(
            "Configura SESSION_SECRET con un secreto aleatorio."
        )

    if not S3_BUCKET or not S3_CUSTOMER_PATH:
        raise RuntimeError("Configura la ubicación de los datos en S3.")

    if not (FRONTEND_PATH / "index.html").is_file():
        raise RuntimeError("No se encontró frontend/index.html.")
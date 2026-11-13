import os
from pathlib import Path

from dotenv import load_dotenv

# Definir directorio del proyecto
PROJECT_PATH = Path(__file__).resolve().parents[2]
FRONTEND_PATH = PROJECT_PATH / "web" / "frontend"


# Cargar variables de entorno
load_dotenv(PROJECT_PATH / ".env", override=False)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2").strip()

S3_BUCKET = os.getenv("S3_BUCKET", "").strip()

S3_CUSTOMER_PATH = os.getenv(
    "S3_CUSTOMER_PATH", "data/gold/customer_model_summary.parquet"
).strip("/")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

SESSION_SECRET = os.getenv("SESSION_SECRET", "").strip()

COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").strip().lower() == "true"


# Validar configuración
def validar_configuracion():
    if not OPENAI_API_KEY or OPENAI_API_KEY in {"TU_API_KEY", "_TU_API_KEY"}:
        raise RuntimeError("Configura OPENAI_API_KEY en .env.")

    if not OPENAI_MODEL or OPENAI_MODEL == "TU_MODELO":
        raise RuntimeError("Configura OPENAI_MODEL en .env.")

    if (
        len(SESSION_SECRET) < 32
        or SESSION_SECRET == "REEMPLAZAR_CON_UN_SECRETO_ALEATORIO"
    ):
        raise RuntimeError("Configura SESSION_SECRET con un secreto aleatorio.")

    if not S3_BUCKET or not S3_CUSTOMER_PATH:
        raise RuntimeError("Configura la ubicación de los datos en S3.")

    if not (FRONTEND_PATH / "index.html").is_file():
        raise RuntimeError("No se encontró web/frontend/index.html.")


# Historial del prototipo, fuera del repositorio y de las cookies
CHAT_DB_PATH = Path(
    os.getenv(
        "CHAT_DB_PATH", str(Path.home() / ".financial_digital_twin" / "chat.sqlite3")
    )
).expanduser()

S3_MONTHLY_PATH = os.getenv(
    "S3_MONTHLY_PATH", "data/gold/customer_monthly_summary.parquet"
).strip("/")

SESSION_SECONDS = 3600
MAX_CHAT_MESSAGES = 30


# Rutas adicionales del panorama financiero
S3_SCENARIOS_PATH = os.getenv(
    "S3_SCENARIOS_PATH", "data/gold/customer_cashflow_scenarios.parquet"
).strip("/")
S3_CATEGORIES_PATH = os.getenv(
    "S3_CATEGORIES_PATH", "data/gold/customer_expenses_by_category.parquet"
).strip("/")

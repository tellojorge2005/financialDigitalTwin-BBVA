import os
from pathlib import Path
from dotenv import load_dotenv


# Ubicar raiz del proyecto y cargar variables de entorno
PROJECT_PATH = Path(__file__).resolve().parents[3]
FRONTEND_PATH = Path(__file__).resolve().parents[1] / "frontend"
load_dotenv(PROJECT_PATH / ".env", override=False)

# Leer configuracion de OpenAI y AWS S3 desde .env
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()

# Leer rutas de monitoreo y Gold definidas en .env
S3_INTERACTIONS_PREFIX = os.getenv("S3_INTERACTIONS_PREFIX", "").strip().strip("/")
S3_CUSTOMER_PATH = os.getenv("S3_CUSTOMER_PATH", "").strip()
S3_MONTHLY_PATH = os.getenv("S3_MONTHLY_PATH", "").strip()
S3_CATEGORIES_PATH = os.getenv("S3_CATEGORIES_PATH", "").strip()


# Validar variables requeridas para iniciar el asistente de BI
def validar_configuracion_bi():
    if not OPENAI_API_KEY:
        raise RuntimeError("Configura OPENAI_API_KEY en el archivo .env.")
    if not S3_BUCKET:
        raise RuntimeError("Configura S3_BUCKET en el archivo .env.")
    if not S3_INTERACTIONS_PREFIX:
        raise RuntimeError("Configura S3_INTERACTIONS_PREFIX en el archivo .env.")

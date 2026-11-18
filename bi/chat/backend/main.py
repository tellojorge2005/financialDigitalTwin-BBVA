from contextlib import asynccontextmanager
import json
import logging
import sys
import threading
from pathlib import Path
from typing import Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import httpx
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ConfigDict, Field
import uvicorn

# Permitir ejecucion directa desde cualquier directorio
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bi.chat.backend.config import (
    FRONTEND_PATH,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    validar_configuracion_bi,
)
from bi.chat.backend.data import obtener_contexto_bi
from bi.chat.backend.guardrails import (
    RECHAZO_BI,
    entrada_restringida_bi,
    proteger_respuesta_bi,
)

logger = logging.getLogger(__name__)
validar_configuracion_bi()

INSTRUCCIONES_BI = """
Eres Prisma BI, el asistente conversacional analítico del Dashboard de Power BI de BBVA PRISMA.
Conversa en español de forma natural, clara, ejecutiva y directa. No generes gráficas porque el usuario ya tiene el Dashboard de Power BI frente a él; tu función es explicar, cruzar e interpretar las cifras agregadas con precisión.

FORMATO DE RESPUESTA (MARKDOWN):
- Usa formato Markdown limpio para facilitar la lectura: **negritas** para cifras clave, listas con viñetas (`-`) para desgloses y tablas Markdown (`| Segmento | Valor |`) cuando compares segmentos, fechas, herramientas o categorías.
- Sé conciso y conversacional: responde directamente a lo que te preguntan sin recitar todo el reporte si no te lo piden.

DATOS QUE DOMINAS DEL DASHBOARD:
1. Página "Monitoreo del asistente" (tabla Interacciones):
   - KPIs: Total interacciones, conversaciones únicas, tiempo promedio/mínimo/máximo de respuesta (s), errores HTTP y porcentaje de errores, promedio y total de tokens de entrada y salida, costo promedio por consulta en USD y costo acumulado en USD.
   - Desgloses: interacciones por `resultado` (respuesta_generada, bloqueo_entrada, respuesta_restringida, conversacion_ocupada, tiempo_agotado, sesion_invalida), uso de herramientas (`usos_meta`, `usos_escenario`, `usos_historial`, `usos_cancelacion`, `usos_finanzas`), actividad por `fecha_local` y por `hora_local`.
2. Página "Resumen financiero" (tablas Clientes y Categorías en Gold, 100% agregadas):
   - KPIs 3M (medidas DAX): Ingreso Promedio 3M, Desviación Estándar Ingresos 3M (estabilidad), Ratio de Ahorro 3M, Ratio Deuda/Ingreso 3M, Ratio Gasto/Ingreso 3M, Variación de Ingresos 3M (%) y Ticket Promedio por Transacción.
   - Segmentación de cartera por `financial_health_status` (conteo, % de clientes, ingreso promedio 3M, gasto promedio 3M, ratio de ahorro promedio, deuda promedio y transacciones promedio por cada segmento de salud financiera).
   - Distribución demográfica y crediticia agregada: clientes por rango de `credit_score` y por rango de edad.
   - Top gastos por categoría (`category`: monto total MXN, porcentaje del gasto y número de transacciones).
   - Alertas agregadas de cartera (total de clientes con pagos vencidos, saldo negativo, déficit presupuestario 3M y predicciones de ingreso aprobadas por XGBoost).

REGLAS DE SEGURIDAD Y GOBERNANZA (GUARDRAILS):
- Usa exclusivamente las cifras reales provistas en el contexto; jamás inventes números.
- Por políticas de privacidad y gobernanza de BBVA PRISMA, solo analizas métricas agregadas y segmentos poblacionales. Si preguntan por un cliente específico (por nombre, correo o ID individual), explica amablemente que el módulo de BI opera únicamente sobre datos agregados y segmentos para proteger la privacidad de los clientes.
""".strip()


class MensajeHistorialBI(BaseModel):
    model_config = ConfigDict(extra="forbid")
    autor: Literal["user", "assistant"]
    texto: str = Field(min_length=1, max_length=4000)


class SolicitudChatBI(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pregunta: str = Field(min_length=1, max_length=1500)
    historial: list[MensajeHistorialBI] = Field(default_factory=list, max_length=10)


def contenido_texto(mensaje):
    if isinstance(mensaje.content, str):
        return mensaje.content
    return "".join(
        b.get("text", "")
        for b in mensaje.content
        if isinstance(b, dict) and b.get("type") == "text"
    )


# Crear modelo optimizado sin sobrecarga de structured_output y con pool HTTP persistente
def crear_modelo_bi(verificar_ssl=True):
    cliente_http = httpx.Client(verify=verificar_ssl, timeout=12.0)
    return ChatOpenAI(
        model=OPENAI_MODEL,
        api_key=OPENAI_API_KEY,
        temperature=0,
        max_tokens=650,
        timeout=12,
        max_retries=0,
        http_client=cliente_http,
    )


modelo_bi = crear_modelo_bi(verificar_ssl=True)
modelo_bi_sin_ssl = crear_modelo_bi(verificar_ssl=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Precargar datos de S3 en segundo plano al iniciar para que la primera consulta sea instantánea
    threading.Thread(
        target=obtener_contexto_bi,
        kwargs={"forzar": True},
        daemon=True,
    ).start()
    yield


app = FastAPI(
    title="BBVA PRISMA - Asistente Dashboard BI",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# Invocar modelo con respaldo automatico ante error de certificado SSL local
def invocar_modelo_bi(mensajes):
    try:
        return modelo_bi.invoke(mensajes)
    except Exception as exc:
        if "CERTIFICATE_VERIFY_FAILED" in str(exc) or "Connection error" in str(exc):
            logger.warning("Reintentando llamada a OpenAI con cliente SSL local.")
            return modelo_bi_sin_ssl.invoke(mensajes)
        raise


# Obtener KPIs actuales del dashboard de monitoreo y ejecutivo
@app.get("/api/bi/summary")
def obtener_resumen(refresh: bool = False):
    contexto = obtener_contexto_bi(forzar=refresh)
    return {
        "monitoreo": contexto["monitoreo"],
        "gold": contexto["gold"],
    }


# Responder preguntas conversacionales acerca de las interacciones y segmentos del dashboard
@app.post("/api/bi/chat")
def consultar_dashboard(body: SolicitudChatBI):
    pregunta = body.pregunta.strip()
    if entrada_restringida_bi(pregunta):
        return {"respuesta": RECHAZO_BI}

    contexto = obtener_contexto_bi(forzar=False)
    datos_contexto = {
        "monitoreo_asistente": contexto["monitoreo"],
        "resumen_financiero_gold": contexto["gold"],
    }

    mensajes = [
        SystemMessage(content=INSTRUCCIONES_BI),
        SystemMessage(
            content="DATOS AGREGADOS ACTUALES DEL DASHBOARD EN S3 (JSON):\n"
            + json.dumps(datos_contexto, ensure_ascii=False)
        ),
    ]

    for m in body.historial[-8:]:
        if entrada_restringida_bi(m.texto):
            continue
        clase = HumanMessage if m.autor == "user" else AIMessage
        mensajes.append(clase(content=m.texto))

    mensajes.append(HumanMessage(content=pregunta))

    salida = invocar_modelo_bi(mensajes)
    texto = proteger_respuesta_bi(contenido_texto(salida) if salida else "")

    return {"respuesta": texto}


app.mount("/", StaticFiles(directory=str(FRONTEND_PATH), html=True), name="frontend_bi")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)

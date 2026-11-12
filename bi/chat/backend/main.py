import json
import logging
import sys
from pathlib import Path
from typing import Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import httpx
from langchain_core.messages import HumanMessage, SystemMessage
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
Eres el Asistente Analítico del Dashboard de Monitoreo y Ejecutivo de BBVA PRISMA.
Tu objetivo es responder preguntas claras, breves y precisas en español sobre:
1. El Dashboard de Monitoreo del Asistente:
   - Total de interacciones, conversaciones únicas, tiempo promedio de respuesta (s),
     porcentaje de errores HTTP, promedio de tokens de entrada y salida, costo promedio
     por consulta en USD, desglose por resultado (respuesta_generada, conversacion_ocupada,
     bloqueo_entrada, tiempo_agotado, sesion_invalida), actividad por fecha_local y
     uso de herramientas (Metas de ahorro, Escenarios, Consulta de historial, Cancelaciones).
2. El Resumen Ejecutivo / Comportamiento Transaccional de Clientes (tablas Gold):
   - Total de clientes, promedios y desviación estándar de ingresos de los últimos 3 meses,
     ratios financieros a 3 meses (ratio de ahorro, gasto/ingreso, deuda/ingreso),
     variación de ingresos a 3 meses y gastos principales por categoría.

REGLAS DE SEGURIDAD Y ESTILO:
- Usa exclusivamente las cifras proporcionadas en el contexto de datos reales.
- Nunca inventes datos ni reveles información personal individual (correos, teléfonos,
  números de tarjeta, CVV, NIP, contraseñas o credenciales internas).
- Si el usuario pregunta por un componente del dashboard, elige en `componente_visual`
  el gráfico que mejor acompañe tu respuesta:
  - "resultados": cuando hablen de resultados, errores, bloqueos o estado de interacciones.
  - "fechas": cuando hablen de volumen de interacciones por día o fecha_local.
  - "tiempos": cuando hablen de latencia, duración o tiempo promedio de respuesta.
  - "herramientas": cuando hablen de metas de ahorro, escenarios, historial o cancelaciones.
  - "gold_3m": cuando hablen de ingresos, promedios de 3 meses, desviación estándar, ratios o deuda de clientes.
  - "categorias": cuando hablen de categorías de gasto o comportamiento de compras.
  - "ninguno": para saludos breves o preguntas que no requieran gráfica.
""".strip()


class SolicitudChatBI(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pregunta: str = Field(min_length=1, max_length=1500)


class RespuestaEstructuradaBI(BaseModel):
    model_config = ConfigDict(extra="forbid")
    respuesta: str
    componente_visual: Literal[
        "resultados",
        "fechas",
        "tiempos",
        "herramientas",
        "gold_3m",
        "categorias",
        "ninguno",
    ]


# Crear modelo con cliente HTTP estandar y respaldo para certificado local en Windows
def crear_modelo_bi(verificar_ssl=True):
    cliente_http = httpx.Client(verify=verificar_ssl, timeout=20.0)
    return ChatOpenAI(
        model=OPENAI_MODEL,
        api_key=OPENAI_API_KEY,
        temperature=0,
        max_tokens=700,
        timeout=20,
        http_client=cliente_http,
    ).with_structured_output(RespuestaEstructuradaBI, method="json_schema")


modelo_bi = crear_modelo_bi(verificar_ssl=True)
modelo_bi_sin_ssl = crear_modelo_bi(verificar_ssl=False)

app = FastAPI(
    title="BBVA PRISMA - Asistente Dashboard BI", docs_url=None, redoc_url=None
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


# Responder preguntas acerca de los datos del dashboard con soporte visual
@app.post("/api/bi/chat")
def consultar_dashboard(body: SolicitudChatBI):
    pregunta = body.pregunta.strip()
    if entrada_restringida_bi(pregunta):
        return {"respuesta": RECHAZO_BI, "visual": None}

    contexto = obtener_contexto_bi(forzar=False)
    datos_prompt = {
        "monitoreo_asistente": contexto["monitoreo"],
        "comportamiento_clientes_gold": contexto["gold"],
    }

    salida = invocar_modelo_bi(
        [
            SystemMessage(content=INSTRUCCIONES_BI),
            HumanMessage(
                content=json.dumps(
                    {
                        "datos_actuales_dashboard": datos_prompt,
                        "pregunta_usuario": pregunta,
                    },
                    ensure_ascii=False,
                )
            ),
        ]
    )

    texto = proteger_respuesta_bi(salida.respuesta if salida else "")
    clave_visual = salida.componente_visual if salida else "ninguno"
    visual = contexto["visuales"].get(clave_visual) if texto != RECHAZO_BI else None

    return {
        "respuesta": texto,
        "visual": visual,
    }


app.mount("/", StaticFiles(directory=str(FRONTEND_PATH), html=True), name="frontend_bi")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)

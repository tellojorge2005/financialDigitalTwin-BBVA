import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from openai import APIError, APITimeoutError, RateLimitError
from pydantic import BaseModel, Field, StringConstraints
from starlette.middleware.sessions import SessionMiddleware

from backend.assistant import crear_asistente
from backend.config import (
    COOKIE_SECURE,
    FRONTEND_PATH,
    SESSION_SECRET,
    validar_configuracion
)
from backend.data import (
    ClienteDuplicado,
    ErrorDatos,
    consultar_cliente
)


logger = logging.getLogger(__name__)

validar_configuracion()


# Definir solicitudes
class SolicitudLogin(BaseModel):
    cliente_id: int = Field(
        strict=True,
        ge=0,
        le=9007199254740991
    )


class SolicitudConsulta(BaseModel):
    pregunta: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            max_length=2000
        )
    ]


# Inicializar asistente
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.assistant = crear_asistente()
    yield


app = FastAPI(
    title="Financial Digital Twin",
    lifespan=lifespan
)


# Configurar sesión
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="financial_twin_session",
    max_age=3600,
    same_site="strict",
    https_only=COOKIE_SECURE
)


# Configurar respuestas
@app.middleware("http")
async def configurar_respuesta(request: Request, call_next):
    response = await call_next(request)

    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"

    return response


# Manejar errores de datos
@app.exception_handler(ErrorDatos)
async def manejar_error_datos(request: Request, exc: ErrorDatos):
    return JSONResponse(
        status_code=503,
        content={
            "detail": (
                "No se pudo leer el resumen GOLD. "
                "Revisa la configuración y los permisos de S3."
            )
        }
    )


@app.exception_handler(ClienteDuplicado)
async def manejar_cliente_duplicado(
    request: Request,
    exc: ClienteDuplicado
):
    return JSONResponse(
        status_code=409,
        content={
            "detail": "Existe más de un resumen para el mismo cliente."
        }
    )


# Consultar sesión
def obtener_cliente_id(request: Request) -> int:
    cliente_id = request.session.get("cliente_id")

    if type(cliente_id) is not int:
        raise HTTPException(
            status_code=401,
            detail="Inicia sesión para continuar."
        )

    return cliente_id


# Iniciar sesión
@app.post("/api/login")
def iniciar_sesion(body: SolicitudLogin, request: Request):
    request.session.clear()

    datos = consultar_cliente(body.cliente_id)

    if not datos:
        raise HTTPException(
            status_code=404,
            detail="No se encontró información para ese ID."
        )

    request.session["cliente_id"] = body.cliente_id

    return {
        "cliente_id": body.cliente_id,
        "datos_cliente": datos
    }


# Recuperar sesión
@app.get("/api/me")
def recuperar_sesion(request: Request):
    cliente_id = obtener_cliente_id(request)
    datos = consultar_cliente(cliente_id)

    if not datos:
        request.session.clear()

        raise HTTPException(
            status_code=401,
            detail="El cliente ya no está disponible. Inicia sesión nuevamente."
        )

    return {
        "cliente_id": cliente_id,
        "datos_cliente": datos
    }


# Cerrar sesión
@app.post("/api/logout", status_code=204)
def cerrar_sesion(request: Request):
    request.session.clear()
    return Response(status_code=204)


# Consultar asistente
@app.post("/api/chat")
def consultar_asistente(body: SolicitudConsulta, request: Request):
    cliente_id = obtener_cliente_id(request)

    try:
        resultado = request.app.state.assistant.invoke({
            "cliente_id": cliente_id,
            "pregunta": body.pregunta
        })

    except (ErrorDatos, ClienteDuplicado):
        raise

    except APITimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="El modelo tardó demasiado. Intenta nuevamente."
        ) from exc

    except RateLimitError as exc:
        raise HTTPException(
            status_code=429,
            detail=(
                "El servicio del modelo alcanzó un límite. "
                "Revisa su disponibilidad o intenta más tarde."
            )
        ) from exc

    except APIError as exc:
        logger.error("Error del servicio del modelo: %s", type(exc).__name__)

        raise HTTPException(
            status_code=502,
            detail="No se pudo obtener una respuesta del modelo."
        ) from exc

    except Exception as exc:
        logger.exception("No se pudo completar la consulta.")

        raise HTTPException(
            status_code=500,
            detail="No se pudo procesar la consulta."
        ) from exc

    if not resultado["encontrado"]:
        request.session.clear()

        raise HTTPException(
            status_code=401,
            detail="El cliente ya no está disponible. Inicia sesión nuevamente."
        )

    return {
        "cliente_id": cliente_id,
        "pregunta": body.pregunta,
        "estado": "encontrado",
        "respuesta": resultado["respuesta"],
        "datos_cliente": resultado["datos_cliente"]
    }


# Servir frontend
app.mount(
    "/",
    StaticFiles(directory=str(FRONTEND_PATH), html=True),
    name="frontend"
)
import asyncio
from time import monotonic
import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from openai import APIError, APITimeoutError, RateLimitError
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, EmailStr
from starlette.concurrency import run_in_threadpool
from starlette.middleware.sessions import SessionMiddleware

from backend.assistant import crear_asistente
from backend.config import (
    COOKIE_SECURE,
    FRONTEND_PATH,
    SESSION_SECRET,
    SESSION_SECONDS,
    validar_configuracion,
)
from backend.data import (
    ClienteDuplicado,
    ErrorDatos,
    consultar_cliente,
    consultar_por_correo,
    panorama_publico,
    resumen_publico,
)
from backend.guardrails import RECHAZO
from backend.turns import ejecutar_turno, LIMITE_TURNO
from backend.history import (
    inicializar_historial,
    crear_conversacion,
    borrar_conversacion,
    leer_conversacion,
    reservar_conversacion,
    estado_conversacion,
    guardar_turno,
    liberar_conversacion,
    SesionExpirada,
    ConversacionOcupada,
)
from backend.monitoring import (
    MetricasInteraccion,
    MonitorInteracciones,
)


logger = logging.getLogger(__name__)
validar_configuracion()


class SolicitudLogin(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: EmailStr = Field(max_length=254)


class SolicitudConsulta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str | None = Field(default=None, min_length=16, max_length=64, pattern=r"^[A-Za-z0-9-]+$")
    pregunta: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            max_length=2000,
        ),
    ]


@asynccontextmanager
async def lifespan(app: FastAPI):
    inicializar_historial()
    app.state.assistant = crear_asistente()

    monitor = MonitorInteracciones()
    await run_in_threadpool(monitor.inicializar)
    app.state.monitor = monitor
    monitor.iniciar_exportador()

    try:
        yield
    finally:
        await run_in_threadpool(monitor.detener_exportador)


app = FastAPI(
    title="Asistente financiero",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    debug=False,
)

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="financial_twin_session",
    max_age=SESSION_SECONDS,
    same_site="strict",
    https_only=COOKIE_SECURE,
)


def resultado_monitoreo(status, marca):
    # Una respuesta generada no equivale a una respuesta correcta
    # ni confirma que el navegador haya llegado a mostrarla.
    if status == 200:
        return marca or "respuesta_generada"

    return {
        401: "sesion_invalida",
        409: "conversacion_ocupada",
        422: "solicitud_invalida",
        429: "limite_servicio",
        502: "error_servicio",
        503: "datos_no_disponibles",
        504: "tiempo_agotado",
    }.get(status, "error_http")


@app.middleware("http")
async def configurar_respuesta(request: Request, call_next):
    es_chat = (
        request.method == "POST"
        and request.url.path.rstrip("/") == "/api/chat"
    )
    metricas = None
    status = None

    if es_chat:
        metricas = MetricasInteraccion()
        request.state.metricas = metricas
        request.state.resultado_monitor = None
        await run_in_threadpool(
            request.app.state.monitor.registrar_inicio,
            metricas,
        )

    try:
        response = await call_next(request)
        status = response.status_code

        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    except Exception:
        status = 500
        raise

    finally:
        if metricas is not None:
            marca = getattr(request.state, "resultado_monitor", None)
            resultado = (
                resultado_monitoreo(status, marca)
                if status is not None
                else "solicitud_interrumpida"
            )
            registro = metricas.registro_final(status, resultado)
            await run_in_threadpool(
                request.app.state.monitor.registrar_fin,
                metricas,
                registro,
            )


# No enviar excepciones, esquemas, ubicaciones ni configuración al navegador.
@app.exception_handler(ErrorDatos)
@app.exception_handler(ClienteDuplicado)
async def manejar_error_datos(request, exc):
    return JSONResponse(
        status_code=503,
        content={
            "detail": (
                "Tu información no está disponible temporalmente. "
                "Intenta más tarde."
            )
        },
    )


@app.exception_handler(RequestValidationError)
async def manejar_solicitud_invalida(request, exc):
    return JSONResponse(
        status_code=422,
        content={"detail": "Revisa los datos enviados e intenta nuevamente."},
    )


@app.exception_handler(SesionExpirada)
async def manejar_sesion_expirada(request, exc):
    request.session.clear()
    return JSONResponse(
        status_code=401,
        content={"detail": "Tu sesión terminó. Inicia sesión nuevamente."},
    )


@app.exception_handler(ConversacionOcupada)
async def manejar_conversacion_ocupada(request, exc):
    return JSONResponse(
        status_code=409,
        content={"detail": "Espera a que termine la respuesta anterior."},
    )


@app.exception_handler(Exception)
async def manejar_error_general(request, exc):
    logger.error("Error no controlado: %s", type(exc).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "No se pudo completar la solicitud. Intenta más tarde."
        },
    )


def obtener_sesion(request):
    cliente = request.session.get("cliente_id")
    sid = request.session.get("sid")

    if (
        type(cliente) is not int
        or cliente < 0
        or not isinstance(sid, str)
    ):
        raise SesionExpirada()

    return cliente, sid


# Acceso de demostración mediante el correo registrado del cliente
@app.post("/api/login")
def iniciar_sesion(body: SolicitudLogin, request: Request):
    datos = consultar_por_correo(str(body.email))
    if not datos:
        raise HTTPException(404, "No se encontró un perfil con ese correo.")

    cliente = datos["id"]
    panorama = panorama_publico(cliente, datos)
    borrar_conversacion(request.session.get("sid"))
    request.session.clear()
    sid = crear_conversacion(cliente)
    request.session.update({"cliente_id": cliente, "sid": sid})
    return {
        "cliente_id": cliente,
        "datos_cliente": resumen_publico(datos),
        "historial": [],
        **panorama,
    }


@app.get("/api/me")
def recuperar_sesion(request: Request):
    cliente, sid = obtener_sesion(request)
    conversacion = leer_conversacion(sid, cliente)
    datos = consultar_cliente(cliente)

    if not datos:
        borrar_conversacion(sid)
        raise SesionExpirada()

    return {
        "cliente_id": cliente,
        "datos_cliente": resumen_publico(datos),
        "historial": conversacion["mensajes"],
        **panorama_publico(cliente, datos),
    }


@app.post("/api/logout", status_code=204)
def cerrar_sesion(request: Request):
    borrar_conversacion(request.session.get("sid"))
    request.session.clear()
    request.app.state.monitor.solicitar_exportacion()
    return Response(status_code=204)


# Rotar la conversación impide que una respuesta en curso restaure el chat borrado.
@app.post("/api/chat/clear", status_code=204)
def limpiar_conversacion(request: Request):
    cliente, sid = obtener_sesion(request)
    leer_conversacion(sid, cliente)
    borrar_conversacion(sid)
    request.session["sid"] = crear_conversacion(cliente)
    request.app.state.monitor.solicitar_exportacion()
    return Response(status_code=204)


@app.get("/api/chat/status")
def recuperar_estado_chat(request: Request):
    cliente, sid = obtener_sesion(request)
    return {"cliente_id": cliente, **estado_conversacion(sid, cliente)}


@app.post("/api/chat")
async def consultar_asistente(body: SolicitudConsulta, request: Request):
    cliente, sid = obtener_sesion(request)

    metricas = request.state.metricas
    metricas.asociar_conversacion(sid)

    token, conversacion = await run_in_threadpool(reservar_conversacion, sid, cliente)

    try:
        # Acotar el turno completo, no solo cada llamada individual
        limite = monotonic() + LIMITE_TURNO - 2
        resultado = await asyncio.wait_for(
            asyncio.to_thread(
                ejecutar_turno,
                request.app.state.assistant,
                {
                    "cliente_id": cliente,
                    "pregunta": body.pregunta,
                    "mensajes": conversacion["mensajes"],
                    "meta": conversacion["meta"],
                },
                {"callbacks": [metricas]},
                limite,
            ),
            timeout=LIMITE_TURNO,
        )

        if not resultado["encontrado"]:
            borrar_conversacion(sid)
            raise SesionExpirada()

        # Identificar la respuesta de bloqueo existente sin cambiar el asistente.
        if (
            resultado.get("texto_usuario")
            == "[Solicitud fuera de alcance]"
        ):
            request.state.resultado_monitor = "bloqueo_entrada"
        elif resultado.get("respuesta") == RECHAZO:
            request.state.resultado_monitor = "respuesta_restringida"

        mensajes = conversacion["mensajes"] + [
            {
                "autor": "user",
                "texto": resultado.get("texto_usuario", body.pregunta),
            },
            {
                "autor": "assistant",
                "texto": resultado["respuesta"],
            },
        ]

        await run_in_threadpool(
            guardar_turno,
            sid,
            cliente,
            token,
            mensajes,
            resultado.get("meta", conversacion["meta"]),
            body.request_id,
        )

        return {
            "cliente_id": cliente,
            "estado": "encontrado",
            "respuesta": resultado["respuesta"],
            "datos_cliente": resumen_publico(resultado["datos_cliente"]),
        }

    except (TimeoutError, APITimeoutError) as exc:
        logger.warning("Turno agotado; se descarta su resultado tardío y se libera la conversación.")
        raise HTTPException(
            504,
            "No se completó la respuesta a tiempo. Tu conversación se conserva; puedes volver a intentar.",
        ) from exc

    except RateLimitError as exc:
        raise HTTPException(
            429,
            "El servicio está ocupado. Intenta más tarde.",
        ) from exc

    except APIError as exc:
        logger.error(
            "Error del servicio de respuesta: %s",
            type(exc).__name__,
        )
        raise HTTPException(
            502,
            "No se pudo preparar una respuesta. Intenta más tarde.",
        ) from exc

    finally:
        # No ocultar una respuesta o excepción anterior por un error al liberar.
        try:
            await run_in_threadpool(liberar_conversacion, sid, token)
        except Exception as exc:
            logger.error(
                "No se pudo liberar la conversación: %s",
                type(exc).__name__,
            )


app.mount(
    "/",
    StaticFiles(directory=str(FRONTEND_PATH), html=True),
    name="frontend",
)

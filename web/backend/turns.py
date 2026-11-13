import logging
from contextvars import ContextVar
from time import monotonic

logger = logging.getLogger(__name__)
LIMITE_TURNO = 60
LIMITE_RESERVA = 70
_limite = ContextVar("limite_turno", default=None)


class TiempoConsulta(TimeoutError):
    pass


def comprobar_plazo():
    limite = _limite.get()
    if limite is not None and monotonic() >= limite:
        raise TiempoConsulta("El turno agotó su tiempo disponible.")


def medir_etapa(nombre, funcion, *args, **kwargs):
    comprobar_plazo()
    inicio = monotonic()
    try:
        resultado = funcion(*args, **kwargs)
        comprobar_plazo()
        return resultado
    finally:
        segundos = monotonic() - inicio
        registrar = logger.warning if segundos >= 5 else logger.info
        registrar("Etapa del chat: %s; duración: %.2fs", nombre, segundos)


def ejecutar_turno(asistente, entrada, config, limite):
    # El trabajador solo calcula: guardar el turno corresponde al endpoint
    token = _limite.set(limite)
    try:
        comprobar_plazo()
        resultado = asistente.invoke(entrada, config=config)
        comprobar_plazo()
        return resultado
    finally:
        _limite.reset(token)

import logging
import math
from datetime import date, datetime
from decimal import Decimal

import boto3
import pyarrow.dataset as ds
import pyarrow.fs as fs

from backend.config import (
    AWS_REGION,
    S3_BUCKET,
    S3_CUSTOMER_PATH,
    S3_MONTHLY_PATH
)


logger = logging.getLogger(__name__)


# Seleccionar información financiera
CUSTOMER_COLUMNS = [
    "id",
    "current_age",
    "retirement_age",
    "yearly_income",
    "per_capita_income",
    "total_debt",
    "credit_score",
    "num_credit_cards",
    "total_cards",
    "total_cards_without_id",
    "average_credit_limit",
    "total_credit_limit",
    "total_credit_limits_imputed",
    "has_imputed_credit_limit",
    "total_transactions",
    "total_spent",
    "average_transaction",
    "minimum_transaction",
    "maximum_transaction",
    "total_merchant_cities",
    "has_card_records",
    "has_transaction_records",
    "spending_to_income_ratio",
    "debt_to_income_ratio",
    "currency",
    "financial_cutoff_date",
    "account_balance",
    "monthly_income",
    "monthly_expenses",
    "monthly_debt_payments",
    "monthly_savings",
    "outstanding_due",
    "max_days_past_due",
    "prediction_month",
    "predicted_income",
    "has_income_prediction"
]


class ErrorDatos(Exception):
    pass


class ClienteDuplicado(Exception):
    pass


# Crear conexión con S3
def crear_filesystem():
    session = boto3.Session(region_name=AWS_REGION)
    credentials = session.get_credentials()

    if credentials is None:
        raise ErrorDatos("No se encontraron credenciales AWS.")

    credentials = credentials.get_frozen_credentials()

    return fs.S3FileSystem(
        access_key=credentials.access_key,
        secret_key=credentials.secret_key,
        session_token=credentials.token,
        region=AWS_REGION,
        connect_timeout=10,
        request_timeout=30
    )


# Preparar valores para JSON
def preparar_valor(valor):
    if isinstance(valor, (date, datetime)):
        return valor.isoformat()

    if isinstance(valor, Decimal):
        valor = float(valor)

    if isinstance(valor, float) and not math.isfinite(valor):
        return None

    return valor


# Consultar información del cliente
def consultar_cliente(cliente_id: int) -> dict:
    try:
        filesystem = crear_filesystem()

        dataset = ds.dataset(
            f"{S3_BUCKET}/{S3_CUSTOMER_PATH}",
            filesystem=filesystem,
            format="parquet",
            exclude_invalid_files=True,
            ignore_prefixes=[".", "_"]
        )

        missing_columns = set(CUSTOMER_COLUMNS) - set(
            dataset.schema.names
        )

        if missing_columns:
            logger.error(
                "Faltan columnas en GOLD: %s",
                sorted(missing_columns)
            )
            raise ErrorDatos(
                "El resumen GOLD no tiene el esquema esperado."
            )

        registros = (
            dataset.scanner(
                columns=CUSTOMER_COLUMNS,
                filter=ds.field("id") == cliente_id
            )
            .head(2)
            .to_pylist()
        )

    except ErrorDatos:
        raise
    except Exception as exc:
        logger.exception("No se pudo consultar el resumen GOLD.")
        raise ErrorDatos(
            "No se pudo consultar la información en S3."
        ) from exc

    if not registros:
        return {}

    if len(registros) > 1:
        raise ClienteDuplicado(
            "Existe más de un resumen para el mismo cliente."
        )

    return {
        campo: preparar_valor(valor)
        for campo, valor in registros[0].items()
    }

# Entregar al navegador solo los indicadores que muestra la interfaz
PUBLIC_COLUMNS = [
    "total_credit_limit", "total_transactions", "total_cards",
    "total_spent", "total_debt", "yearly_income", "has_card_records",
    "has_imputed_credit_limit", "total_credit_limits_imputed"
]


def resumen_publico(datos):
    return {campo: datos.get(campo) for campo in PUBLIC_COLUMNS}


# Aplicar el filtro de cliente antes de recuperar su historial
def consultar_historial(cliente_id):
    columnas = [
        "month", "currency", "monthly_income", "monthly_expenses",
        "monthly_debt_payments", "monthly_transactions",
        "closing_balance", "outstanding_due"
    ]
    try:
        dataset = ds.dataset(
            f"{S3_BUCKET}/{S3_MONTHLY_PATH}",
            filesystem=crear_filesystem(),
            format="parquet",
            exclude_invalid_files=True,
            ignore_prefixes=[".", "_"]
        )
        if not set(columnas + ["client_id"]).issubset(dataset.schema.names):
            raise ErrorDatos("Historial con esquema incompatible.")

        # El modelo no proporciona identificadores ni filtros.
        tabla = dataset.scanner(
            columns=columnas,
            filter=ds.field("client_id") == cliente_id
        ).head(2401)

        if tabla.num_rows > 2400:
            raise ErrorDatos("Historial superior al límite del prototipo.")

        registros = tabla.to_pylist()
        claves = [(str(r["month"]), r["currency"]) for r in registros]
        if len(claves) != len(set(claves)):
            raise ErrorDatos("Historial duplicado.")

        return sorted(
            [
                {campo: preparar_valor(valor) for campo, valor in fila.items()}
                for fila in registros
            ],
            key=lambda fila: (fila["month"] or "", fila["currency"] or "")
        )
    except ErrorDatos:
        raise
    except Exception as exc:
        logger.exception("Error al recuperar historial.")
        raise ErrorDatos("No se pudo recuperar el historial.") from exc


# Cache breve por cliente. No compartir valores entre identificadores.
# Limitar memoria y permitir que los datos nuevos se vean tras 30 segundos.
from collections import OrderedDict
from copy import deepcopy
from functools import wraps
from threading import RLock
from time import monotonic

_cache = OrderedDict()
_cache_lock = RLock()


def cache_cliente(nombre):
    def decorar(funcion):
        @wraps(funcion)
        def consultar(cliente_id):
            if type(cliente_id) is not int or cliente_id < 0:
                raise ErrorDatos("Identificador inválido.")
            clave = (nombre, cliente_id)
            ahora = monotonic()
            with _cache_lock:
                entrada = _cache.get(clave)
                if entrada and entrada[0] > ahora:
                    _cache.move_to_end(clave)
                    return deepcopy(entrada[1])
                _cache.pop(clave, None)

            # No mantener el bloqueo mientras se consulta el almacenamiento.
            resultado = funcion(cliente_id)
            with _cache_lock:
                _cache[clave] = (monotonic() + 30, deepcopy(resultado))
                _cache.move_to_end(clave)
                while len(_cache) > 64:
                    _cache.popitem(last=False)
            return resultado
        return consultar
    return decorar


consultar_cliente = cache_cliente("perfil")(consultar_cliente)
consultar_historial = cache_cliente("historial")(consultar_historial)




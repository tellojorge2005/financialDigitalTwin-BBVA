import logging
import math
from decimal import Decimal

import boto3
import pyarrow.dataset as ds
import pyarrow.fs as fs

from backend.config import (
    AWS_REGION,
    S3_BUCKET,
    S3_CUSTOMER_PATH
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
    "debt_to_income_ratio"
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
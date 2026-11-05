import logging
from calendar import monthrange
from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from functools import wraps
import math
from threading import RLock
from time import monotonic

import boto3
import pyarrow.dataset as ds
import pyarrow.fs as fs

from backend.config import (
    AWS_REGION, S3_BUCKET, S3_CUSTOMER_PATH, S3_MONTHLY_PATH,
    S3_SCENARIOS_PATH, S3_CATEGORIES_PATH,
)

from backend.turns import comprobar_plazo, TiempoConsulta, medir_etapa

logger = logging.getLogger(__name__)

# Seleccionar únicamente información que utiliza el asistente
CUSTOMER_COLUMNS = [
    "id", "full_name", "email", "currency", "financial_cutoff_date",
    "current_age", "retirement_age", "yearly_income", "per_capita_income",
    "total_debt", "credit_score", "num_credit_cards", "total_cards",
    "total_cards_without_id", "average_credit_limit", "total_credit_limit",
    "total_credit_limits_imputed", "has_imputed_credit_limit",
    "total_transactions", "total_spent", "average_transaction",
    "minimum_transaction", "maximum_transaction", "total_merchant_cities",
    "has_card_records", "has_transaction_records", "spending_to_income_ratio",
    "debt_to_income_ratio", "account_balance", "monthly_income",
    "monthly_expenses", "monthly_debt_payments", "monthly_savings",
    "outstanding_due", "max_days_past_due", "prediction_month",
    "predicted_income", "has_income_prediction", "prediction_validation_status",
    "months_available_3m", "period_start", "income_3m", "expenses_3m",
    "debt_payments_3m", "savings_3m", "average_income_3m",
    "average_expenses_3m", "savings_rate_3m", "debt_payment_ratio_3m",
    "expense_coverage_months", "has_overdue_payments", "has_negative_balance",
    "has_budget_deficit_3m", "financial_health_status",
]
PUBLIC_COLUMNS = [
    "full_name", "email", "currency", "financial_cutoff_date",
    "total_credit_limit", "total_transactions", "total_cards", "total_spent",
    "total_debt", "yearly_income", "has_card_records",
    "has_imputed_credit_limit", "total_credit_limits_imputed",
]
MONTHLY_COLUMNS = [
    "month", "currency", "monthly_income", "monthly_expenses",
    "monthly_debt_payments", "monthly_transactions", "closing_balance",
    "outstanding_due", "max_days_past_due", "monthly_savings",
    "transfers_in", "transfers_out",
]
SCENARIO_COLUMNS = [
    "currency", "feature_month", "prediction_month", "horizon_months", "status",
    "income_p10", "income_p50", "income_p90", "expenses_p10", "expenses_p50",
    "expenses_p90", "balance_p10", "balance_p50", "balance_p90",
    "base_debt_payment", "initial_balance", "prob_negative_balance",
]


class ErrorDatos(Exception):
    pass


class ClienteDuplicado(Exception):
    pass


# Cache breve y acotada; cada cliente conserva su propia entrada
_cache = OrderedDict()
_lock = RLock()
_datasets = OrderedDict()
CACHE_SECONDS = 120


def cache_cliente(nombre):
    def decorar(funcion):
        @wraps(funcion)
        def consultar(cliente_id):
            comprobar_plazo()
            if type(cliente_id) is not int or cliente_id < 0:
                raise ErrorDatos("Identificador inválido.")
            clave = (nombre, cliente_id)
            with _lock:
                entrada = _cache.get(clave)
                if entrada and entrada[0] > monotonic():
                    _cache.move_to_end(clave)
                    return deepcopy(entrada[1])
                _cache.pop(clave, None)
            resultado = medir_etapa(nombre, funcion, cliente_id)
            with _lock:
                _cache[clave] = (monotonic() + CACHE_SECONDS, deepcopy(resultado))
                while len(_cache) > 128:
                    _cache.popitem(last=False)
            return resultado
        return consultar
    return decorar


def crear_filesystem():
    credenciales = boto3.Session(region_name=AWS_REGION).get_credentials()
    if credenciales is None:
        raise ErrorDatos("No se encontraron credenciales AWS.")
    credenciales = credenciales.get_frozen_credentials()
    return fs.S3FileSystem(
        access_key=credenciales.access_key, secret_key=credenciales.secret_key,
        session_token=credenciales.token, region=AWS_REGION,
        connect_timeout=5, request_timeout=15,
    )


def preparar_valor(valor):
    if isinstance(valor, (date, datetime)):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        valor = float(valor)
    if isinstance(valor, float) and not math.isfinite(valor):
        return None
    return valor


# El filtro se construye en el servidor; nunca se recibe una ruta del modelo
def leer_filas(ruta, columnas, requeridas, filtro, limite, opcional=False):
    try:
        comprobar_plazo()
        dataset = abrir_dataset(ruta, opcional)
        if dataset is None:
            return []
        if not set(requeridas).issubset(dataset.schema.names):
            raise ErrorDatos("El archivo no tiene el esquema esperado.")
        disponibles = [c for c in columnas if c in dataset.schema.names]
        tabla = dataset.scanner(columns=disponibles, filter=filtro).head(limite + 1)
        comprobar_plazo()
        if tabla.num_rows > limite:
            raise ErrorDatos("La consulta supera el límite esperado.")
        return [
            {c: preparar_valor(fila.get(c)) for c in columnas}
            for fila in tabla.to_pylist()
        ]
    except (ErrorDatos, TiempoConsulta):
        raise
    except Exception as exc:
        logger.error("No se pudo consultar GOLD: %s", type(exc).__name__)
        raise ErrorDatos("No se pudo consultar la información financiera.") from exc


def perfil_unico(filas):
    if len(filas) > 1:
        raise ClienteDuplicado("Más de un perfil coincide con la consulta.")
    if not filas:
        return {}
    perfil = filas[0]
    if type(perfil.get("id")) is not int or perfil["id"] < 0:
        raise ErrorDatos("Identificador del perfil inválido.")
    if not perfil.get("full_name") or not perfil.get("email"):
        raise ErrorDatos("El perfil no tiene nombre o correo.")
    if perfil.get("currency") not in (None, "MXN"):
        raise ErrorDatos("El perfil utiliza una moneda diferente.")
    return perfil


def consultar_por_correo(correo):
    # Silver ya normaliza el correo a minúsculas y valida su unicidad
    normalizado = correo.strip().lower()
    filas = leer_filas(
        S3_CUSTOMER_PATH, CUSTOMER_COLUMNS, ["id", "full_name", "email"],
        ds.field("email") == normalizado, 2,
    )
    return perfil_unico(filas)


@cache_cliente("perfil")
def consultar_cliente(cliente_id):
    return perfil_unico(leer_filas(
        S3_CUSTOMER_PATH, CUSTOMER_COLUMNS, ["id", "full_name", "email"],
        ds.field("id") == cliente_id, 2,
    ))


def resumen_publico(datos):
    return {c: datos.get(c) for c in PUBLIC_COLUMNS}


@cache_cliente("historial")
def consultar_historial(cliente_id):
    filas = leer_filas(
        S3_MONTHLY_PATH, MONTHLY_COLUMNS,
        ["client_id", *MONTHLY_COLUMNS[:8]],
        ds.field("client_id") == cliente_id, 2400,
    )
    claves = set()
    for fila in filas:
        try:
            mes = date.fromisoformat(fila["month"])
        except (TypeError, ValueError):
            raise ErrorDatos("Fecha mensual inválida.") from None
        if mes.day != 1 or fila["currency"] != "MXN" or mes in claves:
            raise ErrorDatos("Clave mensual inválida o duplicada.")
        claves.add(mes)
        for campo in ["monthly_income", "monthly_expenses", "monthly_debt_payments"]:
            valor = fila.get(campo)
            if not isinstance(valor, (float, int)) or not math.isfinite(valor) or valor < 0:
                raise ErrorDatos("Importe mensual inválido.")
    return sorted(filas, key=lambda f: f["month"])


def meses_completos(perfil, historial):
    corte = perfil.get("financial_cutoff_date")
    if not corte:
        return []
    try:
        limite = min(date.fromisoformat(str(corte)[:10]), date.today())
    except ValueError:
        raise ErrorDatos("Corte financiero inválido.") from None
    return [fila for fila in historial if date.fromisoformat(fila["month"]).replace(
        day=monthrange(int(fila["month"][:4]), int(fila["month"][5:7]))[1]
    ) <= limite]


@cache_cliente("categorias")
def consultar_categorias(cliente_id):
    perfil = consultar_cliente(cliente_id)
    meses = meses_completos(perfil, consultar_historial(cliente_id))[-3:]
    if not meses:
        return []
    return leer_filas(
        S3_CATEGORIES_PATH, ["month", "currency", "category", "total_expenses"],
        ["client_id", "month", "currency", "category", "total_expenses"],
        (ds.field("client_id") == cliente_id)
        & (ds.field("month") >= date.fromisoformat(meses[0]["month"]))
        & (ds.field("month") <= date.fromisoformat(meses[-1]["month"])),
        50000, opcional=True,
    )

@cache_cliente("proyecciones")
def consultar_proyecciones(cliente_id):
    perfil = consultar_cliente(cliente_id)
    historial = meses_completos(perfil, consultar_historial(cliente_id))
    if not historial:
        return []
    corte = historial[-1]["month"]
    filas = leer_filas(
        S3_SCENARIOS_PATH, SCENARIO_COLUMNS,
        ["client_id", *SCENARIO_COLUMNS[:8], "expenses_p50"],
        (ds.field("client_id") == cliente_id)
        & (ds.field("feature_month") == date.fromisoformat(corte)),
        3, opcional=True,
    )
    if not filas:
        from backend.finances import simular_proyecciones
        return simular_proyecciones(cliente_id, historial)
    claves = set()
    for fila in filas:
        horizonte = fila["horizon_months"]
        mes = date.fromisoformat(corte)
        numero = mes.year * 12 + mes.month - 1 + (horizonte or 0)
        esperado = date(numero // 12, numero % 12 + 1, 1).isoformat()
        if (type(horizonte) is not int or horizonte not in (1, 2, 3)
                or horizonte in claves or fila["currency"] != "MXN"
                or fila["prediction_month"] != esperado):
            raise ErrorDatos("Proyección incompatible con el corte.")
        claves.add(horizonte)
        if fila["status"] == "GENERADO":
            for campo in ["income_p50", "expenses_p50"]:
                valor = fila[campo]
                if not isinstance(valor, (int, float)) or not math.isfinite(valor) or valor < 0:
                    raise ErrorDatos("Proyección inválida.")
    return sorted(filas, key=lambda f: f["horizon_months"])


def panorama_publico(cliente_id, perfil):
    meses = meses_completos(perfil, consultar_historial(cliente_id))[-3:]
    try:
        escenarios = consultar_proyecciones(cliente_id)
    except (ErrorDatos, TiempoConsulta):
        # Un escenario no disponible no impide consultar el resumen y el chat
        logger.warning("Proyección temporalmente no disponible.")
        escenarios = []
    campos = ["feature_month", "prediction_month", "horizon_months", "currency",
              "status", "income_p50", "expenses_p50"]
    return {
        "monthly_history": [
            {c: fila[c] for c in ["month", "currency", "monthly_income", "monthly_expenses"]}
            for fila in meses
        ],
        "cashflow_scenarios": [{c: f.get(c) for c in campos} for f in escenarios],
    }


def abrir_dataset(ruta, opcional=False):
    # Compartir metadatos brevemente; los filtros de cliente siguen en cada lectura
    comprobar_plazo()
    with _lock:
        entrada = _datasets.get(ruta)
        if entrada and entrada[0] > monotonic():
            return entrada[1]
    filesystem = crear_filesystem()
    ubicacion = f"{S3_BUCKET}/{ruta}"
    if opcional and filesystem.get_file_info(ubicacion).type == fs.FileType.NotFound:
        dataset = None
    else:
        dataset = ds.dataset(
            ubicacion, filesystem=filesystem, format="parquet",
            exclude_invalid_files=True, ignore_prefixes=[".", "_"],
        )
    comprobar_plazo()
    with _lock:
        _datasets[ruta] = (monotonic() + CACHE_SECONDS, dataset)
        _datasets.move_to_end(ruta)
        while len(_datasets) > 8:
            _datasets.popitem(last=False)
    return dataset

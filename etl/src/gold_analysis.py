import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    round,
    countDistinct,
    when
)
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Cargar variables de entorno
# En local: <proyecto>/etl/src. En Docker: /opt/project/src.
PROJECT_PATH = Path(__file__).resolve().parent.parent
if PROJECT_PATH.name == "etl":
    PROJECT_PATH = PROJECT_PATH.parent

load_dotenv(PROJECT_PATH / ".env", override=False)


# Rutas S3
BUCKET = os.getenv("S3_BUCKET", "").strip()

if not BUCKET:
    raise RuntimeError("Configura S3_BUCKET en .env.")

INPUT_PATH = f"s3a://{BUCKET}/data/silver"
OUTPUT_PATH = f"s3a://{BUCKET}/data/gold"


# Crear resumen de tarjetas
def create_cards_summary(cards_spark):
    return (
        cards_spark
        .groupBy("client_id")
        .agg(
            count("*").alias("total_cards"),
            count("id").alias("total_cards_with_id"),
            countDistinct("card_brand").alias("total_card_brands"),
            countDistinct("card_type").alias("total_card_types"),
            round(avg("credit_limit"), 2).alias("average_credit_limit"),
            round(sum("credit_limit"), 2).alias("total_credit_limit"),
            sum(
                col("credit_limit_imputed").cast("int")
            ).alias("total_credit_limits_imputed")
        )
        .withColumn(
            "total_cards_without_id",
            col("total_cards") - col("total_cards_with_id")
        )
        .withColumn(
            "has_imputed_credit_limit",
            col("total_credit_limits_imputed") > 0
        )
    )


# Crear resumen de transacciones
def create_transactions_summary(transactions_spark):
    return (
        transactions_spark
        .groupBy("client_id")
        .agg(
            count("id").alias("total_transactions"),
            round(sum("amount"), 2).alias("total_spent"),
            round(avg("amount"), 2).alias("average_transaction"),
            round(min("amount"), 2).alias("minimum_transaction"),
            round(max("amount"), 2).alias("maximum_transaction"),
            countDistinct("merchant_city").alias("total_merchant_cities")
        )
    )


# Crear información financiera
def create_users_summary(users_spark):
    # Crear información financiera

    users_gold = users_spark.select(
        "id",
        "full_name",
        "email",
        "city",
        "state",
        "country",
        "postal_code",
        "current_age",
        "retirement_age",
        "gender",
        "yearly_income",
        "per_capita_income",
        "total_debt",
        "credit_score",
        "num_credit_cards",
        F.col("currency").alias("profile_currency")
    )
    return users_gold


def create_customer_summary(users_gold, cards_gold, transactions_gold):
    # Unir información
    customer_gold = (
        users_gold
        .join(
            cards_gold,
            users_gold.id == cards_gold.client_id,
            "left"
        )
        .drop(cards_gold.client_id)
        .join(
            transactions_gold,
            users_gold.id == transactions_gold.client_id,
            "left"
        )
        .drop(transactions_gold.client_id)
    )

    # Identificar información disponible
    customer_gold = (
        customer_gold
        .withColumn(
            "has_card_records",
            col("total_cards").isNotNull()
        )
        .withColumn(
            "has_transaction_records",
            col("total_transactions").isNotNull()
        )
    )

    # Crear métricas financieras
    customer_gold = (
        customer_gold
        .withColumn(
            "spending_to_income_ratio",
            when(
                col("yearly_income").isNotNull()
                & (col("yearly_income") != 0),
                round(
                    col("total_spent") / col("yearly_income"),
                    4
                )
            )
        )
        .withColumn(
            "debt_to_income_ratio",
            when(
                col("yearly_income").isNotNull()
                & (col("yearly_income") != 0),
                round(
                    col("total_debt") / col("yearly_income"),
                    4
                )
            )
        )
    )

    return customer_gold

# Definir tipos permitidos
TIPOS_ENTEROS = (
    T.ByteType,
    T.ShortType,
    T.IntegerType,
    T.LongType
)

TIPOS_NUMERICOS = TIPOS_ENTEROS + (
    T.FloatType,
    T.DoubleType,
    T.DecimalType
)

TIPOS_BOOLEANOS = (T.BooleanType,)
TIPOS_TEXTO = (T.StringType,)
TIPOS_FECHA = (T.TimestampType, T.DateType)


# Definir tolerancias
MAX_PCT_FECHAS_NULAS = 6.0


class ErrorCalidadDatos(ValueError):
    pass


# Validar únicamente valores disponibles
def fuera_de_rango(columna, minimo, maximo=None):
    valor = F.col(columna)
    error = valor < minimo

    if maximo is not None:
        error = error | (valor > maximo)

    return valor.isNotNull() & error


# Detectar decimales cuando se espera un valor entero
def tiene_decimales(columna):
    valor = F.col(columna)

    return (
        valor.isNotNull()
        & (valor != F.floor(valor))
    )


# Validar una tabla y mostrar resultados
def validar_tabla(nombre, df, tipos, reglas, clave):
    esquema = {
        campo.name: campo.dataType
        for campo in df.schema.fields
    }

    errores_esquema = []

    for columna, tipos_permitidos in tipos.items():
        if columna not in esquema:
            errores_esquema.append(
                f"Falta la columna {columna}"
            )
        elif not isinstance(esquema[columna], tipos_permitidos):
            esperados = ", ".join(
                tipo.__name__
                for tipo in tipos_permitidos
            )

            errores_esquema.append(
                f"{columna}: tipo {esquema[columna].simpleString()}, "
                f"se esperaba {esperados}"
            )

    if errores_esquema:
        raise ErrorCalidadDatos(
            f"{nombre}: esquema incorrecto.\n"
            + "\n".join(errores_esquema)
        )

    reglas = list(reglas)

    # Validar identificador
    reglas.append((
        f"{clave}: nulo o negativo",
        F.col(clave).isNull() | (F.col(clave) < 0),
        0.0
    ))

    # Detectar valores numéricos no finitos
    for columna, tipo in esquema.items():
        if isinstance(tipo, TIPOS_NUMERICOS):
            valor = F.col(columna).cast("double")

            reglas.append((
                f"{columna}: NaN o infinito",
                F.isnan(valor)
                | (F.abs(valor) == F.lit(float("inf"))),
                0.0
            ))

    expresiones = [
        F.count(F.lit(1)).alias("total"),
        F.count(clave).alias("ids_presentes"),
        F.countDistinct(clave).alias("ids_distintos")
    ]

    for indice, (_, condicion_error, _) in enumerate(reglas):
        expresiones.append(
            F.sum(
                F.when(
                    F.coalesce(condicion_error, F.lit(False)),
                    1
                ).otherwise(0)
            ).alias(f"error_{indice}")
        )

    resultado = df.agg(*expresiones).first()
    total = resultado["total"]

    if total == 0:
        raise ErrorCalidadDatos(
            f"{nombre}: el dataset está vacío."
        )

    mediciones = [
        (
            nombre_regla,
            int(resultado[f"error_{indice}"] or 0),
            tolerancia
        )
        for indice, (
            nombre_regla,
            _,
            tolerancia
        ) in enumerate(reglas)
    ]

    # Contar repeticiones adicionales de IDs no nulos
    duplicados = (
        resultado["ids_presentes"]
        - resultado["ids_distintos"]
    )

    mediciones.append((
        f"{clave}: duplicados adicionales",
        int(duplicados),
        0.0
    ))

    fallos = []

    print(f"\nCalidad de {nombre}: {total} registros")

    for nombre_regla, errores, tolerancia in mediciones:
        porcentaje = errores * 100.0 / total
        cumple = porcentaje <= tolerancia
        estado = "OK" if cumple else "ERROR"

        print(
            f"[{estado}] {nombre_regla}: "
            f"{errores}/{total} ({porcentaje:.2f} %), "
            f"máximo {tolerancia:.2f} %"
        )

        if not cumple:
            fallos.append(
                f"{nombre_regla}: {porcentaje:.2f} % "
                f"> {tolerancia:.2f} %"
            )

    if fallos:
        raise ErrorCalidadDatos(
            f"{nombre}: calidad rechazada.\n"
            + "\n".join(fallos)
        )

    return total


# Validar referencias a clientes existentes
def validar_clientes(nombre, df, users_spark):
    clientes = users_spark.select(
        F.col("id").alias("client_id")
    )

    sin_cliente = (
        df.select("client_id")
        .join(clientes, "client_id", "left_anti")
        .count()
    )

    if sin_cliente:
        raise ErrorCalidadDatos(
            f"{nombre}: {sin_cliente} registros "
            "sin un cliente existente."
        )


# Validar SILVER antes de construir GOLD
def validar_entrada_gold(
    users_spark,
    cards_spark,
    transactions_spark
):
    tipos_users = {
        "id": TIPOS_ENTEROS,
        "current_age": TIPOS_NUMERICOS,
        "retirement_age": TIPOS_NUMERICOS,
        "gender": TIPOS_TEXTO,
        "yearly_income": TIPOS_NUMERICOS,
        "per_capita_income": TIPOS_NUMERICOS,
        "total_debt": TIPOS_NUMERICOS,
        "credit_score": TIPOS_NUMERICOS,
        "num_credit_cards": TIPOS_NUMERICOS
    }

    reglas_users = []

    for columna in [
        "current_age",
        "retirement_age",
        "credit_score",
        "num_credit_cards"
    ]:
        reglas_users.append((
            f"{columna}: debe ser un valor entero",
            tiene_decimales(columna),
            0.0
        ))

    for columna, minimo, maximo in [
        ("current_age", 18, 120),
        ("retirement_age", 0, 120),
        ("credit_score", 300, 850),
        ("num_credit_cards", 0, None),
        ("yearly_income", 0, None),
        ("per_capita_income", 0, None),
        ("total_debt", 0, None)
    ]:
        reglas_users.append((
            f"{columna}: fuera de rango",
            fuera_de_rango(columna, minimo, maximo),
            0.0
        ))

    # Límite propuesto para las etiquetas actuales de gender
    reglas_users.append((
        "gender: vacío o mayor de 10 caracteres",
        F.col("gender").isNotNull()
        & (
            (F.length(F.trim(F.col("gender"))) == 0)
            | (F.length(F.col("gender")) > 10)
        ),
        0.0
    ))

    total_users = validar_tabla(
        "users_silver",
        users_spark,
        tipos_users,
        reglas_users,
        "id"
    )

    validar_tabla(
        "cards_silver",
        cards_spark,
        {
            "id": TIPOS_ENTEROS,
            "client_id": TIPOS_ENTEROS,
            "card_brand": TIPOS_TEXTO,
            "card_type": TIPOS_TEXTO,
            "credit_limit": TIPOS_NUMERICOS,
            "credit_limit_imputed": TIPOS_BOOLEANOS
        },
        [
            (
                "client_id: nulo o negativo",
                F.col("client_id").isNull()
                | (F.col("client_id") < 0),
                0.0
            ),
            (
                "credit_limit: nulo o negativo",
                F.col("credit_limit").isNull()
                | (F.col("credit_limit") < 0),
                0.0
            ),
            (
                "credit_limit_imputed: nulo",
                F.col("credit_limit_imputed").isNull(),
                0.0
            )
        ],
        "id"
    )

    validar_tabla(
        "transactions_silver",
        transactions_spark,
        {
            "id": TIPOS_ENTEROS,
            "client_id": TIPOS_ENTEROS,
            "amount": TIPOS_NUMERICOS,
            "date": TIPOS_FECHA,
            "merchant_city": TIPOS_TEXTO
        },
        [
            (
                "client_id: nulo o negativo",
                F.col("client_id").isNull()
                | (F.col("client_id") < 0),
                0.0
            ),
            (
                "amount: nulo",
                F.col("amount").isNull(),
                0.0
            ),
            (
                "date: nula",
                F.col("date").isNull(),
                MAX_PCT_FECHAS_NULAS
            )
        ],
        "id"
    )

    validar_clientes(
        "cards_silver",
        cards_spark,
        users_spark
    )

    validar_clientes(
        "transactions_silver",
        transactions_spark,
        users_spark
    )

    return total_users

# Validar resumen final
def validar_salida_gold(customer_gold, total_users):
    conteos = [
        "total_cards",
        "total_cards_with_id",
        "total_cards_without_id",
        "total_card_brands",
        "total_card_types",
        "total_credit_limits_imputed",
        "total_transactions",
        "total_merchant_cities"
    ]

    importes = [
        "average_credit_limit",
        "total_credit_limit",
        "total_spent",
        "average_transaction",
        "minimum_transaction",
        "maximum_transaction",
        "spending_to_income_ratio",
        "debt_to_income_ratio"
    ]

    tipos = {
        "id": TIPOS_ENTEROS,
        "has_card_records": TIPOS_BOOLEANOS,
        "has_transaction_records": TIPOS_BOOLEANOS,
        "has_imputed_credit_limit": TIPOS_BOOLEANOS
    }

    tipos.update({
        columna: TIPOS_ENTEROS
        for columna in conteos
    })

    tipos.update({
        columna: TIPOS_NUMERICOS
        for columna in importes
    })

    reglas = [
        (
            f"{columna}: negativo",
            fuera_de_rango(columna, 0),
            0.0
        )
        for columna in conteos
    ]

    # Comprobar presencia de resúmenes y sus campos
    grupos = [
        (
            "has_card_records",
            "total_cards",
            [
                "total_cards",
                "total_cards_with_id",
                "total_cards_without_id",
                "total_card_brands",
                "total_card_types",
                "total_credit_limits_imputed",
                "average_credit_limit",
                "total_credit_limit",
                "has_imputed_credit_limit"
            ]
        ),
        (
            "has_transaction_records",
            "total_transactions",
            [
                "total_transactions",
                "total_merchant_cities",
                "total_spent",
                "average_transaction",
                "minimum_transaction",
                "maximum_transaction"
            ]
        )
    ]

    for indicador, cantidad, columnas in grupos:
        reglas.append((
            f"{indicador}: inconsistente",
            ~F.col(indicador).eqNullSafe(
                F.col(cantidad).isNotNull()
            ),
            0.0
        ))

        reglas.append((
            f"{cantidad}: debe ser positivo si hay registros",
            F.col(indicador)
            & (F.col(cantidad) <= 0),
            0.0
        ))

        for columna in columnas:
            reglas.append((
                f"{columna}: disponibilidad inconsistente",
                ~F.col(columna).isNotNull().eqNullSafe(
                    F.col(indicador)
                ),
                0.0
            ))

    reglas.extend([
        (
            "tarjetas: conteos inconsistentes",
            F.col("has_card_records")
            & ~F.col("total_cards").eqNullSafe(
                F.col("total_cards_with_id")
                + F.col("total_cards_without_id")
            ),
            0.0
        ),
        (
            "tarjetas: límites estimados inconsistentes",
            F.col("has_card_records")
            & (
                (
                    F.col("total_credit_limits_imputed")
                    > F.col("total_cards")
                )
                | ~F.col("has_imputed_credit_limit").eqNullSafe(
                    F.col("total_credit_limits_imputed") > 0
                )
            ),
            0.0
        ),
        (
            "transacciones: mínimo, promedio y máximo inconsistentes",
            F.col("has_transaction_records")
            & (
                (
                    F.col("minimum_transaction")
                    > F.col("average_transaction")
                )
                | (
                    F.col("average_transaction")
                    > F.col("maximum_transaction")
                )
            ),
            0.0
        )
    ])

    for columna, limite in [
        ("total_card_brands", "total_cards"),
        ("total_card_types", "total_cards"),
        ("total_merchant_cities", "total_transactions")
    ]:
        reglas.append((
            f"{columna}: mayor que {limite}",
            F.col(columna) > F.col(limite),
            0.0
        ))

    # Validar ratios con el mismo redondeo del pipeline
    for metrica, numerador in [
        ("spending_to_income_ratio", "total_spent"),
        ("debt_to_income_ratio", "total_debt")
    ]:
        esperado = F.when(
            F.col("yearly_income").isNotNull()
            & (F.col("yearly_income") != 0),
            F.round(
                F.col(numerador) / F.col("yearly_income"),
                4
            )
        )

        reglas.append((
            f"{metrica}: cálculo inconsistente",
            ~F.col(metrica).eqNullSafe(esperado),
            0.0
        ))

    total_gold = validar_tabla(
        "customer_gold",
        customer_gold,
        tipos,
        reglas,
        "id"
    )

    if total_gold != total_users:
        raise ErrorCalidadDatos(
            f"GOLD contiene {total_gold} clientes; "
            f"se esperaban {total_users}."
        )


def create_monthly_summaries(accounts_spark, obligations_spark, movements_spark):
    # El historial actual contiene cuentas abiertas al inicio del mes
    if accounts_spark.filter(F.dayofmonth("opened_at") != 1).limit(1).count():
        raise ValueError(
            "Hay cuentas abiertas a mitad de mes. "
            "Se requiere incorporar el saldo del primer mes parcial."
        )

    # Definir meses completos disponibles

    accounts_calendar = (
        accounts_spark
        .withColumn(
            "first_month",
            F.when(
                F.dayofmonth("opened_at") == 1,
                F.trunc(F.to_date("opened_at"), "month")
            ).otherwise(
                F.add_months(
                    F.trunc(F.to_date("opened_at"), "month"),
                    1
                )
            )
        )
        .withColumn(
            "last_month",
            F.when(
                F.to_date("balance_as_of")
                == F.last_day("balance_as_of"),
                F.trunc(F.to_date("balance_as_of"), "month")
            ).otherwise(
                F.add_months(
                    F.trunc(F.to_date("balance_as_of"), "month"),
                    -1
                )
            )
        )
        .filter(F.col("first_month") <= F.col("last_month"))
        .withColumn(
            "month",
            F.explode(
                F.sequence(
                    "first_month",
                    "last_month",
                    F.expr("interval 1 month")
                )
            )
        )
        .select(
            "account_id",
            "client_id",
            "currency",
            "opening_balance",
            "month"
        )
    )


    # Preparar movimientos mensuales

    movements_monthly = movements_spark.withColumn(
        "month",
        F.trunc(F.to_date("date"), "month")
    )


    # Agregar flujos por cuenta

    def sumar_movimiento(tipo, nombre, signo=1):
        return F.sum(
            F.when(
                F.col("movement_type") == tipo,
                F.col("amount") * signo
            ).otherwise(0)
        ).alias(nombre)


    account_flows = (
        movements_monthly
        .groupBy("account_id", "month")
        .agg(
            F.count("*").alias("monthly_transactions"),
            sumar_movimiento("income", "monthly_income"),
            sumar_movimiento("expense", "monthly_expenses", -1),
            sumar_movimiento(
                "debt_payment",
                "monthly_debt_payments",
                -1
            ),
            sumar_movimiento("transfer_in", "transfers_in"),
            sumar_movimiento("transfer_out", "transfers_out", -1),
            F.sum("amount").alias("monthly_net_flow"),
            F.max(
                F.struct("date", "id", "balance_after")
            ).alias("last_movement")
        )
        .withColumn(
            "observed_balance",
            F.col("last_movement.balance_after")
        )
        .drop("last_movement")
    )


    # Completar meses cubiertos

    columnas_flujo = [
        "monthly_transactions",
        "monthly_income",
        "monthly_expenses",
        "monthly_debt_payments",
        "transfers_in",
        "transfers_out",
        "monthly_net_flow"
    ]

    account_monthly = (
        accounts_calendar
        .join(
            account_flows,
            ["account_id", "month"],
            "left"
        )
        .fillna(0, subset=columnas_flujo)
    )


    # Recuperar saldo al cierre de cada mes

    ventana_cuenta = (
        Window.partitionBy("account_id")
        .orderBy("month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    account_monthly = account_monthly.withColumn(
        "closing_balance",
        F.coalesce(
            F.last(
                "observed_balance",
                ignorenulls=True
            ).over(ventana_cuenta),
            F.col("opening_balance")
        )
    )


    # Crear resumen mensual por cliente y moneda

    monthly_gold = (
        account_monthly
        .groupBy("client_id", "currency", "month")
        .agg(
            *[
                F.sum(columna).alias(columna)
                for columna in columnas_flujo
            ],
            F.sum("closing_balance").alias("closing_balance"),
            F.countDistinct("account_id").alias("total_accounts")
        )
        .withColumn(
            "monthly_savings",
            F.col("monthly_income")
            - F.col("monthly_expenses")
            - F.col("monthly_debt_payments")
        )
        .withColumn(
            "savings_rate",
            F.when(
                F.col("monthly_income") > 0,
                F.round(
                    F.col("monthly_savings")
                    / F.col("monthly_income"),
                    4
                )
            )
        )
        .withColumn(
            "expense_to_income_ratio",
            F.when(
                F.col("monthly_income") > 0,
                F.round(
                    F.col("monthly_expenses")
                    / F.col("monthly_income"),
                    4
                )
            )
        )
        .withColumn(
            "debt_payment_ratio",
            F.when(
                F.col("monthly_income") > 0,
                F.round(
                    F.col("monthly_debt_payments")
                    / F.col("monthly_income"),
                    4
                )
            )
        )
    )

    # Preparar cortes mensuales

    cortes = (
        monthly_gold
        .select("client_id", "currency", "month")
        .withColumn(
            "cutoff_date",
            F.last_day("month")
        )
    )


    # Relacionar obligaciones con cada corte

    obligaciones_historicas = (
        cortes
        .join(
            obligations_spark.select(
                "client_id",
                "currency",
                "obligation_id",
                "due_date",
                "payment_date",
                "amount_due",
                "amount_paid"
            ),
            ["client_id", "currency"],
            "left"
        )
        .withColumn(
            "due_at_cutoff",
            F.when(
                F.col("due_date") <= F.col("cutoff_date"),
                F.col("amount_due")
            ).otherwise(0)
        )
        .withColumn(
            "paid_at_cutoff",
            F.when(
                (F.col("due_date") <= F.col("cutoff_date"))
                & (F.col("payment_date") <= F.col("cutoff_date")),
                F.col("amount_paid")
            ).otherwise(0)
        )
        .withColumn(
            "outstanding_at_cutoff",
            F.col("due_at_cutoff") - F.col("paid_at_cutoff")
        )
        .withColumn(
            "days_past_due",
            F.when(
                F.col("outstanding_at_cutoff") > 0,
                F.greatest(
                    F.datediff("cutoff_date", "due_date"),
                    F.lit(0)
                )
            ).otherwise(0)
        )
    )


    # Agregar obligaciones pendientes

    obligations_monthly = (
        obligaciones_historicas
        .groupBy("client_id", "currency", "month")
        .agg(
            F.sum(
                "outstanding_at_cutoff"
            ).alias("outstanding_due"),
            F.max(
                "days_past_due"
            ).alias("max_days_past_due"),
            F.sum(
                F.when(
                    (F.col("outstanding_at_cutoff") > 0)
                    & (F.col("days_past_due") >= 30),
                    1
                ).otherwise(0)
            ).alias("installments_30_days_past_due")
        )
    )

    monthly_gold = monthly_gold.join(
        obligations_monthly,
        ["client_id", "currency", "month"],
        "left"
    )

    # Definir ventanas temporales

    ventana_cliente = Window.partitionBy(
        "client_id",
        "currency"
    ).orderBy("month")

    ventana_tres_meses = ventana_cliente.rowsBetween(-2, 0)

    ventana_historial = ventana_cliente.rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )


    # Crear variables mensuales

    monthly_gold = (
        monthly_gold
        .withColumn(
            "month_number",
            F.month("month")
        )
        .withColumn(
            "history_months",
            F.count("*").over(ventana_historial)
        )
        .withColumn(
            "income_lag_1",
            F.lag("monthly_income", 1).over(ventana_cliente)
        )
        .withColumn(
            "income_lag_3",
            F.lag("monthly_income", 3).over(ventana_cliente)
        )
        .withColumn(
            "income_mean_3",
            F.avg("monthly_income").over(ventana_tres_meses)
        )
        .withColumn(
            "income_std_3",
            F.stddev_samp("monthly_income").over(
                ventana_tres_meses
            )
        )
        .withColumn(
            "expenses_mean_3",
            F.avg("monthly_expenses").over(ventana_tres_meses)
        )
        .withColumn(
            "expenses_std_3",
            F.stddev_samp("monthly_expenses").over(
                ventana_tres_meses
            )
        )
        .withColumn(
            "income_cv_3",
            F.when(
                F.col("income_mean_3") > 0,
                F.round(
                    F.col("income_std_3")
                    / F.col("income_mean_3"),
                    4
                )
            )
        )
        .withColumn(
            "income_to_mean_3_ratio",
            F.when(
                F.col("income_mean_3") > 0,
                F.round(
                    F.col("monthly_income")
                    / F.col("income_mean_3"),
                    4
                )
            )
        )
        .withColumn(
            "income_change_3",
            F.when(
                F.col("income_mean_3") > 0,
                F.round(
                    (
                        F.col("monthly_income")
                        - F.col("income_mean_3")
                    )
                    / F.col("income_mean_3"),
                    4
                )
            )
        )
        .withColumn(
            "negative_balance_months_3",
            F.sum(
                F.when(
                    F.col("closing_balance") < 0,
                    1
                ).otherwise(0)
            ).over(ventana_tres_meses)
        )
    )


    # Verificar continuidad mensual

    revision_meses = monthly_gold.withColumn(
        "previous_month",
        F.lag("month").over(ventana_cliente)
    )

    if (
        revision_meses
        .filter(
            F.col("previous_month").isNotNull()
            & (
                F.col("month")
                != F.add_months("previous_month", 1)
            )
        )
        .limit(1)
        .count()
    ):
        raise ValueError(
            "El historial mensual contiene períodos discontinuos."
        )


    # Seleccionar último mes de cada cliente

    ventana_ultimo = Window.partitionBy(
        "client_id",
        "currency"
    ).orderBy(F.col("month").desc())

    latest_monthly = (
        monthly_gold
        .withColumn(
            "position",
            F.row_number().over(ventana_ultimo)
        )
        .filter(F.col("position") == 1)
        .drop("position")
    )


    # Evitar mezclar monedas en el perfil

    if (
        latest_monthly.groupBy("client_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError(
            "Hay clientes con varias monedas. "
            "El perfil debe conservar un resumen por moneda."
        )


    # Preparar último resumen

    latest_summary = latest_monthly.select(
        F.col("client_id").alias("id"),
        "currency",
        F.last_day("month").alias("financial_cutoff_date"),
        F.col("closing_balance").alias("account_balance"),
        "monthly_income",
        "monthly_expenses",
        "monthly_debt_payments",
        "monthly_savings",
        "savings_rate",
        "expense_to_income_ratio",
        "debt_payment_ratio",
        "monthly_transactions",
        "transfers_in",
        "transfers_out",
        "outstanding_due",
        "max_days_past_due",
        "installments_30_days_past_due",
        "income_mean_3",
        "income_std_3",
        "expenses_mean_3",
        "expenses_std_3",
        "income_cv_3",
        "income_to_mean_3_ratio",
        "income_change_3",
        "history_months",
        "income_lag_1",
        "income_lag_3",
        "negative_balance_months_3",
        "month_number"
    )


    return account_monthly, monthly_gold, latest_summary


def create_recent_summaries(customer_gold, monthly_gold, movements_spark):
    # Recuperar el último mes de cada cliente
    latest_monthly = (
        monthly_gold
        .withColumn("_position", F.row_number().over(
            Window.partitionBy("client_id", "currency").orderBy(F.col("month").desc())
        ))
        .filter(F.col("_position") == 1)
        .drop("_position")
    )

    # Conservar una sola columna de moneda en el perfil
    customer_gold = (
        customer_gold
        .withColumn(
            "currency",
            F.coalesce(
                F.col("currency"),
                F.col("profile_currency")
            )
        )
        .drop("profile_currency")
        .withColumn(
            "has_financial_history",
            F.col("financial_cutoff_date").isNotNull()
        )
    )


    # Seleccionar los últimos tres meses completos
    ventana_reciente = Window.partitionBy(
        "client_id",
        "currency"
    ).orderBy(F.col("month").desc())

    recent_months_gold = (
        monthly_gold
        .withColumn(
            "_position",
            F.row_number().over(ventana_reciente)
        )
        .filter(F.col("_position") <= 3)
        .drop("_position")
    )


    # Crear información de gastos por categoría y mes
    expense_categories_gold = (
        movements_spark
        .filter(F.col("movement_type") == "expense")
        .withColumn(
            "month",
            F.trunc(F.to_date("date"), "month")
        )
        .join(
            monthly_gold.select(
                "client_id",
                "currency",
                "month"
            ),
            ["client_id", "currency", "month"],
            "left_semi"
        )
        .groupBy(
            "client_id",
            "currency",
            "month",
            "category"
        )
        .agg(
            F.count("*").alias("expense_transactions"),
            F.sum(-F.col("amount")).alias("total_expenses")
        )
    )


    # Resumir los últimos tres meses por categoría
    recent_categories_gold = (
        expense_categories_gold
        .join(
            recent_months_gold.select(
                "client_id",
                "currency",
                "month"
            ),
            ["client_id", "currency", "month"],
            "left_semi"
        )
        .groupBy(
            "client_id",
            "currency",
            "category"
        )
        .agg(
            F.sum("expense_transactions").alias(
                "expense_transactions"
            ),
            F.sum("total_expenses").alias("total_expenses")
        )
    )


    # Crear indicadores para consultar la salud financiera
    health_gold = (
        recent_months_gold
        .groupBy("client_id", "currency")
        .agg(
            F.count("*").alias("months_available_3m"),
            F.min("month").alias("period_start"),
            F.last_day(
                F.max("month")
            ).alias("financial_cutoff_date"),
            F.sum("monthly_income").alias("income_3m"),
            F.sum("monthly_expenses").alias("expenses_3m"),
            F.sum(
                "monthly_debt_payments"
            ).alias("debt_payments_3m"),
            F.sum("monthly_savings").alias("savings_3m"),
            F.sum(
                "monthly_transactions"
            ).alias("transactions_3m"),
            F.avg("monthly_income").alias("average_income_3m"),
            F.avg("monthly_expenses").alias("average_expenses_3m"),
            F.avg(
                "monthly_debt_payments"
            ).alias("average_debt_payments_3m"),
            F.avg("monthly_savings").alias("average_savings_3m"),
            F.stddev_samp("monthly_income").alias("income_std_3m"),
            F.stddev_samp("monthly_expenses").alias("expenses_std_3m")
        )
        .join(
            latest_monthly.select(
                "client_id",
                "currency",
                "monthly_income",
                "monthly_expenses",
                "monthly_debt_payments",
                "savings_rate",
                "expense_to_income_ratio",
                "debt_payment_ratio",
                "closing_balance",
                "outstanding_due",
                "max_days_past_due"
            ),
            ["client_id", "currency"],
            "left"
        )
        .withColumn(
            "savings_rate_3m",
            F.when(
                F.col("income_3m") > 0,
                F.round(
                    F.col("savings_3m") / F.col("income_3m"),
                    4
                )
            )
        )
        .withColumn(
            "debt_payment_ratio_3m",
            F.when(
                F.col("income_3m") > 0,
                F.round(
                    F.col("debt_payments_3m") / F.col("income_3m"),
                    4
                )
            )
        )
        .withColumn(
            "expense_to_income_ratio_3m",
            F.when(
                F.col("income_3m") > 0,
                F.round(
                    F.col("expenses_3m") / F.col("income_3m"),
                    4
                )
            )
        )
        .withColumn(
            "income_variation_ratio_3m",
            F.when(
                (F.col("average_income_3m") > 0)
                & F.col("income_std_3m").isNotNull(),
                F.round(
                    F.col("income_std_3m")
                    / F.col("average_income_3m"),
                    4
                )
            )
        )
        .withColumn(
            "income_to_average_ratio_3m",
            F.when(
                F.col("average_income_3m") > 0,
                F.round(
                    F.col("monthly_income")
                    / F.col("average_income_3m"),
                    4
                )
            )
        )
        .withColumn(
            "income_change_3m",
            F.when(
                F.col("average_income_3m") > 0,
                F.round(
                    (
                        F.col("monthly_income")
                        - F.col("average_income_3m")
                    )
                    / F.col("average_income_3m"),
                    4
                )
            )
        )
        .withColumn(
            "expenses_change_3m",
            F.when(
                F.col("average_expenses_3m") > 0,
                F.round(
                    (
                        F.col("monthly_expenses")
                        - F.col("average_expenses_3m")
                    )
                    / F.col("average_expenses_3m"),
                    4
                )
            )
        )
        .withColumn(
            "savings_rate_change_3m",
            F.when(
                F.col("savings_rate").isNotNull()
                & F.col("savings_rate_3m").isNotNull(),
                F.round(
                    F.col("savings_rate")
                    - F.col("savings_rate_3m"),
                    4
                )
            )
        )
        .withColumn(
            "expense_ratio_change_3m",
            F.when(
                F.col("expense_to_income_ratio").isNotNull()
                & F.col("expense_to_income_ratio_3m").isNotNull(),
                F.round(
                    F.col("expense_to_income_ratio")
                    - F.col("expense_to_income_ratio_3m"),
                    4
                )
            )
        )
        .withColumn(
            "debt_payment_ratio_change_3m",
            F.when(
                F.col("debt_payment_ratio").isNotNull()
                & F.col("debt_payment_ratio_3m").isNotNull(),
                F.round(
                    F.col("debt_payment_ratio")
                    - F.col("debt_payment_ratio_3m"),
                    4
                )
            )
        )
        .withColumn(
            "expense_coverage_months",
            F.when(
                (
                    F.col("expenses_3m")
                    + F.col("debt_payments_3m")
                ) > 0,
                F.round(
                    F.greatest(
                        F.col("closing_balance"),
                        F.lit(0)
                    )
                    / (
                        (
                            F.col("expenses_3m")
                            + F.col("debt_payments_3m")
                        )
                        / F.col("months_available_3m")
                    ),
                    2
                )
            )
        )
        .withColumn(
            "has_overdue_payments",
            (F.col("outstanding_due") > 0)
            & (F.col("max_days_past_due") > 0)
        )
        .withColumn(
            "has_negative_balance",
            F.col("closing_balance") < 0
        )
        .withColumn(
            "has_budget_deficit_3m",
            F.col("savings_3m") < 0
        )
        .withColumn(
            "financial_health_status",
            F.when(
                F.col("months_available_3m") < 3,
                "historial_insuficiente"
            )
            .when(
                F.col("has_overdue_payments")
                | F.col("has_negative_balance")
                | F.col("has_budget_deficit_3m"),
                "requiere_atencion"
            )
            .when(
                F.col("income_3m") <= 0,
                "sin_ingresos"
            )
            .when(
                F.col("savings_3m") > 0,
                "margen_positivo"
            )
            .otherwise("sin_margen")
        )
    )


    # Incorporar indicadores al resumen del cliente
    columnas_salud = [
        "months_available_3m",
        "period_start",
        "income_3m",
        "expenses_3m",
        "debt_payments_3m",
        "savings_3m",
        "transactions_3m",
        "average_income_3m",
        "average_expenses_3m",
        "average_debt_payments_3m",
        "average_savings_3m",
        "income_std_3m",
        "expenses_std_3m",
        "savings_rate_3m",
        "debt_payment_ratio_3m",
        "expense_to_income_ratio_3m",
        "income_variation_ratio_3m",
        "income_to_average_ratio_3m",
        "income_change_3m",
        "expenses_change_3m",
        "savings_rate_change_3m",
        "expense_ratio_change_3m",
        "debt_payment_ratio_change_3m",
        "expense_coverage_months",
        "has_overdue_payments",
        "has_negative_balance",
        "has_budget_deficit_3m",
        "financial_health_status"
    ]

    customer_gold = (
        customer_gold
        .drop(*columnas_salud)
        .join(
            health_gold.select(
                F.col("client_id").alias("id"),
                "currency",
                "financial_cutoff_date",
                *columnas_salud
            ),
            ["id", "currency", "financial_cutoff_date"],
            "left"
        )
        .withColumn(
            "financial_health_status",
            F.coalesce(
                F.col("financial_health_status"),
                F.lit("historial_insuficiente")
            )
        )
    )


    return (
        customer_gold, recent_months_gold, expense_categories_gold,
        recent_categories_gold, health_gold
    )


def create_customer_model_summary(customer_gold, predictions_spark=None):
    # Conservar el perfil base separado de sus predicciones
    columnas_prediccion = [
        "feature_month",
        "prediction_month",
        "predicted_income",
        "income_prediction_method",
        "has_income_prediction",
        "prediction_validation_status"
    ]
    customer = customer_gold.drop(*columnas_prediccion)

    # Al reconstruir GOLD, invalidar las predicciones del corte anterior
    if predictions_spark is None:
        return (
            customer
            .withColumn("feature_month", F.lit(None).cast("date"))
            .withColumn("prediction_month", F.lit(None).cast("date"))
            .withColumn("predicted_income", F.lit(None).cast("double"))
            .withColumn("income_prediction_method", F.lit(None).cast("string"))
            .withColumn("has_income_prediction", F.lit(False))
            .withColumn("prediction_validation_status", F.lit("PENDIENTE"))
        )

    # Validar columnas y tipos antes de interpretar las predicciones
    tipos = {
        "client_id": TIPOS_ENTEROS,
        "currency": TIPOS_TEXTO,
        "feature_month": TIPOS_FECHA,
        "prediction_month": TIPOS_FECHA,
        "predicted_income": TIPOS_NUMERICOS,
        "method": TIPOS_TEXTO
    }
    esquema = {
        campo.name: campo.dataType
        for campo in predictions_spark.schema.fields
    }
    for columna, permitidos in tipos.items():
        if columna not in esquema or not isinstance(esquema[columna], permitidos):
            raise ErrorCalidadDatos(
                f"Predicciones: falta la columna {columna} o su tipo es incorrecto."
            )

    # El perfil debe conservar una fila por cliente
    if customer.filter(
        F.col("id").isNull() | (F.col("id") < 0)
    ).limit(1).count():
        raise ErrorCalidadDatos("Perfil: identificador inválido.")

    if customer.groupBy("id").count().filter(
        F.col("count") > 1
    ).limit(1).count():
        raise ErrorCalidadDatos("Perfil: clientes duplicados.")

    if customer.filter(
        F.col("financial_cutoff_date").isNotNull()
        & (
            F.col("currency").isNull()
            | (F.length(F.trim("currency")) == 0)
            | (
                F.col("financial_cutoff_date")
                != F.last_day("financial_cutoff_date")
            )
        )
    ).limit(1).count():
        raise ErrorCalidadDatos("Perfil: moneda o corte inválido.")

    predicciones = predictions_spark.select(
        F.col("client_id").alias("id"),
        "currency",
        F.to_date("feature_month").alias("feature_month"),
        F.to_date("prediction_month").alias("prediction_month"),
        F.col("predicted_income").cast("double").alias("predicted_income"),
        F.col("method").alias("income_prediction_method")
    )

    errores = (
        F.col("id").isNull()
        | (F.col("id") < 0)
        | F.col("currency").isNull()
        | (F.length(F.trim("currency")) == 0)
        | F.col("feature_month").isNull()
        | F.col("prediction_month").isNull()
        | (F.dayofmonth("feature_month") != 1)
        | (
            F.col("prediction_month")
            != F.add_months("feature_month", 1)
        )
        | F.col("predicted_income").isNull()
        | F.isnan("predicted_income")
        | (F.abs(F.col("predicted_income")) == F.lit(float("inf")))
        | (F.col("predicted_income") < 0)
        | F.col("income_prediction_method").isNull()
        | ~F.col("income_prediction_method").isin("xgboost", "last_month")
    )
    if predicciones.filter(errores).limit(1).count():
        raise ErrorCalidadDatos("Las predicciones contienen valores inválidos.")

    if predicciones.groupBy("id", "currency").count().filter(
        F.col("count") > 1
    ).limit(1).count():
        raise ErrorCalidadDatos(
            "Existe más de una predicción por cliente y moneda."
        )

    claves = ["id", "currency", "financial_cutoff_date"]
    predicciones = predicciones.withColumn(
        "financial_cutoff_date",
        F.last_day("feature_month")
    )
    clientes_con_historial = customer.filter(
        F.col("financial_cutoff_date").isNotNull()
    ).select(*claves)

    if predicciones.join(
        clientes_con_historial, claves, "left_anti"
    ).limit(1).count():
        raise ErrorCalidadDatos(
            "Hay predicciones sin un cliente, moneda y corte coincidentes en GOLD."
        )

    if clientes_con_historial.join(
        predicciones.select(*claves), claves, "left_anti"
    ).limit(1).count():
        raise ErrorCalidadDatos(
            "Faltan predicciones para clientes con historial."
        )

    return (
        customer.join(predicciones, claves, "left")
        .withColumn(
            "has_income_prediction",
            F.col("predicted_income").isNotNull()
        )
        .withColumn(
            "prediction_validation_status",
            F.when(F.col("has_income_prediction"), "APROBADO")
            .otherwise("SIN_HISTORIAL")
        )
    )


def guardar_validacion_modelo(spark, output_path, estado, detalle, **conteos):
    columnas = [
        "expected_rows", "received_rows", "missing_rows", "unexpected_rows",
        "duplicate_keys", "invalid_rows", "xgboost_rows", "last_month_rows"
    ]
    control = spark.range(1).select(
        F.lit(estado).alias("status"),
        F.lit(detalle).alias("detail"),
        *[
            F.lit(conteos.get(columna)).cast("long").alias(columna)
            for columna in columnas
        ],
        F.current_timestamp().alias("validated_at")
    )
    control.write.mode("overwrite").parquet(
        f"{output_path}/income_model_validation.parquet"
    )


def contar_predicciones(customer, predictions):
    # Comprobar el esquema antes de calcular la cobertura
    tipos = {
        "client_id": TIPOS_ENTEROS,
        "currency": TIPOS_TEXTO,
        "feature_month": TIPOS_FECHA,
        "prediction_month": TIPOS_FECHA,
        "predicted_income": TIPOS_NUMERICOS,
        "method": TIPOS_TEXTO
    }
    esquema = {campo.name: campo.dataType for campo in predictions.schema.fields}
    for columna, permitidos in tipos.items():
        if columna not in esquema or not isinstance(esquema[columna], permitidos):
            raise ErrorCalidadDatos(
                f"Predicciones: falta la columna {columna} o su tipo es incorrecto."
            )

    esperados = customer.filter(
        F.col("financial_cutoff_date").isNotNull()
    ).select(
        F.col("id").alias("client_id"), "currency",
        F.trunc("financial_cutoff_date", "month").alias("feature_month"),
        F.add_months(F.trunc("financial_cutoff_date", "month"), 1)
        .alias("prediction_month")
    )
    claves = ["client_id", "currency", "feature_month", "prediction_month"]
    errores = (
        F.col("client_id").isNull() | (F.col("client_id") < 0)
        | F.col("currency").isNull() | (F.col("currency") != "MXN")
        | F.col("feature_month").isNull()
        | (F.dayofmonth("feature_month") != 1)
        | F.col("prediction_month").isNull()
        | (F.to_date("prediction_month") != F.add_months("feature_month", 1))
        | F.col("predicted_income").isNull()
        | F.isnan(F.col("predicted_income").cast("double"))
        | (F.abs(F.col("predicted_income")) == F.lit(float("inf")))
        | (F.col("predicted_income") < 0)
        | F.col("method").isNull()
        | ~F.col("method").isin("xgboost", "last_month")
    )
    recibidos = predictions.select(
        "client_id", "currency",
        F.to_date("feature_month").alias("feature_month"),
        F.to_date("prediction_month").alias("prediction_month")
    )
    return {
        "expected_rows": esperados.count(),
        "received_rows": predictions.count(),
        "missing_rows": esperados.join(recibidos, claves, "left_anti").count(),
        "unexpected_rows": recibidos.join(esperados, claves, "left_anti").count(),
        "duplicate_keys": predictions.groupBy("client_id", "currency")
        .count().filter(F.col("count") > 1).count(),
        "invalid_rows": predictions.filter(errores).count(),
        "xgboost_rows": predictions.filter(F.col("method") == "xgboost").count(),
        "last_month_rows": predictions.filter(F.col("method") == "last_month").count()
    }


def run_prediction_pipeline(spark, gold_path=OUTPUT_PATH, predictions_path=None):
    if predictions_path is None:
        predictions_path = f"{gold_path}/income_predictions.parquet"

    customer = spark.read.parquet(
        f"{gold_path}/customer_financial_summary.parquet"
    )
    base = create_customer_model_summary(customer)
    conteos = {
        "expected_rows": customer.filter(
            F.col("financial_cutoff_date").isNotNull()
        ).count()
    }

    # Invalidar predicciones anteriores antes de revisar el nuevo resultado
    base.write.mode("overwrite").parquet(
        f"{gold_path}/customer_model_summary.parquet"
    )
    guardar_validacion_modelo(
        spark, gold_path, "PENDIENTE", "Validando salida del modelo.", **conteos
    )

    try:
        predictions = spark.read.parquet(predictions_path)
        conteos.update(contar_predicciones(customer, predictions))
        enriched = create_customer_model_summary(customer, predictions)

        if (
            conteos["expected_rows"] == 0
            or conteos["received_rows"] != conteos["expected_rows"]
            or any(conteos[c] > 0 for c in [
                "missing_rows", "unexpected_rows", "duplicate_keys", "invalid_rows"
            ])
        ):
            raise ErrorCalidadDatos(
                "La salida del modelo está incompleta o contiene valores inválidos."
            )

        enriched.write.mode("overwrite").parquet(
            f"{gold_path}/customer_model_summary.parquet"
        )
        guardar_validacion_modelo(
            spark, gold_path, "APROBADO",
            "Cobertura, claves, moneda, períodos e importes aprobados.", **conteos
        )
    except Exception as error:
        # Conservar el perfil base sin predicciones cuando la salida es rechazada
        base.withColumn(
            "prediction_validation_status", F.lit("RECHAZADO")
        ).write.mode("overwrite").parquet(
            f"{gold_path}/customer_model_summary.parquet"
        )
        guardar_validacion_modelo(
            spark, gold_path, "RECHAZADO", str(error), **conteos
        )
        raise

    print("Predicciones validadas e incorporadas a GOLD")


def run_pipeline(spark, input_path=INPUT_PATH, output_path=OUTPUT_PATH):
    # Cargar datasets
    users_spark = spark.read.parquet(f"{input_path}/users.parquet")
    cards_spark = spark.read.parquet(f"{input_path}/cards.parquet")
    transactions_spark = spark.read.parquet(f"{input_path}/transactions.parquet")
    accounts_spark = spark.read.parquet(f"{input_path}/accounts.parquet")
    obligations_spark = spark.read.parquet(f"{input_path}/obligations_payments.parquet")

    # Validar información antes de agrupar
    total_users = validar_entrada_gold(users_spark, cards_spark, transactions_spark)

    # Separar convenciones
    legacy_transactions = transactions_spark.filter(
        F.col("amount_convention") == "legacy_unchanged"
    )

    movements_spark = transactions_spark.filter(
        F.col("amount_convention") == "signed_cashflow"
    )

    # Crear resúmenes anteriores
    cards_gold = create_cards_summary(cards_spark)
    transactions_gold = create_transactions_summary(legacy_transactions)
    users_gold = create_users_summary(users_spark)
    customer_gold = create_customer_summary(users_gold, cards_gold, transactions_gold)

    # Crear historial mensual
    account_monthly, monthly_gold, latest_summary = create_monthly_summaries(
        accounts_spark,
        obligations_spark,
        movements_spark
    )

    customer_gold = customer_gold.join(latest_summary, "id", "left")

    # Preparar los últimos tres meses y los indicadores financieros
    (
        customer_gold, recent_months_gold, expense_categories_gold,
        recent_categories_gold, health_gold
    ) = create_recent_summaries(customer_gold, monthly_gold, movements_spark)

    # Validar antes de guardar
    validar_salida_gold(customer_gold, total_users)

    if (
        monthly_gold.groupBy("client_id", "currency", "month")
        .count().filter(F.col("count") > 1).limit(1).count()
    ):
        raise ErrorCalidadDatos("Existen resúmenes mensuales duplicados.")

    flujo_esperado = (
        F.col("monthly_income")
        - F.col("monthly_expenses")
        - F.col("monthly_debt_payments")
        + F.col("transfers_in")
        - F.col("transfers_out")
    )

    if monthly_gold.filter(
        ~F.col("monthly_net_flow").eqNullSafe(flujo_esperado)
    ).limit(1).count():
        raise ErrorCalidadDatos("Los flujos mensuales no coinciden.")

    # Validar que las categorías sumen el gasto mensual
    gastos_categorias = (
        expense_categories_gold
        .groupBy("client_id", "currency", "month")
        .agg(
            F.sum("total_expenses").alias("category_expenses")
        )
    )

    revision_categorias = (
        monthly_gold
        .select(
            "client_id",
            "currency",
            "month",
            "monthly_expenses"
        )
        .join(
            gastos_categorias,
            ["client_id", "currency", "month"],
            "left"
        )
        .fillna(0, subset=["category_expenses"])
    )

    if revision_categorias.filter(
        ~F.col("monthly_expenses").eqNullSafe(F.col("category_expenses"))
    ).limit(1).count():
        raise ValueError(
            "Los gastos por categoría no coinciden con el total mensual."
        )



    # Guardar GOLD
    datasets = {
        "customer_model_summary": create_customer_model_summary(customer_gold),
        "customer_financial_summary": customer_gold,
        "transactions_summary": transactions_gold,
        "cards_summary": cards_gold,
        "customer_monthly_summary": monthly_gold,
        "account_monthly_summary": account_monthly,
        "customer_recent_months": recent_months_gold,
        "customer_expenses_by_category": expense_categories_gold,
        "customer_recent_expenses_by_category": recent_categories_gold,
        "customer_financial_health": health_gold,
        "accounts": accounts_spark,
        "obligations_payments": obligations_spark,
        "movements": movements_spark
    }

    for nombre, datos in datasets.items():
        datos.write.mode("overwrite").parquet(
            f"{output_path}/{nombre}.parquet"
        )

    guardar_validacion_modelo(
        spark, output_path, "PENDIENTE",
        "GOLD base generado; falta validar la salida del modelo.",
        expected_rows=customer_gold.filter(
            F.col("financial_cutoff_date").isNotNull()
        ).count()
    )
    print("Pipeline GOLD completado")


def main():
    # Separar la construcción de features de la incorporación del modelo
    parser = argparse.ArgumentParser(description="Procesamiento de GOLD")
    parser.add_argument(
        "--stage", choices=["base", "predictions"], default="base"
    )
    parser.add_argument("--input-path", default=INPUT_PATH)
    parser.add_argument("--output-path", default=OUTPUT_PATH)
    parser.add_argument("--predictions-path", default=None)
    args = parser.parse_args()

    # Crear sesión de Spark
    spark = (
        SparkSession.builder
        .appName("FinancialDigitalTwin_Gold")
        .getOrCreate()
    )

    print("Spark inicializado")

    try:
        if args.stage == "base":
            run_pipeline(spark, args.input_path, args.output_path)
        else:
            run_prediction_pipeline(
                spark,
                gold_path=args.output_path,
                predictions_path=args.predictions_path
            )
    finally:
        # Finalizar Spark
        spark.stop()


if __name__ == "__main__":
    main()

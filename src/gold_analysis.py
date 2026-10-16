import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
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
PROJECT_PATH = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_PATH / ".env", override=False)


# Rutas S3
BUCKET = os.getenv("S3_BUCKET", "").strip()

if not BUCKET:
    raise RuntimeError("Configura S3_BUCKET en .env.")

INPUT_PATH = f"s3a://{BUCKET}/silver"
OUTPUT_PATH = f"s3a://{BUCKET}/gold"


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
    return users_spark.select(
        "id",
        "current_age",
        "retirement_age",
        "gender",
        "yearly_income",
        "per_capita_income",
        "total_debt",
        "credit_score",
        "num_credit_cards"
    )


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


def run_pipeline(spark, input_path=INPUT_PATH, output_path=OUTPUT_PATH):
    # Cargar datasets
    users_spark = spark.read.parquet(
        f"{input_path}/users.parquet"
    )

    cards_spark = spark.read.parquet(
        f"{input_path}/cards.parquet"
    )

    transactions_spark = spark.read.parquet(
        f"{input_path}/transactions.parquet"
    )

    print("Datos cargados")

    # Validar información antes de agrupar
    total_users = validar_entrada_gold(
        users_spark,
        cards_spark,
        transactions_spark
    )

    # Crear resúmenes
    cards_gold = create_cards_summary(cards_spark)
    transactions_gold = create_transactions_summary(transactions_spark)
    users_gold = create_users_summary(users_spark)

    customer_gold = create_customer_summary(
        users_gold,
        cards_gold,
        transactions_gold
    )

    # Validar información antes de guardar
    validar_salida_gold(
        customer_gold,
        total_users
    )

    print("Validaciones aprobadas")

    # Guardar GOLD
    customer_gold.write.mode("overwrite").parquet(
        f"{output_path}/customer_financial_summary.parquet"
    )

    transactions_gold.write.mode("overwrite").parquet(
        f"{output_path}/transactions_summary.parquet"
    )

    cards_gold.write.mode("overwrite").parquet(
        f"{output_path}/cards_summary.parquet"
    )

    print("Pipeline GOLD completado")


def main():
    # Crear sesión de Spark
    spark = (
        SparkSession.builder
        .appName("FinancialDigitalTwin_Gold")
        .getOrCreate()
    )

    print("Spark inicializado")

    try:
        run_pipeline(spark)
    finally:
        # Finalizar Spark
        spark.stop()


if __name__ == "__main__":
    main()
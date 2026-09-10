from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim, lower, regexp_replace, to_timestamp, coalesce, try_to_timestamp, lit
import os


# Rutas
INPUT_PATH = "/opt/project/data/processed/bronze"
OUTPUT_PATH = "/opt/project/data/processed/silver"


# Crear sesión de Spark
spark = (
    SparkSession.builder
    .appName("FinancialDigitalTwin_Silver")
    .getOrCreate()
)

print("Spark inicializado")


# Cargar datasets

os.makedirs(OUTPUT_PATH, exist_ok=True)

users_spark = (
    spark.read
    .parquet(f"{INPUT_PATH}/users.parquet")
)

cards_spark = (
    spark.read
    .parquet(f"{INPUT_PATH}/cards.parquet")
)

transactions_spark = (
    spark.read
    .parquet(f"{INPUT_PATH}/transactions.parquet")
)

print("Datos cargados")


# Transformar users

users_silver = users_spark.withColumn(
    "gender",
    trim(lower(col("gender")))
)

users_silver = users_silver.withColumn(
    "address",
    trim(lower(col("address")))
)

users_silver = users_silver.withColumn(
    "yearly_income",
    col("yearly_income").cast("double")
)

users_silver = users_silver.withColumn(
    "per_capita_income",
    col("per_capita_income").cast("double")
)

users_silver = users_silver.withColumn(
    "total_debt",
    col("total_debt").cast("double")
)


# Transformar cards

cards_silver = cards_spark.withColumn(
    "card_brand",
    trim(lower(col("card_brand")))
)

cards_silver = cards_silver.withColumn(
    "card_type",
    trim(lower(col("card_type")))
)

cards_silver = cards_silver.withColumn(
    "has_chip",
    trim(lower(col("has_chip")))
)

cards_silver = cards_silver.withColumn(
    "card_on_dark_web",
    trim(lower(col("card_on_dark_web")))
)

cards_silver = cards_silver.withColumn(
    "credit_limit",
    col("credit_limit").cast("double")
)


# Transformar transactions

transactions_silver = transactions_spark.withColumn(
    "use_chip",
    trim(lower(col("use_chip")))
)

transactions_silver = transactions_silver.withColumn(
    "merchant_city",
    trim(lower(col("merchant_city")))
)

transactions_silver = transactions_silver.withColumn(
    "merchant_state",
    trim(lower(col("merchant_state")))
)

transactions_silver = transactions_silver.withColumn(
    "amount",
    col("amount").cast("double")
)


# Convertir fecha de transactions

transactions_silver = transactions_silver.withColumn(
    "date",
    coalesce(
        try_to_timestamp(
            col("date"),
            lit("dd/MM/yyyy HH:mm")
        ),
        try_to_timestamp(
            col("date"),
            lit("MM/dd/yyyy HH:mm")
        )
    )
)


# Eliminar duplicados

users_silver = users_silver.dropDuplicates(["id"])

cards_silver = cards_silver.dropDuplicates(["id"])

transactions_silver = transactions_silver.dropDuplicates(["id"])


# Verificar valores faltantes

users_nulls = users_silver.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in users_silver.columns
])

cards_nulls = cards_silver.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in cards_silver.columns
])

transactions_nulls = transactions_silver.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in transactions_silver.columns
])

print("Valores faltantes en users:")
users_nulls.show()

print("Valores faltantes en cards:")
cards_nulls.show()

print("Valores faltantes en transactions:")
transactions_nulls.show()


# Guardar SILVER

users_silver.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/users.parquet"
)

cards_silver.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/cards.parquet"
)

transactions_silver.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/transactions.parquet"
)

print("Pipeline SILVER completado")


# Finalizar Spark

spark.stop()
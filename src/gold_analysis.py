from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    round,
    countDistinct
)


# Rutas S3
BUCKET = "financial-digital-twin-bbva-022950218031-us-east-2-an"

INPUT_PATH = f"s3a://{BUCKET}/silver"
OUTPUT_PATH = f"s3a://{BUCKET}/gold"


# Crear sesión de Spark
spark = (
    SparkSession.builder
    .appName("FinancialDigitalTwin_Gold")
    .getOrCreate()
)

print("Spark inicializado")


# Cargar datasets

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


# Crear resumen de tarjetas

cards_gold = (
    cards_spark
    .groupBy("client_id")
    .agg(
        count("id").alias("total_cards"),
        countDistinct("card_brand").alias("total_card_brands"),
        countDistinct("card_type").alias("total_card_types"),
        round(avg("credit_limit"), 2).alias("average_credit_limit"),
        round(sum("credit_limit"), 2).alias("total_credit_limit")
    )
)


# Crear resumen de transacciones

transactions_gold = (
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

users_gold = (
    users_spark
    .select(
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
)


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


# Crear métricas financieras

customer_gold = (
    customer_gold
    .withColumn(
        "spending_to_income_ratio",
        round(
            col("total_spent") / col("yearly_income"),
            4
        )
    )
    .withColumn(
        "debt_to_income_ratio",
        round(
            col("total_debt") / col("yearly_income"),
            4
        )
    )
)


# Guardar GOLD

customer_gold.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/customer_financial_summary.parquet"
)

transactions_gold.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/transactions_summary.parquet"
)

cards_gold.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/cards_summary.parquet"
)

print("Pipeline GOLD completado")


# Finalizar Spark

spark.stop()
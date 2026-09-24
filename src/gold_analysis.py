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


# Rutas S3
BUCKET = "financial-digital-twin-bbva-022950218031-us-east-2-an"

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

    cards_gold = create_cards_summary(cards_spark)
    transactions_gold = create_transactions_summary(transactions_spark)
    users_gold = create_users_summary(users_spark)

    customer_gold = create_customer_summary(
        users_gold,
        cards_gold,
        transactions_gold
    )

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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, when, isnan
from pyspark.ml.feature import Imputer


# Rutas S3
BUCKET = "financial-digital-twin-bbva-022950218031-us-east-2-an"

INPUT_PATH = f"s3a://{BUCKET}/incoming"
OUTPUT_PATH = f"s3a://{BUCKET}/bronze"


# Limpiar strings: quitar espacios y convertir a minúsculas
def clean_string(df, column):
    return df.withColumn(
        column,
        trim(lower(col(column)))
    )


# Limpiar moneda: quitar $ y comas
def clean_currency(df, column):
    return df.withColumn(
        column,
        regexp_replace(
            regexp_replace(col(column), r"\$", ""),
            ",",
            ""
        ).cast("float")
    )


# Imputar con moda
def impute_mode(df, column):
    mode_row = (
        df.filter(col(column).isNotNull())
        .groupBy(column)
        .count()
        .orderBy(col("count").desc(), col(column).asc())
        .first()
    )

    if mode_row is None:
        return df

    return df.fillna({column: mode_row[column]})


# Imputar con mediana
def impute_median(df, column):
    valid_values = df.filter(
        col(column).isNotNull()
        & ~isnan(col(column))
    )

    if not valid_values.take(1):
        return df

    imputer = Imputer(
        inputCols=[column],
        outputCols=[column]
    ).setStrategy("median")

    return imputer.fit(df).transform(df)


# Limpiar users
def clean_users(users_spark):
    # Limpiar strings
    users_spark = clean_string(users_spark, "gender")
    users_spark = clean_string(users_spark, "address")

    # Limpiar columnas de moneda
    currency_columns = [
        "per_capita_income",
        "yearly_income",
        "total_debt"
    ]

    for column in currency_columns:
        if column in users_spark.columns:
            users_spark = clean_currency(users_spark, column)

    # Imputar current_age con mediana
    users_spark = impute_median(users_spark, "current_age")

    # Imputar gender con moda
    users_spark = impute_mode(users_spark, "gender")

    # Validar rangos
    users_spark = users_spark.withColumn(
        "current_age",
        when(col("current_age") < 18, 18)
        .when(col("current_age") > 120, 120)
        .otherwise(col("current_age"))
    )

    users_spark = users_spark.withColumn(
        "credit_score",
        when(col("credit_score") < 300, 300)
        .when(col("credit_score") > 850, 850)
        .otherwise(col("credit_score"))
    )

    return users_spark


# Limpiar cards
def clean_cards(cards_spark):
    # Limpiar strings
    string_columns = [
        "card_brand",
        "card_type",
        "has_chip",
        "card_on_dark_web"
    ]

    for column in string_columns:
        if column in cards_spark.columns:
            cards_spark = clean_string(cards_spark, column)

    # Limpiar credit_limit
    if "credit_limit" in cards_spark.columns:
        cards_spark = clean_currency(cards_spark, "credit_limit")

    # Imputar num_cards_issued con mediana
    cards_spark = impute_median(cards_spark, "num_cards_issued")

    # Imputar card_brand con moda
    cards_spark = impute_mode(cards_spark, "card_brand")

    # Validar credit_limit
    cards_spark = cards_spark.withColumn(
        "credit_limit",
        when(col("credit_limit") < 0, 0)
        .otherwise(col("credit_limit"))
    )

    return cards_spark


# Limpiar transactions
def clean_transactions(transactions_spark):
    # Limpiar strings
    string_columns = [
        "use_chip",
        "merchant_city",
        "merchant_state",
        "zip",
        "errors"
    ]

    for column in string_columns:
        if column in transactions_spark.columns:
            transactions_spark = clean_string(
                transactions_spark,
                column
            )

    # Limpiar amount
    if "amount" in transactions_spark.columns:
        transactions_spark = clean_currency(
            transactions_spark,
            "amount"
        )

    # Imputar amount con mediana
    transactions_spark = impute_median(
        transactions_spark,
        "amount"
    )

    # Imputar merchant_city con moda
    transactions_spark = impute_mode(
        transactions_spark,
        "merchant_city"
    )

    # Validar amount
    transactions_spark = transactions_spark.withColumn(
        "amount",
        when(col("amount") < -10000, -10000)
        .when(col("amount") > 10000, 10000)
        .otherwise(col("amount"))
    )

    return transactions_spark


def run_pipeline(spark, input_path=INPUT_PATH, output_path=OUTPUT_PATH):
    # Cargar datasets
    users_spark = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{input_path}/users_data.csv")
    )

    cards_spark = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{input_path}/cards_data.csv")
    )

    transactions_spark = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{input_path}/transactions_data.csv")
    )

    print("Datos cargados")

    users_spark = clean_users(users_spark)
    cards_spark = clean_cards(cards_spark)
    transactions_spark = clean_transactions(transactions_spark)

    # Guardar BRONZE
    users_spark.write.mode("overwrite").parquet(
        f"{output_path}/users.parquet"
    )

    cards_spark.write.mode("overwrite").parquet(
        f"{output_path}/cards.parquet"
    )

    transactions_spark.write.mode("overwrite").parquet(
        f"{output_path}/transactions.parquet"
    )

    print("Pipeline BRONZE completado")


def main():
    # Crear sesión de Spark
    spark = (
        SparkSession.builder
        .appName("FinancialDigitalTwin_Bronze")
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
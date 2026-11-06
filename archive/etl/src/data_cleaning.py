from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.ml.feature import Imputer

def clean_users_data(users_spark):

    # Imputar current_age utilizando la mediana
    imputer = Imputer(
        inputCols=["current_age"],
        outputCols=["current_age"]
    ).setStrategy("median")

    users_spark = (
        imputer
        .fit(users_spark)
        .transform(users_spark)
    )

    # Imputar gender utilizando la moda
    gender_mode = (
        users_spark
        .filter(col("gender").isNotNull())
        .groupBy("gender")
        .count()
        .orderBy(col("count").desc())
        .first()["gender"]
    )

    users_spark = users_spark.fillna({
        "gender": gender_mode
    })

    return users_spark


def main():

    # Crear sesión de Spark
    spark = (
        SparkSession.builder
        .appName("FinancialDigitalTwin_DataCleaning")
        .getOrCreate()
    )

    # Cargar datasets

    cards_spark = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("../data/modified/cards_data.csv")
    )

    transactions_spark = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("../data/modified/transactions_data.csv")
    )

    users_spark = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("../data/modified/users_data.csv")
    )

    # Limpieza de users

    users_spark = clean_users_data(users_spark)

    # Verificación de valores faltantes

    users_nulls = users_spark.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in users_spark.columns
    ])

    cards_nulls = cards_spark.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in cards_spark.columns
    ])

    transactions_nulls = transactions_spark.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in transactions_spark.columns
    ])

    # Mostrar resultados de validación

    print("Valores faltantes en users:")
    users_nulls.show()

    print("Valores faltantes en cards:")
    cards_nulls.show()

    print("Valores faltantes en transactions:")
    transactions_nulls.show()

    # Finalizar Spark

    spark.stop()


if __name__ == "__main__":
    main()
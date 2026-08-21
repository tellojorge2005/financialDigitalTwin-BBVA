import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, when, lit, concat
from pyspark.ml.feature import Imputer

# Configurar Spark para tests
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    yield spark
    spark.stop()

# Datos de prueba para users
@pytest.fixture
def users_data(spark):
    data = [
        (1, 25, "Male", 720),
        (2, None, "Female", 680),
        (3, 40, None, 750),
    ]
    columns = ["id", "current_age", "gender", "credit_score"]
    return spark.createDataFrame(data, columns)

# Prueba: limpieza de strings
def test_clean_string(spark, users_data):
    # Funcion simple para limpiar strings
    def clean_string(df, column):
        return df.withColumn(column, trim(lower(col(column))))

    cleaned = clean_string(users_data, "gender")

    # Verificar que "Male" paso a "male"
    assert cleaned.filter(col("gender") == "male").count() == 1

# Prueba: limpieza de moneda
def test_clean_currency(spark, users_data):
    # Funcion simple para limpiar moneda
    def clean_currency(df, column):
        return df.withColumn(column, regexp_replace(col(column), "\\$", "").cast("float"))

    # Crear columna de moneda con $
    users_with_currency = users_data.withColumn("salary", lit("$100"))

    cleaned = clean_currency(users_with_currency, "salary")

    # Verificar que se elimino el $
    assert cleaned.filter(col("salary") == 100.0).count() == 3

# Prueba: imputacion con mediana
def test_impute_median(spark, users_data):
    imputer = Imputer(
        inputCols=["current_age"],
        outputCols=["current_age"]
    ).setStrategy("median")

    cleaned = imputer.fit(users_data).transform(users_data)

    # Verificar que no hay nulos
    assert cleaned.filter(col("current_age").isNull()).count() == 0
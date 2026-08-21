from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, when
from pyspark.ml.feature import Imputer
import os

# Configurar Hadoop para Windows
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] = f"{os.environ.get('PATH', '')};C:\\hadoop\\bin"

# Rutas
INPUT_PATH = "../data/modified"
OUTPUT_PATH = "../data/processed/bronze"

# Crear sesion de Spark
spark = SparkSession.builder.appName("Bronze_Cleaning").getOrCreate()

# Funcion para limpiar strings
def clean_string(df, column):
    return df.withColumn(column, trim(lower(col(column))))

# Funcion para limpiar moneda
def clean_currency(df, column):
    return df.withColumn(
        column,
        regexp_replace(regexp_replace(col(column), "\\$", ""), ",", "").cast("float")
    )

# Funcion para imputar con moda
def impute_mode(df, column):
    mode_value = (
        df.filter(col(column).isNotNull())
        .groupBy(column)
        .count()
        .orderBy(col("count").desc())
        .first()[column]
    )
    return df.fillna({column: mode_value})

# Cargar datos
users_spark = spark.read.option("header", True).option("inferSchema", True).csv(f"{INPUT_PATH}/users_data.csv")
cards_spark = spark.read.option("header", True).option("inferSchema", True).csv(f"{INPUT_PATH}/cards_data.csv")
transactions_spark = spark.read.option("header", True).option("inferSchema", True).csv(f"{INPUT_PATH}/transactions_data.csv")

# Limpiar users
print("Limpiando users...")
users_spark = clean_string(users_spark, "gender")
users_spark = clean_string(users_spark, "address")
users_spark = clean_currency(users_spark, "per_capita_income")
users_spark = clean_currency(users_spark, "yearly_income")
users_spark = clean_currency(users_spark, "total_debt")
imputer = Imputer(inputCols=["current_age"], outputCols=["current_age"]).setStrategy("median")
users_spark = imputer.fit(users_spark).transform(users_spark)
users_spark = impute_mode(users_spark, "gender")
users_spark = users_spark.withColumn(
    "current_age",
    when(col("current_age") < 18, 18).when(col("current_age") > 120, 120).otherwise(col("current_age"))
)

# Limpiar cards
print("Limpiando cards...")
cards_spark = clean_string(cards_spark, "card_brand")
cards_spark = clean_string(cards_spark, "card_type")
cards_spark = clean_currency(cards_spark, "credit_limit")
imputer = Imputer(inputCols=["num_cards_issued"], outputCols=["num_cards_issued"]).setStrategy("median")
cards_spark = imputer.fit(cards_spark).transform(cards_spark)
cards_spark = impute_mode(cards_spark, "card_brand")

# Limpiar transactions
print("Limpiando transactions...")
transactions_spark = clean_string(transactions_spark, "use_chip")
transactions_spark = clean_string(transactions_spark, "merchant_city")
transactions_spark = transactions_spark.withColumn(
    "amount",
    regexp_replace(regexp_replace(col("amount"), "\\$", ""), ",", "").cast("float")
)
imputer = Imputer(inputCols=["amount"], outputCols=["amount"]).setStrategy("median")
transactions_spark = imputer.fit(transactions_spark).transform(transactions_spark)
transactions_spark = impute_mode(transactions_spark, "merchant_city")

# Guardar en BRONZE
print("Guardando datos en BRONZE...")
os.makedirs(OUTPUT_PATH, exist_ok=True)
users_spark.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/users.parquet")
cards_spark.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/cards.parquet")
transactions_spark.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/transactions.parquet")

print("Pipeline BRONZE completado")

# Cerrar Spark
spark.stop()
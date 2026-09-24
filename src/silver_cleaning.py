import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import GroupShuffleSplit
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    trim,
    lower,
    coalesce,
    try_to_timestamp,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType
)


# Rutas S3
BUCKET = "financial-digital-twin-bbva-022950218031-us-east-2-an"

INPUT_PATH = f"s3a://{BUCKET}/bronze"
OUTPUT_PATH = f"s3a://{BUCKET}/silver"


# Definir variables
NUMERIC_COLUMNS = [
    "num_cards_issued",
    "current_age",
    "yearly_income",
    "total_debt",
    "credit_score"
]

CATEGORICAL_COLUMNS = [
    "card_brand",
    "card_type",
    "has_chip"
]

FEATURE_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS


# Transformar users
def clean_users(users_spark):
    users_silver = users_spark

    for column in ["gender", "address"]:
        users_silver = users_silver.withColumn(
            column,
            trim(lower(col(column)))
        )

    for column in [
        "yearly_income",
        "per_capita_income",
        "total_debt"
    ]:
        users_silver = users_silver.withColumn(
            column,
            col(column).cast("double")
        )

    # Eliminar duplicados
    return users_silver.dropDuplicates(["id"])


# Transformar cards
def clean_cards(cards_spark):
    cards_silver = cards_spark

    for column in [
        "card_brand",
        "card_type",
        "has_chip",
        "card_on_dark_web"
    ]:
        cards_silver = cards_silver.withColumn(
            column,
            trim(lower(col(column)))
        )

    cards_silver = cards_silver.withColumn(
        "credit_limit",
        col("credit_limit").cast("double")
    )

    # Eliminar duplicados
    return cards_silver.dropDuplicates(["id"])


# Transformar transactions
def clean_transactions(transactions_spark):
    transactions_silver = transactions_spark

    for column in [
        "use_chip",
        "merchant_city",
        "merchant_state"
    ]:
        transactions_silver = transactions_silver.withColumn(
            column,
            trim(lower(col(column)))
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
    return transactions_silver.dropDuplicates(["id"])


# Crear modelo
def create_model():
    # Preparar variables numéricas
    numeric_pipeline = Pipeline([
        (
            "imputer",
            SimpleImputer(
                strategy="median",
                keep_empty_features=True
            )
        ),
        (
            "scaler",
            StandardScaler()
        )
    ])

    # Preparar variables categóricas
    categorical_pipeline = Pipeline([
        (
            "imputer",
            SimpleImputer(
                strategy="most_frequent",
                keep_empty_features=True
            )
        ),
        (
            "encoder",
            OneHotEncoder(handle_unknown="ignore")
        )
    ])

    # Crear transformaciones
    preprocessor = ColumnTransformer([
        (
            "numeric",
            numeric_pipeline,
            NUMERIC_COLUMNS
        ),
        (
            "categorical",
            categorical_pipeline,
            CATEGORICAL_COLUMNS
        )
    ])

    return Pipeline([
        (
            "preprocessor",
            preprocessor
        ),
        (
            "knn",
            KNeighborsRegressor(
                n_neighbors=5,
                weights="distance"
            )
        )
    ])


# Estimar valores faltantes
def estimate_credit_limits(known_cards, missing_cards):
    if known_cards.empty:
        raise ValueError(
            "No hay límites de crédito conocidos para entrenar."
        )

    # Separar variables
    X = known_cards[FEATURE_COLUMNS]
    y = known_cards["credit_limit"]
    groups = known_cards["client_id"]

    selected_method = "mediana"
    model = None

    if groups.nunique() >= 2:
        # Dividir datos
        split = GroupShuffleSplit(
            n_splits=1,
            test_size=0.20,
            random_state=42
        )

        train_index, test_index = next(
            split.split(X, y, groups=groups)
        )

        X_train = X.iloc[train_index]
        X_test = X.iloc[test_index]

        y_train = y.iloc[train_index]
        y_test = y.iloc[test_index]

        if len(X_train) >= 5:
            model = create_model()

            # Entrenar modelo
            model.fit(X_train, y_train)

            # Generar predicciones
            knn_predictions = model.predict(X_test)

            median_predictions = np.full(
                len(y_test),
                y_train.median()
            )

            # Calcular errores
            knn_mae = mean_absolute_error(
                y_test,
                knn_predictions
            )

            median_mae = mean_absolute_error(
                y_test,
                median_predictions
            )

            # Comparar resultados
            results = pd.DataFrame({
                "modelo": ["KNN", "Mediana"],
                "MAE": [knn_mae, median_mae]
            })

            print(results.to_string(index=False))

            # Seleccionar método
            selected_method = (
                "knn"
                if knn_mae < median_mae
                else "mediana"
            )

    print("Método seleccionado:", selected_method)

    if selected_method == "knn":
        model.fit(X, y)

        predictions = model.predict(
            missing_cards[FEATURE_COLUMNS]
        )
    else:
        predictions = np.full(
            len(missing_cards),
            y.median()
        )

    return selected_method, np.round(predictions, 2)


def impute_credit_limits(cards_silver, users_silver):
    cards_silver = (
        cards_silver
        .withColumn(
            "credit_limit_imputed",
            lit(False)
        )
        .withColumn(
            "credit_limit_imputation_method",
            lit(None).cast("string")
        )
    )

    # Preparar información de usuarios
    users_features = users_silver.select(
        col("id").alias("client_id"),
        "current_age",
        "yearly_income",
        "total_debt",
        "credit_score"
    )

    # Unir información
    cards_features = (
        cards_silver
        .select(
            "id",
            "client_id",
            "card_brand",
            "card_type",
            "has_chip",
            "num_cards_issued",
            "credit_limit"
        )
        .join(
            users_features,
            "client_id",
            "left"
        )
    )

    # Convertir datos a pandas
    cards_pd = cards_features.toPandas()

    # Preparar valores faltantes
    cards_pd[NUMERIC_COLUMNS] = (
        cards_pd[NUMERIC_COLUMNS]
        .astype("float64")
        .replace([np.inf, -np.inf], np.nan)
    )

    for column in CATEGORICAL_COLUMNS:
        cards_pd[column] = cards_pd[column].map(
            lambda value: (
                np.nan
                if pd.isna(value) or str(value).strip() == ""
                else str(value).strip().lower()
            )
        )

    # Separar registros
    known_cards = cards_pd.loc[
        cards_pd["credit_limit"].notna()
        & cards_pd["client_id"].notna()
    ].copy()

    missing_cards = cards_pd.loc[
        cards_pd["credit_limit"].isna()
        & cards_pd["id"].notna()
        & cards_pd["client_id"].notna()
    ].copy()

    print("Tarjetas con límite conocido:", len(known_cards))
    print("Tarjetas con límite faltante:", len(missing_cards))

    if missing_cards.empty:
        print("No hay límites de crédito para imputar")
        return cards_silver

    if known_cards.empty:
        print("No hay límites de crédito conocidos para imputar")
        return cards_silver

    # Estimar valores faltantes
    selected_method, predictions = estimate_credit_limits(
        known_cards,
        missing_cards
    )

    # Preparar estimaciones
    estimated_cards = missing_cards[
        ["id", "client_id"]
    ].copy()

    estimated_cards["credit_limit_estimated"] = predictions

    # Convertir estimaciones a Spark
    estimated_schema = StructType([
        StructField("id", LongType(), False),
        StructField(
            "credit_limit_estimated",
            DoubleType(),
            False
        )
    ])

    estimated_rows = [
        (
            int(row.id),
            float(row.credit_limit_estimated)
        )
        for row in estimated_cards.itertuples(index=False)
    ]

    estimated_spark = cards_silver.sparkSession.createDataFrame(
        estimated_rows,
        schema=estimated_schema
    )

    # Actualizar cards
    cards_silver = (
        cards_silver
        .join(
            estimated_spark,
            "id",
            "left"
        )
        .withColumn(
            "credit_limit_imputed",
            col("credit_limit").isNull()
            & col("credit_limit_estimated").isNotNull()
        )
        .withColumn(
            "credit_limit_imputation_method",
            when(
                col("credit_limit_imputed"),
                lit(selected_method)
            ).otherwise(lit(None).cast("string"))
        )
        .withColumn(
            "credit_limit",
            coalesce(
                col("credit_limit"),
                col("credit_limit_estimated")
            )
        )
        .drop("credit_limit_estimated")
    )

    print("Límites de crédito completados")

    return cards_silver


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

    users_silver = clean_users(users_spark)
    cards_silver = clean_cards(cards_spark)
    transactions_silver = clean_transactions(transactions_spark)

    cards_silver = impute_credit_limits(
        cards_silver,
        users_silver
    )

    # Verificar valores faltantes
    for name, df in [
        ("users", users_silver),
        ("cards", cards_silver),
        ("transactions", transactions_silver)
    ]:
        nulls = df.select([
            count(
                when(col(column).isNull(), column)
            ).alias(column)
            for column in df.columns
        ])

        print(f"Valores faltantes en {name}:")
        nulls.show()

    # Guardar SILVER
    users_silver.write.mode("overwrite").parquet(
        f"{output_path}/users.parquet"
    )

    cards_silver.write.mode("overwrite").parquet(
        f"{output_path}/cards.parquet"
    )

    transactions_silver.write.mode("overwrite").parquet(
        f"{output_path}/transactions.parquet"
    )

    print("Pipeline SILVER completado")


def main():
    # Crear sesión de Spark
    spark = (
        SparkSession.builder
        .appName("FinancialDigitalTwin_Silver")
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
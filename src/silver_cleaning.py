import os
from pathlib import Path

import numpy as np
import pandas as pd

from dotenv import load_dotenv
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import GroupShuffleSplit
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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


# Cargar variables de entorno
PROJECT_PATH = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_PATH / ".env", override=False)


# Rutas S3
BUCKET = os.getenv("S3_BUCKET", "").strip()

if not BUCKET:
    raise RuntimeError("Configura S3_BUCKET en .env.")

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
def exigir(nombre, df, condicion):
    errores = df.filter(
        ~F.coalesce(condicion, F.lit(False))
    )

    if errores.limit(1).count():
        errores.show(5, truncate=False)

        raise ValueError(
            f"{nombre}: existen registros inconsistentes."
        )

def exigir_clave(nombre, df, clave):
    exigir(
        nombre,
        df,
        F.col(clave).isNotNull()
    )

    if (
        df.groupBy(clave)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError(
            f"{nombre}: existen claves duplicadas."
        )

def exigir_referencia(nombre, df, referencia, columnas):
    if (
        df.join(
            referencia.select(*columnas).distinct(),
            columnas,
            "left_anti"
        )
        .limit(1)
        .count()
    ):
        raise ValueError(
            f"{nombre}: existen referencias inválidas."
        )

def clean_transactions(transactions_spark):
    # Transformar transactions

    transactions_silver = transactions_spark

    for column in [
        "use_chip",
        "merchant_city",
        "merchant_state",
        "movement_type",
        "category",
        "direction",
        "income_source",
        "amount_convention"
    ]:
        transactions_silver = transactions_silver.withColumn(
            column,
            F.trim(F.lower(F.col(column)))
        )

    for column in [
        "amount",
        "balance_before",
        "balance_after"
    ]:
        transactions_silver = transactions_silver.withColumn(
            column,
            F.col(column).cast("decimal(18, 2)")
        )

    # Convertir fecha de transactions

    transactions_silver = transactions_silver.withColumn(
        "date",
        F.coalesce(
            F.try_to_timestamp(
                F.col("date").cast("string"),
                F.lit("yyyy-MM-dd HH:mm:ss")
            ),
            F.try_to_timestamp(
                F.col("date").cast("string"),
                F.lit("dd/MM/yyyy HH:mm")
            ),
            F.try_to_timestamp(
                F.col("date").cast("string"),
                F.lit("MM/dd/yyyy HH:mm")
            )
        )
    )

    # No ocultar movimientos distintos con el mismo identificador
    transactions_silver = transactions_silver.dropDuplicates()

    movimientos = transactions_silver.filter(
        F.col("amount_convention") == "signed_cashflow"
    )

    exigir_clave("Movements", movimientos, "id")

    ids_movimientos = movimientos.select("id")

    anteriores = transactions_silver.filter(
        F.col("amount_convention") == "legacy_unchanged"
    )

    if anteriores.join(ids_movimientos, "id", "inner").limit(1).count():
        raise ValueError("Un ID aparece en las dos convenciones de transacciones.")

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


def prepare_financial_data(
    accounts_spark,
    obligations_spark,
    transactions_silver,
    transactions_rejected,
    clientes
):
    # Preparar cuentas

    accounts_silver = accounts_spark.dropDuplicates()

    for columna in ["opened_at", "balance_as_of"]:
        accounts_silver = accounts_silver.withColumn(
            columna,
            F.try_to_timestamp(
                F.col(columna),
                F.lit("yyyy-MM-dd HH:mm:ss")
            )
        )

    exigir_clave(
        "Accounts",
        accounts_silver,
        "account_id"
    )

    exigir_referencia(
        "Accounts",
        accounts_silver,
        clientes,
        ["client_id"]
    )

    exigir(
        "Accounts",
        accounts_silver,
        F.col("opening_balance").isNotNull()
        & F.col("closing_balance").isNotNull()
        & F.col("currency").isNotNull()
        & (F.col("opened_at") <= F.col("balance_as_of"))
    )


    # Validar convenciones

    exigir(
        "Transactions",
        transactions_silver,
        F.col("amount_convention").isin(
            "legacy_unchanged",
            "signed_cashflow"
        )
    )


    # Separar registros anteriores incompletos

    es_anterior = (
        F.col("amount_convention") == "legacy_unchanged"
    )

    datos_completos = (
        F.col("id").isNotNull()
        & (F.col("id") >= 0)
        & F.col("date").isNotNull()
        & F.col("amount").isNotNull()
    )

    incompletos = (
        transactions_silver
        .filter(
            es_anterior
            & ~F.coalesce(datos_completos, F.lit(False))
        )
        .withColumn(
            "rejection_reason",
            F.lit("ID, fecha o importe inválido")
        )
    )

    # Homologar date para unirlo con los rechazados anteriores

    transactions_rejected = (
        transactions_rejected
        .withColumn("date", F.col("date").cast("string"))
        .unionByName(
            incompletos.withColumn(
                "date",
                F.col("date").cast("string")
            ),
            allowMissingColumns=True
        )
    )

    transactions_silver = transactions_silver.filter(
        ~es_anterior
        | F.coalesce(datos_completos, F.lit(False))
    )




    # Seleccionar movimientos de cuenta

    movements_silver = transactions_silver.filter(
        F.col("amount_convention") == "signed_cashflow"
    )

    exigir_clave(
        "Movements",
        movements_silver,
        "id"
    )

    exigir_referencia(
        "Movements",
        movements_silver,
        accounts_silver,
        ["account_id", "client_id", "currency"]
    )

    exigir(
        "Movements",
        movements_silver,
        (F.col("id") >= 0)
        & F.col("date").isNotNull()
        & F.col("amount").isNotNull()
        & F.col("balance_before").isNotNull()
        & F.col("balance_after").isNotNull()
        & F.col("movement_type").isin(
            "income",
            "expense",
            "debt_payment",
            "transfer_in",
            "transfer_out"
        )
        & (
            F.col("balance_after")
            == F.col("balance_before") + F.col("amount")
        )
    )

    es_entrada = F.col("movement_type").isin(
        "income",
        "transfer_in"
    )

    exigir(
        "Dirección de movimientos",
        movements_silver,
        F.when(
            es_entrada,
            (F.col("amount") > 0)
            & (F.col("direction") == "inflow")
        ).otherwise(
            (F.col("amount") < 0)
            & (F.col("direction") == "outflow")
        )
    )


    # Validar continuidad de saldos

    orden = Window.partitionBy(
        "account_id"
    ).orderBy("date", "id")

    orden_inverso = Window.partitionBy(
        "account_id"
    ).orderBy(
        F.col("date").desc(),
        F.col("id").desc()
    )

    revision_saldos = (
        movements_silver
        .withColumn(
            "previous_balance",
            F.lag("balance_after").over(orden)
        )
        .withColumn(
            "first_position",
            F.row_number().over(orden)
        )
        .withColumn(
            "last_position",
            F.row_number().over(orden_inverso)
        )
        .join(
            accounts_silver.select(
                "account_id",
                "opening_balance",
                "closing_balance",
                "opened_at",
                "balance_as_of"
            ),
            "account_id"
        )
    )

    exigir(
        "Continuidad de saldos",
        revision_saldos,
        (
            F.col("balance_before")
            == F.when(
                F.col("first_position") == 1,
                F.col("opening_balance")
            ).otherwise(F.col("previous_balance"))
        )
        & (
            (F.col("last_position") != 1)
            | (
                F.col("balance_after")
                == F.col("closing_balance")
            )
        )
        & (F.col("date") >= F.col("opened_at"))
        & (F.col("date") <= F.col("balance_as_of"))
    )


    # Validar cuentas sin movimientos

    sin_movimientos = accounts_silver.join(
        movements_silver.select("account_id").distinct(),
        "account_id",
        "left_anti"
    )

    exigir(
        "Cuentas sin movimientos",
        sin_movimientos,
        F.col("opening_balance") == F.col("closing_balance")
    )


    # Validar transferencias

    es_transferencia = F.col("movement_type").isin(
        "transfer_in",
        "transfer_out"
    )

    exigir(
        "Identificadores de transferencias",
        movements_silver,
        F.when(
            es_transferencia,
            F.col("transfer_id").isNotNull()
            & F.col("counterparty_account_id").isNotNull()
        ).otherwise(
            F.col("transfer_id").isNull()
            & F.col("counterparty_account_id").isNull()
        )
    )

    transferencias = movements_silver.filter(
        es_transferencia
    )

    revision_transferencias = transferencias.groupBy(
        "transfer_id"
    ).agg(
        F.count("*").alias("legs"),
        F.sum("amount").alias("net_amount"),
        F.countDistinct("account_id").alias("accounts"),
        F.countDistinct("date").alias("dates"),
        F.countDistinct("currency").alias("currencies"),
        F.countDistinct("movement_type").alias("types"),
        F.sort_array(
            F.collect_set("account_id")
        ).alias("owners"),
        F.sort_array(
            F.collect_set("counterparty_account_id")
        ).alias("counterparties")
    )

    exigir(
        "Transferencias",
        revision_transferencias,
        (F.col("legs") == 2)
        & (F.col("net_amount") == 0)
        & (F.col("accounts") == 2)
        & (F.col("dates") == 1)
        & (F.col("currencies") == 1)
        & (F.col("types") == 2)
        & (F.col("owners") == F.col("counterparties"))
    )

    print("Cuentas y movimientos aprobados")

    # Preparar obligaciones

    obligations_silver = obligations_spark.dropDuplicates()

    for columna in [
        "contract_start_date",
        "due_date",
        "payment_date",
        "as_of_date"
    ]:
        obligations_silver = obligations_silver.withColumn(
            columna,
            F.to_date(
                F.try_to_timestamp(
                    F.col(columna),
                    F.lit("yyyy-MM-dd")
                )
            )
        )

    exigir_clave(
        "Obligations",
        obligations_silver,
        "obligation_id"
    )

    exigir_referencia(
        "Obligations",
        obligations_silver,
        accounts_silver,
        ["account_id", "client_id", "currency"]
    )

    exigir(
        "Importes de obligaciones",
        obligations_silver,
        F.col("contract_id").isNotNull()
        & (F.col("installment_number") > 0)
        & (F.col("contract_principal") > 0)
        & (F.col("annual_interest_rate") >= 0)
        & (F.col("amount_due") > 0)
        & (F.col("amount_paid") >= 0)
        & (F.col("amount_paid") <= F.col("amount_due"))
        & (
            F.col("outstanding_amount")
            == F.col("amount_due") - F.col("amount_paid")
        )
        & (F.col("contract_start_date") <= F.col("due_date"))
        & (F.col("due_date") <= F.col("as_of_date"))
    )

    tiene_pago = F.col("amount_paid") > 0

    exigir(
        "Fechas de pago",
        obligations_silver,
        F.when(
            tiene_pago,
            F.col("payment_transaction_id").isNotNull()
            & (F.col("payment_date") >= F.col("contract_start_date"))
            & (F.col("payment_date") <= F.col("as_of_date"))
        ).otherwise(
            F.col("payment_transaction_id").isNull()
            & F.col("payment_date").isNull()
        )
    )


    # Comprobar movimientos de pago

    pagos = (
        movements_silver
        .filter(F.col("movement_type") == "debt_payment")
        .select(
            F.col("id").alias("payment_transaction_id"),
            "obligation_id",
            "account_id",
            "client_id",
            "currency",
            (-F.col("amount")).alias("amount_paid"),
            F.to_date("date").alias("payment_date")
        )
    )

    claves_pago = [
        "payment_transaction_id",
        "obligation_id",
        "account_id",
        "client_id",
        "currency",
        "amount_paid",
        "payment_date"
    ]

    obligaciones_pagadas = obligations_silver.filter(
        tiene_pago
    )

    exigir_referencia(
        "Pagos de obligaciones",
        obligaciones_pagadas,
        pagos,
        claves_pago
    )

    exigir_referencia(
        "Movimientos de pago",
        pagos,
        obligaciones_pagadas,
        claves_pago
    )

    exigir_clave(
        "Pago por obligación",
        pagos,
        "obligation_id"
    )


    # Calcular atraso a la fecha de corte

    obligations_silver = (
        obligations_silver
        .withColumn(
            "days_past_due_as_of",
            F.when(
                F.col("outstanding_amount") > 0,
                F.greatest(
                    F.datediff("as_of_date", "due_date"),
                    F.lit(0)
                )
            ).otherwise(0)
        )
        .withColumn(
            "days_to_full_payment",
            F.when(
                F.col("amount_paid") == F.col("amount_due"),
                F.greatest(
                    F.datediff("payment_date", "due_date"),
                    F.lit(0)
                )
            )
        )
    )

    print("Obligaciones y pagos aprobados")

    return (
        transactions_silver,
        accounts_silver,
        obligations_silver,
        transactions_rejected
    )


def run_pipeline(spark, input_path=INPUT_PATH, output_path=OUTPUT_PATH):
    # Cargar datasets
    users_spark = spark.read.parquet(f"{input_path}/users.parquet")
    cards_spark = spark.read.parquet(f"{input_path}/cards.parquet")
    transactions_spark = spark.read.parquet(f"{input_path}/transactions.parquet")
    accounts_spark = spark.read.parquet(f"{input_path}/accounts.parquet")
    obligations_spark = spark.read.parquet(f"{input_path}/obligations_payments.parquet")

    # Limpiar usuarios
    users_silver = clean_users(users_spark)

    clientes = (
        users_silver
        .filter(col("id").isNotNull() & (col("id") >= 0))
        .select(col("id").alias("client_id"))
        .distinct()
    )

    # Separar tarjetas rechazadas
    cards_invalid_id = (
        cards_spark
        .filter(col("id").isNull() | (col("id") < 0))
        .withColumn("rejection_reason", lit("ID de tarjeta nulo o negativo"))
    )

    cards_valid_id = cards_spark.filter(
        col("id").isNotNull() & (col("id") >= 0)
    )

    cards_without_client = (
        cards_valid_id
        .join(clientes, "client_id", "left_anti")
        .withColumn("rejection_reason", lit("Cliente inexistente"))
    )

    cards_rejected = cards_invalid_id.unionByName(cards_without_client)
    cards_valid = cards_valid_id.join(clientes, "client_id", "left_semi")

    # Validar antes de separar transacciones
    exigir(
        "Transactions",
        transactions_spark,
        F.col("amount_convention").isin("legacy_unchanged", "signed_cashflow")
    )

    exigir_referencia(
        "Movimientos de cuenta",
        transactions_spark.filter(F.col("amount_convention") == "signed_cashflow"),
        clientes,
        ["client_id"]
    )

    transactions_rejected = (
        transactions_spark
        .join(clientes, "client_id", "left_anti")
        .withColumn("rejection_reason", lit("Cliente inexistente"))
    )

    transactions_valid = transactions_spark.join(
        clientes, "client_id", "left_semi"
    )

    # Transformar registros aceptados
    cards_silver = clean_cards(cards_valid)
    transactions_silver = clean_transactions(transactions_valid)

    # Validar cuentas, saldos, transferencias y pagos
    (
        transactions_silver,
        accounts_silver,
        obligations_silver,
        transactions_rejected
    ) = prepare_financial_data(
        accounts_spark,
        obligations_spark,
        transactions_silver,
        transactions_rejected,
        clientes
    )

    # Completar límites de crédito
    cards_silver = impute_credit_limits(cards_silver, users_silver)

    # Guardar resultados después de las validaciones
    datasets = {
        "users": users_silver,
        "cards": cards_silver,
        "transactions": transactions_silver,
        "accounts": accounts_silver,
        "obligations_payments": obligations_silver,
        "rejected/cards": cards_rejected,
        "rejected/transactions": transactions_rejected
    }

    for nombre, datos in datasets.items():
        datos.write.mode("overwrite").parquet(
            f"{output_path}/{nombre}.parquet"
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
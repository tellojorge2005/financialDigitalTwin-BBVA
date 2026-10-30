from decimal import Decimal
from datetime import datetime

from pyspark.sql import functions as F
from src.silver_cleaning import prepare_financial_data

import numpy as np
import pandas as pd
import pytest

from pyspark.sql.functions import date_format

from src.silver_cleaning import (
    clean_users,
    clean_cards,
    clean_transactions,
    estimate_credit_limits,
    impute_credit_limits,
    run_pipeline
)


@pytest.fixture
def users_data(spark):
    return spark.createDataFrame(
        [
            (
                1, 30.0, " Male ", " New York ",
                30000.0, 60000.0, 10000.0, 750
            ),
            (
                2, 40.0, " Female ", " Boston ",
                40000.0, 80000.0, 20000.0, 720
            ),
            (
                3, 50.0, " Male ", " Miami ",
                25000.0, 50000.0, 15000.0, 700
            )
        ],
        """
        id long,
        current_age double,
        gender string,
        address string,
        per_capita_income double,
        yearly_income double,
        total_debt double,
        credit_score long
        """
    )


@pytest.fixture
def cards_schema():
    return """
        id long,
        client_id long,
        card_brand string,
        card_type string,
        has_chip string,
        card_on_dark_web string,
        num_cards_issued double,
        credit_limit double
    """


# Validar limpieza de usuarios
def test_clean_users(users_data):
    duplicated = users_data.unionByName(users_data)
    cleaned = clean_users(duplicated)

    rows = {
        row.id: row
        for row in cleaned.collect()
    }

    assert len(rows) == 3
    assert rows[1].gender == "male"
    assert rows[1].address == "new york"

    for column in [
        "yearly_income",
        "per_capita_income",
        "total_debt"
    ]:
        assert (
            cleaned.schema[column].dataType.simpleString()
            == "double"
        )


# Validar limpieza de tarjetas
def test_clean_cards(spark, cards_schema):
    df = spark.createDataFrame(
        [
            (
                1, 1, " VISA ", " Credit ",
                " YES ", " NO ", 1.0, 10000.0
            ),
            (
                1, 1, " VISA ", " Credit ",
                " YES ", " NO ", 1.0, 10000.0
            )
        ],
        cards_schema
    )

    cleaned = clean_cards(df)
    rows = cleaned.collect()

    assert len(rows) == 1
    assert rows[0].card_brand == "visa"
    assert rows[0].card_type == "credit"
    assert rows[0].has_chip == "yes"
    assert rows[0].card_on_dark_web == "no"
    assert (
        cleaned.schema["credit_limit"].dataType.simpleString()
        == "double"
    )


# Validar limpieza de transacciones


# Validar imputación de límites
def test_impute_credit_limits(spark, users_data, cards_schema):
    df = spark.createDataFrame(
        [
            (1, 1, "visa", "credit", "yes", "no", 1.0, 1000.0),
            (2, 2, "visa", "credit", "yes", "no", 1.0, 3000.0),
            (3, 3, "visa", "credit", "yes", "no", 1.0, None),
            (4, None, "visa", "credit", "yes", "no", 1.0, None),
            (None, 3, "visa", "credit", "yes", "no", 1.0, None)
        ],
        cards_schema
    )

    cleaned = impute_credit_limits(
        clean_cards(df),
        clean_users(users_data)
    )

    rows = {
        row.id: row
        for row in cleaned.collect()
    }

    assert len(rows) == 5

    assert rows[1].credit_limit == 1000.0
    assert rows[2].credit_limit == 3000.0
    assert rows[1].credit_limit_imputed is False
    assert rows[1].credit_limit_imputation_method is None

    assert rows[3].credit_limit == 2000.0
    assert rows[3].credit_limit_imputed is True
    assert rows[3].credit_limit_imputation_method == "mediana"

    assert rows[4].credit_limit is None
    assert rows[4].credit_limit_imputed is False

    assert rows[None].credit_limit is None
    assert rows[None].credit_limit_imputed is False


def test_imputation_without_missing_limits(
    spark,
    users_data,
    cards_schema
):
    df = spark.createDataFrame(
        [
            (1, 1, "visa", "credit", "yes", "no", 1.0, 1000.0)
        ],
        cards_schema
    )

    row = impute_credit_limits(
        clean_cards(df),
        clean_users(users_data)
    ).first()

    assert row.credit_limit == 1000.0
    assert row.credit_limit_imputed is False
    assert row.credit_limit_imputation_method is None


def test_imputation_without_known_limits(
    spark,
    users_data,
    cards_schema
):
    df = spark.createDataFrame(
        [
            (1, 1, "visa", "credit", "yes", "no", 1.0, None)
        ],
        cards_schema
    )

    row = impute_credit_limits(
        clean_cards(df),
        clean_users(users_data)
    ).first()

    assert row.credit_limit is None
    assert row.credit_limit_imputed is False
    assert row.credit_limit_imputation_method is None


# Validar selección del modelo
def test_estimate_credit_limits_knn():
    known_cards = pd.DataFrame([
        {
            "client_id": index,
            "num_cards_issued": 1.0,
            "current_age": 30.0 if index % 2 == 0 else 60.0,
            "yearly_income": (
                20000.0 if index % 2 == 0 else 100000.0
            ),
            "total_debt": (
                1000.0 if index % 2 == 0 else 10000.0
            ),
            "credit_score": 600.0 if index % 2 == 0 else 800.0,
            "card_brand": "visa",
            "card_type": "credit",
            "has_chip": "yes",
            "credit_limit": 1000.0 if index % 2 == 0 else 9000.0
        }
        for index in range(40)
    ])

    missing_cards = known_cards.iloc[[0, 1]].copy()
    missing_cards["credit_limit"] = np.nan

    method, predictions = estimate_credit_limits(
        known_cards,
        missing_cards
    )

    assert method == "knn"
    np.testing.assert_allclose(
        predictions,
        [1000.0, 9000.0]
    )


def test_estimate_credit_limits_median():
    known_cards = pd.DataFrame([
        {
            "client_id": index,
            "num_cards_issued": 1.0,
            "current_age": 30.0,
            "yearly_income": 60000.0,
            "total_debt": 10000.0,
            "credit_score": 750.0,
            "card_brand": "visa",
            "card_type": "credit",
            "has_chip": "yes",
            "credit_limit": 2000.0
        }
        for index in range(20)
    ])

    missing_cards = known_cards.iloc[[0]].copy()
    missing_cards["credit_limit"] = np.nan

    method, predictions = estimate_credit_limits(
        known_cards,
        missing_cards
    )

    assert method == "mediana"
    np.testing.assert_allclose(predictions, [2000.0])


def test_estimate_credit_limits_single_client():
    known_cards = pd.DataFrame({
        "client_id": [1, 1],
        "num_cards_issued": [1.0, 2.0],
        "current_age": [30.0, 30.0],
        "yearly_income": [60000.0, 60000.0],
        "total_debt": [10000.0, 10000.0],
        "credit_score": [750.0, 750.0],
        "card_brand": ["visa", "visa"],
        "card_type": ["credit", "credit"],
        "has_chip": ["yes", "yes"],
        "credit_limit": [1000.0, 3000.0]
    })

    missing_cards = known_cards.iloc[[0]].copy()
    missing_cards["credit_limit"] = np.nan

    method, predictions = estimate_credit_limits(
        known_cards,
        missing_cards
    )

    assert method == "mediana"
    np.testing.assert_allclose(predictions, [2000.0])


# Validar separación de rechazados en el pipeline


# Validar fechas y precisión de los movimientos
def test_clean_transactions(bronze_bundle):
    original = bronze_bundle["transactions"]
    df = original.unionByName(original.filter(F.col("id") == 10))
    cleaned = clean_transactions(df)
    # Comparar la fecha en Spark antes de convertirla a objetos de Python.
    # collect() convierte TimestampType a la zona local del sistema.
    rows = {r.id: r for r in cleaned.withColumn(
        "date", F.date_format("date", "yyyy-MM-dd HH:mm:ss")
    ).collect()}

    assert len(rows) == 14
    assert rows[1].date == "2009-12-15 10:00:00"
    assert rows[10].date == "2010-01-05 10:00:00"
    assert rows[3].date is None
    assert rows[11].amount == Decimal("-300.00")
    assert rows[10].merchant_city is None
    assert cleaned.schema["amount"].dataType.simpleString() == "decimal(18,2)"


@pytest.mark.parametrize(
    "texto,esperado",
    [("01/20/2010 10:00", "2010-01-20 10:00:00"),
     ("02/03/2010 12:00", "2010-03-02 12:00:00")]
)
def test_date_formats(bronze_bundle, texto, esperado):
    df = bronze_bundle["transactions"].filter(F.col("id") == 1)
    df = df.withColumn("date", F.lit(texto))
    fecha = clean_transactions(df).select(
        F.date_format("date", "yyyy-MM-dd HH:mm:ss").alias("fecha")
    ).first().fecha
    assert fecha == esperado


def test_rejects_conflicting_movement_ids(bronze_bundle):
    original = bronze_bundle["transactions"]
    conflict = original.filter(F.col("id") == 10).withColumn(
        "amount", F.lit(1001).cast("decimal(18,2)")
    )
    with pytest.raises(ValueError, match="claves duplicadas"):
        clean_transactions(original.unionByName(conflict))


def test_rejects_id_shared_between_conventions(bronze_bundle):
    original = bronze_bundle["transactions"]
    conflict = original.filter(F.col("id") == 1).withColumn("id", F.lit(10).cast("long"))
    with pytest.raises(ValueError, match="dos convenciones"):
        clean_transactions(original.unionByName(conflict))


@pytest.fixture
def financial_arguments(bronze_bundle):
    clientes = bronze_bundle["users"].select(F.col("id").alias("client_id"))
    tx = bronze_bundle["transactions"]
    rechazados = tx.join(clientes, "client_id", "left_anti").withColumn(
        "rejection_reason", F.lit("Cliente inexistente")
    )
    aceptados = clean_transactions(tx.join(clientes, "client_id", "left_semi"))
    return {
        "accounts_spark": bronze_bundle["accounts"],
        "obligations_spark": bronze_bundle["obligations_payments"],
        "transactions_silver": aceptados,
        "transactions_rejected": rechazados,
        "clientes": clientes
    }


def test_prepare_financial_data(financial_arguments):
    tx, accounts, obligations, rejected = prepare_financial_data(**financial_arguments)
    assert tx.count() == 12
    assert accounts.count() == 2
    assert {r.id for r in rejected.collect()} == {3, 4}
    rows = {r.obligation_id: r for r in obligations.collect()}
    assert rows["O-1"].days_past_due_as_of == 0
    assert rows["O-1"].days_to_full_payment == 0
    assert rows["O-2"].days_past_due_as_of == 39
    assert rows["O-2"].days_to_full_payment is None
    assert rows["O-3"].days_past_due_as_of == 75


# Introducir inconsistencias conservando los demás campos
@pytest.mark.parametrize(
    "caso,mensaje",
    [("saldo", "Continuidad de saldos"),
     ("cuenta", "referencias inválidas"),
     ("fecha", "Movements"),
     ("transferencia", "Transferencias"),
     ("pago", "Pagos de obligaciones")]
)
def test_financial_validation_rejects_errors(financial_arguments, caso, mensaje):
    args = dict(financial_arguments)
    tx = args["transactions_silver"]
    if caso == "saldo":
        args["accounts_spark"] = args["accounts_spark"].withColumn(
            "closing_balance",
            F.when(F.col("account_id") == "A-1", F.lit(2101))
            .otherwise(F.col("closing_balance")).cast("decimal(18,2)")
        )
    elif caso == "cuenta":
        args["transactions_silver"] = tx.withColumn(
            "account_id", F.when(F.col("id") == 10, "A-999").otherwise(F.col("account_id"))
        )
    elif caso == "fecha":
        args["transactions_silver"] = tx.withColumn(
            "date", F.when(F.col("id") == 10, F.lit(None).cast("timestamp"))
            .otherwise(F.col("date"))
        )
    elif caso == "transferencia":
        args["transactions_silver"] = tx.withColumn(
            "transfer_id", F.when(F.col("id") == 12, "T-incorrecta").otherwise(F.col("transfer_id"))
        )
    elif caso == "pago":
        args["obligations_spark"] = (
            args["obligations_spark"]
            .withColumn("amount_paid", F.when(F.col("obligation_id") == "O-1", F.lit(99))
                        .otherwise(F.col("amount_paid")).cast("decimal(18,2)"))
            .withColumn("outstanding_amount", F.col("amount_due") - F.col("amount_paid"))
        )

    with pytest.raises(ValueError, match=mensaje):
        prepare_financial_data(**args)


def test_pipeline_separa_rechazados(spark, tmp_path, bronze_bundle, escribir_parquet):
    entrada = escribir_parquet(bronze_bundle, tmp_path / "bronze")
    salida = tmp_path / "silver"
    run_pipeline(spark, input_path=entrada, output_path=salida.as_uri())

    def leer(nombre):
        return spark.read.parquet((salida / f"{nombre}.parquet").as_uri())

    assert leer("users").count() == 3
    assert {r.id for r in leer("cards").collect()} == {1, 2}
    assert leer("transactions").count() == 12
    assert leer("accounts").count() == 2
    assert leer("obligations_payments").count() == 3
    assert {r.id for r in leer("rejected/cards").collect()} == {None, 3}
    assert {r.id for r in leer("rejected/transactions").collect()} == {3, 4}
    assert leer("accounts").filter(F.col("account_id") == "A-1").first().closing_balance == Decimal("2100.00")


def test_pipeline_does_not_drop_orphan_account_movements(
    spark, tmp_path, bronze_bundle, escribir_parquet
):
    datos = dict(bronze_bundle)
    datos["transactions"] = datos["transactions"].withColumn(
        "client_id", F.when(F.col("id") == 10, F.lit(999).cast("long"))
        .otherwise(F.col("client_id"))
    )
    entrada = escribir_parquet(datos, tmp_path / "bronze")
    salida = tmp_path / "silver"
    with pytest.raises(ValueError, match="referencias inválidas"):
        run_pipeline(spark, input_path=entrada, output_path=salida.as_uri())
    assert not salida.exists()

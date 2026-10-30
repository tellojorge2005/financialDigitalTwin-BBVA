from decimal import Decimal

from pyspark.sql import functions as F
from src.bronze_cleaning import clean_accounts, clean_obligations, run_pipeline

import pytest

from src.bronze_cleaning import (
    clean_string,
    clean_currency,
    impute_mode,
    impute_median,
    clean_users,
    clean_cards,
    clean_transactions
)


def test_clean_string(spark):
    df = spark.createDataFrame(
        [(" Male ",), (" FEMALE ",), (None,)],
        "gender string"
    )

    rows = clean_string(df, "gender").collect()

    assert [row.gender for row in rows] == [
        "male",
        "female",
        None
    ]


def test_clean_currency(spark):
    df = spark.createDataFrame(
        [("$80,000",), ("$-1,250.50",), (None,)],
        "income string"
    )

    cleaned = clean_currency(df, "income")
    rows = cleaned.collect()

    assert rows[0].income == pytest.approx(80000.0)
    assert rows[1].income == pytest.approx(-1250.50)
    assert rows[2].income is None
    assert cleaned.schema["income"].dataType.simpleString() == "float"


def test_impute_mode(spark):
    df = spark.createDataFrame(
        [("male",), ("male",), ("female",), (None,)],
        "gender string"
    )

    values = [
        row.gender
        for row in impute_mode(df, "gender").collect()
    ]

    assert values.count("male") == 3
    assert None not in values


def test_impute_mode_without_values(spark):
    df = spark.createDataFrame(
        [(None,), (None,)],
        "gender string"
    )

    rows = impute_mode(df, "gender").collect()

    assert len(rows) == 2
    assert all(row.gender is None for row in rows)


def test_impute_median(spark):
    df = spark.createDataFrame(
        [(1, 20.0), (2, 40.0), (3, 60.0), (4, None)],
        "id long, current_age double"
    )

    rows = {
        row.id: row
        for row in impute_median(df, "current_age").collect()
    }

    assert rows[4].current_age == 40.0
    assert rows[1].current_age == 20.0


def test_impute_median_without_values(spark):
    df = spark.createDataFrame(
        [(None,), (None,)],
        "current_age double"
    )

    rows = impute_median(df, "current_age").collect()

    assert all(row.current_age is None for row in rows)


def test_clean_users(spark):
    df = spark.createDataFrame(
        [
            (1, 10.0, " Male ", " New York ", "$30,000", "$60,000", "$5,000", 200),
            (2, 130.0, "male", " Chicago ", "$40,000", "$80,000", "$8,000", 900),
            (3, 40.0, "Female", " Miami ", "$35,000", "$70,000", "$6,000", 700),
            (4, None, None, " Boston ", "$20,000", "$40,000", "$2,000", None)
        ],
        """
        id long,
        current_age double,
        gender string,
        address string,
        per_capita_income string,
        yearly_income string,
        total_debt string,
        credit_score long
        """
    )

    rows = {
        row.id: row
        for row in clean_users(df).collect()
    }

    assert rows[1].current_age == 18
    assert rows[2].current_age == 120
    assert rows[4].current_age == 40
    assert rows[4].gender == "male"
    assert rows[1].address == "new york"
    assert rows[1].yearly_income == 60000.0
    assert rows[1].credit_score == 300
    assert rows[2].credit_score == 850
    assert rows[4].credit_score is None


def test_clean_cards(spark):
    df = spark.createDataFrame(
        [
            (1, " VISA ", " Credit ", " YES ", " NO ", "$-100", 1.0),
            (2, "visa", "debit", "no", "yes", "$2,000", 3.0),
            (3, None, " Credit ", " YES ", " NO ", None, None)
        ],
        """
        id long,
        card_brand string,
        card_type string,
        has_chip string,
        card_on_dark_web string,
        credit_limit string,
        num_cards_issued double
        """
    )

    rows = {
        row.id: row
        for row in clean_cards(df).collect()
    }

    assert rows[1].credit_limit == 0.0
    assert rows[2].credit_limit == 2000.0
    assert rows[3].credit_limit is None
    assert rows[3].num_cards_issued == 1.0
    assert rows[3].card_brand == "visa"
    assert rows[1].card_type == "credit"
    assert rows[1].has_chip == "yes"
    assert rows[1].card_on_dark_web == "no"




# Validar que los movimientos no se recorten ni se imputen
def test_clean_transactions(raw_bundle):
    df = raw_bundle["transactions"].withColumn(
        "amount",
        F.when(F.col("id") == "10", "$20,000.25")
        .when(F.col("id") == "11", "-$20,000.25")
        .otherwise(F.col("amount"))
    )

    cleaned = clean_transactions(df)
    rows = {r.id: r for r in cleaned.collect()}

    assert rows[10].amount == Decimal("20000.25")
    assert rows[11].amount == Decimal("-20000.25")
    assert rows[3].amount is None
    assert rows[10].merchant_city is None
    assert rows[10].card_id is None
    assert rows[10].account_id == "A-1"
    assert rows[10].currency == "USD"
    assert rows[10].balance_after == Decimal("1100.00")
    assert rows[1].merchant_city == "city"
    assert rows[1].use_chip == "chip"
    assert cleaned.schema["amount"].dataType.simpleString() == "decimal(18,2)"


def test_invalid_amount_remains_missing(raw_bundle):
    df = raw_bundle["transactions"].withColumn("amount", F.lit("incorrecto"))
    assert clean_transactions(df).filter(F.col("amount").isNotNull()).count() == 0


def test_clean_accounts(raw_bundle):
    rows = {r.account_id: r for r in clean_accounts(raw_bundle["accounts"]).collect()}
    assert rows["A-1"].client_id == 1
    assert rows["A-1"].opening_balance == Decimal("100.00")
    assert rows["A-1"].closing_balance == Decimal("2100.00")
    assert rows["A-1"].overdraft_allowed is True
    assert rows["A-1"].currency == "USD"


def test_clean_obligations(raw_bundle):
    rows = {
        r.obligation_id: r
        for r in clean_obligations(raw_bundle["obligations_payments"]).collect()
    }
    assert rows["O-2"].amount_due == Decimal("150.00")
    assert rows["O-2"].amount_paid == Decimal("50.00")
    assert rows["O-2"].outstanding_amount == Decimal("100.00")
    assert rows["O-2"].payment_transaction_id == 17
    assert rows["O-2"].days_to_full_payment is None
    assert rows["O-3"].payment_date is None
    assert rows["O-3"].payment_transaction_id is None


# Validar las cinco entradas CSV y sus salidas locales
def test_run_pipeline(spark, tmp_path, raw_bundle):
    entrada = tmp_path / "incoming"
    salida = tmp_path / "bronze"

    for nombre, df in raw_bundle.items():
        df.coalesce(1).write.option("header", True).csv(
            (entrada / f"{nombre}_data.csv").as_uri()
        )

    run_pipeline(spark, input_path=entrada.as_uri(), output_path=salida.as_uri())

    expected = {"users": 3, "cards": 4, "transactions": 14,
                "accounts": 2, "obligations_payments": 3}
    for nombre, cantidad in expected.items():
        datos = spark.read.parquet((salida / f"{nombre}.parquet").as_uri())
        assert datos.count() == cantidad

    tx = spark.read.parquet((salida / "transactions.parquet").as_uri())
    assert tx.filter(F.col("id") == 3).first().amount is None
    assert tx.filter(F.col("id") == 10).first().balance_after == Decimal("1100.00")

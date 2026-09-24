from datetime import datetime
from pyspark.sql.functions import date_format

import numpy as np
import pandas as pd
import pytest

from src.silver_cleaning import (
    clean_users,
    clean_cards,
    clean_transactions,
    estimate_credit_limits,
    impute_credit_limits
)


@pytest.fixture
def users_data(spark):
    return spark.createDataFrame(
        [
            (1, 30.0, " Male ", " New York ", 30000.0, 60000.0, 10000.0, 750),
            (2, 40.0, " Female ", " Boston ", 40000.0, 80000.0, 20000.0, 720),
            (3, 50.0, " Male ", " Miami ", 25000.0, 50000.0, 15000.0, 700)
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
        assert cleaned.schema[column].dataType.simpleString() == "double"


def test_clean_cards(spark, cards_schema):
    df = spark.createDataFrame(
        [
            (1, 1, " VISA ", " Credit ", " YES ", " NO ", 1.0, 10000.0),
            (1, 1, " VISA ", " Credit ", " YES ", " NO ", 1.0, 10000.0)
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
    assert cleaned.schema["credit_limit"].dataType.simpleString() == "double"


def test_clean_transactions(spark):
    df = spark.createDataFrame(
        [
            (1, "15/01/2010 08:13", 50.0, " Swipe ", " New York ", " NY "),
            (2, "01/20/2010 10:00", 75.0, " Chip ", " Boston ", " MA "),
            (3, "02/03/2010 12:00", -10.0, " Swipe ", " Miami ", " FL "),
            (4, "fecha incorrecta", 25.0, " Chip ", " Boston ", " MA "),
            (1, "15/01/2010 08:13", 50.0, " Swipe ", " New York ", " NY ")
        ],
        """
        id long,
        date string,
        amount double,
        use_chip string,
        merchant_city string,
        merchant_state string
        """
    )

    cleaned = clean_transactions(df)

    assert cleaned.schema["date"].dataType.simpleString() == "timestamp"

    rows = {
        row.id: row
        for row in cleaned.withColumn(
            "date",
            date_format("date", "yyyy-MM-dd HH:mm")
        ).collect()
    }

    assert len(rows) == 4
    assert rows[1].date == "2010-01-15 08:13"
    assert rows[2].date == "2010-01-20 10:00"
    assert rows[3].date == "2010-03-02 12:00"
    assert rows[4].date is None
    assert rows[1].use_chip == "swipe"
    assert rows[1].merchant_city == "new york"
    assert rows[1].merchant_state == "ny"
    assert rows[3].amount == -10.0


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


def test_imputation_without_missing_limits(spark, users_data, cards_schema):
    df = spark.createDataFrame(
        [(1, 1, "visa", "credit", "yes", "no", 1.0, 1000.0)],
        cards_schema
    )

    row = impute_credit_limits(
        clean_cards(df),
        clean_users(users_data)
    ).first()

    assert row.credit_limit == 1000.0
    assert row.credit_limit_imputed is False
    assert row.credit_limit_imputation_method is None


def test_imputation_without_known_limits(spark, users_data, cards_schema):
    df = spark.createDataFrame(
        [(1, 1, "visa", "credit", "yes", "no", 1.0, None)],
        cards_schema
    )

    row = impute_credit_limits(
        clean_cards(df),
        clean_users(users_data)
    ).first()

    assert row.credit_limit is None
    assert row.credit_limit_imputed is False
    assert row.credit_limit_imputation_method is None


def test_estimate_credit_limits_knn():
    known_cards = pd.DataFrame([
        {
            "client_id": index,
            "num_cards_issued": 1.0,
            "current_age": 30.0 if index % 2 == 0 else 60.0,
            "yearly_income": 20000.0 if index % 2 == 0 else 100000.0,
            "total_debt": 1000.0 if index % 2 == 0 else 10000.0,
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
    np.testing.assert_allclose(predictions, [1000.0, 9000.0])


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
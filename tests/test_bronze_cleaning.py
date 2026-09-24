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


def test_clean_transactions(spark):
    df = spark.createDataFrame(
        [
            (1, "$-20,000", " Swipe ", " Miami ", " FL ", " 33101 ", " ERROR "),
            (2, "$20,000", " Chip ", "miami", " FL ", "33101", None),
            (3, "$50", " Swipe ", " Boston ", " MA ", "02108", None),
            (4, None, " Chip ", None, " FL ", None, None)
        ],
        """
        id long,
        amount string,
        use_chip string,
        merchant_city string,
        merchant_state string,
        zip string,
        errors string
        """
    )

    rows = {
        row.id: row
        for row in clean_transactions(df).collect()
    }

    assert rows[1].amount == -10000.0
    assert rows[2].amount == 10000.0
    assert rows[4].amount == 50.0
    assert rows[4].merchant_city == "miami"
    assert rows[1].use_chip == "swipe"
    assert rows[1].merchant_state == "fl"
    assert rows[1].zip == "33101"
    assert rows[1].errors == "error"
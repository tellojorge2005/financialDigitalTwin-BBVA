import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, coalesce, try_to_timestamp, lit


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestSilverCleaning")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def users_data(spark):
    data = [
        (1, " Male ", "  New York  ", 72000.0, 80000.0, 10000.0),
        (2, "Female", "Los Angeles", 68000.0, 75000.0, 20000.0),
        (3, " male ", " Chicago ", 75000.0, 90000.0, 15000.0),
    ]

    columns = [
        "id",
        "gender",
        "address",
        "per_capita_income",
        "yearly_income",
        "total_debt",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def cards_data(spark):
    data = [
        (1, 100, " VISA ", " Credit ", " YES ", " NO ", 15000.0),
        (2, 100, "mastercard", "debit", "yes", "no", 20000.0),
        (3, 101, " Visa ", " debit ", " NO ", " YES ", 10000.0),
    ]

    columns = [
        "id",
        "client_id",
        "card_brand",
        "card_type",
        "has_chip",
        "card_on_dark_web",
        "credit_limit",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def transactions_data(spark):
    data = [
        (
            1,
            "01/01/2010 00:01",
            100,
            1,
            50.0,
            " Swipe Transaction ",
            " New York ",
            " NY ",
        ),
        (
            2,
            "15/01/2010 08:13",
            100,
            2,
            75.0,
            "chip transaction",
            " Chicago ",
            " IL ",
        ),
        (
            3,
            "01/20/2010 10:00",
            101,
            3,
            100.0,
            " Swipe Transaction ",
            " Los Angeles ",
            " CA ",
        ),
    ]

    columns = [
        "id",
        "date",
        "client_id",
        "card_id",
        "amount",
        "use_chip",
        "merchant_city",
        "merchant_state",
    ]

    return spark.createDataFrame(data, columns)


def test_clean_users(spark, users_data):
    cleaned = users_data.withColumn(
        "gender",
        trim(lower(col("gender")))
    )

    cleaned = cleaned.withColumn(
        "address",
        trim(lower(col("address")))
    )

    cleaned = cleaned.withColumn(
        "yearly_income",
        col("yearly_income").cast("double")
    )

    cleaned = cleaned.withColumn(
        "per_capita_income",
        col("per_capita_income").cast("double")
    )

    cleaned = cleaned.withColumn(
        "total_debt",
        col("total_debt").cast("double")
    )

    assert cleaned.filter(
        col("gender") == "male"
    ).count() == 2

    assert cleaned.filter(
        col("gender") == "female"
    ).count() == 1

    assert cleaned.filter(
        col("address") == "new york"
    ).count() == 1

    assert cleaned.schema["yearly_income"].dataType.simpleString() == "double"
    assert cleaned.schema["per_capita_income"].dataType.simpleString() == "double"
    assert cleaned.schema["total_debt"].dataType.simpleString() == "double"


def test_clean_cards(spark, cards_data):
    cleaned = cards_data.withColumn(
        "card_brand",
        trim(lower(col("card_brand")))
    )

    cleaned = cleaned.withColumn(
        "card_type",
        trim(lower(col("card_type")))
    )

    cleaned = cleaned.withColumn(
        "has_chip",
        trim(lower(col("has_chip")))
    )

    cleaned = cleaned.withColumn(
        "card_on_dark_web",
        trim(lower(col("card_on_dark_web")))
    )

    cleaned = cleaned.withColumn(
        "credit_limit",
        col("credit_limit").cast("double")
    )

    assert cleaned.filter(
        col("card_brand") == "visa"
    ).count() == 2

    assert cleaned.filter(
        col("card_brand") == "mastercard"
    ).count() == 1

    assert cleaned.filter(
        col("card_type") == "credit"
    ).count() == 1

    assert cleaned.filter(
        col("has_chip") == "yes"
    ).count() == 2

    assert cleaned.filter(
        col("card_on_dark_web") == "yes"
    ).count() == 1


def test_clean_transactions(spark, transactions_data):
    cleaned = transactions_data.withColumn(
        "use_chip",
        trim(lower(col("use_chip")))
    )

    cleaned = cleaned.withColumn(
        "merchant_city",
        trim(lower(col("merchant_city")))
    )

    cleaned = cleaned.withColumn(
        "merchant_state",
        trim(lower(col("merchant_state")))
    )

    cleaned = cleaned.withColumn(
        "amount",
        col("amount").cast("double")
    )

    assert cleaned.filter(
        col("use_chip") == "swipe transaction"
    ).count() == 2

    assert cleaned.filter(
        col("merchant_city") == "new york"
    ).count() == 1

    assert cleaned.filter(
        col("merchant_state") == "ny"
    ).count() == 1

    assert cleaned.schema["amount"].dataType.simpleString() == "double"


def test_convert_transaction_date(spark, transactions_data):
    cleaned = transactions_data.withColumn(
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

    assert cleaned.filter(
        col("date").isNull()
    ).count() == 0

    assert cleaned.filter(
        col("date").isNotNull()
    ).count() == 3


def test_remove_duplicates(spark):
    data = [
        (1, "male"),
        (1, "male"),
        (2, "female"),
    ]

    df = spark.createDataFrame(
        data,
        ["id", "gender"]
    )

    cleaned = df.dropDuplicates(["id"])

    assert cleaned.count() == 2

    assert cleaned.filter(
        col("id") == 1
    ).count() == 1
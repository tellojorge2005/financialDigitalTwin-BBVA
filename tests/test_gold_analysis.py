import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    round,
    countDistinct
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestGoldAnalysis")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def users_data(spark):
    data = [
        (1, 30, 65, "male", 60000.0, 30000.0, 10000.0, 750, 3),
        (2, 40, 67, "female", 80000.0, 40000.0, 20000.0, 720, 4),
        (3, 50, 68, "male", 50000.0, 25000.0, 15000.0, 700, 2),
    ]

    columns = [
        "id",
        "current_age",
        "retirement_age",
        "gender",
        "yearly_income",
        "per_capita_income",
        "total_debt",
        "credit_score",
        "num_credit_cards",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def cards_data(spark):
    data = [
        (1, 1, "visa", "credit", 10000.0),
        (2, 1, "mastercard", "debit", 20000.0),
        (3, 2, "visa", "credit", 15000.0),
        (4, 2, "visa", "debit", 5000.0),
        (5, 3, "amex", "credit", 12000.0),
    ]

    columns = [
        "id",
        "client_id",
        "card_brand",
        "card_type",
        "credit_limit",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def transactions_data(spark):
    data = [
        (1, 1, 50.0, "new york"),
        (2, 1, 100.0, "chicago"),
        (3, 1, 150.0, "new york"),
        (4, 2, 200.0, "miami"),
        (5, 2, 100.0, "miami"),
        (6, 3, 75.0, "boston"),
    ]

    columns = [
        "id",
        "client_id",
        "amount",
        "merchant_city",
    ]

    return spark.createDataFrame(data, columns)


def test_cards_summary(spark, cards_data):
    cards_gold = (
        cards_data
        .groupBy("client_id")
        .agg(
            count("id").alias("total_cards"),
            countDistinct("card_brand").alias("total_card_brands"),
            countDistinct("card_type").alias("total_card_types"),
            round(avg("credit_limit"), 2).alias("average_credit_limit"),
            round(sum("credit_limit"), 2).alias("total_credit_limit")
        )
    )

    client = cards_gold.filter(
        col("client_id") == 1
    ).first()

    assert client["total_cards"] == 2
    assert client["total_card_brands"] == 2
    assert client["total_card_types"] == 2
    assert client["average_credit_limit"] == 15000.0
    assert client["total_credit_limit"] == 30000.0


def test_transactions_summary(spark, transactions_data):
    transactions_gold = (
        transactions_data
        .groupBy("client_id")
        .agg(
            count("id").alias("total_transactions"),
            round(sum("amount"), 2).alias("total_spent"),
            round(avg("amount"), 2).alias("average_transaction"),
            round(min("amount"), 2).alias("minimum_transaction"),
            round(max("amount"), 2).alias("maximum_transaction"),
            countDistinct("merchant_city").alias("total_merchant_cities")
        )
    )

    client = transactions_gold.filter(
        col("client_id") == 1
    ).first()

    assert client["total_transactions"] == 3
    assert client["total_spent"] == 300.0
    assert client["average_transaction"] == 100.0
    assert client["minimum_transaction"] == 50.0
    assert client["maximum_transaction"] == 150.0
    assert client["total_merchant_cities"] == 2


def test_users_gold(spark, users_data):
    users_gold = (
        users_data
        .select(
            "id",
            "current_age",
            "retirement_age",
            "gender",
            "yearly_income",
            "per_capita_income",
            "total_debt",
            "credit_score",
            "num_credit_cards"
        )
    )

    assert users_gold.count() == 3

    assert users_gold.columns == [
        "id",
        "current_age",
        "retirement_age",
        "gender",
        "yearly_income",
        "per_capita_income",
        "total_debt",
        "credit_score",
        "num_credit_cards",
    ]


def test_join_customer_information(spark, users_data, cards_data, transactions_data):
    cards_gold = (
        cards_data
        .groupBy("client_id")
        .agg(
            count("id").alias("total_cards"),
            countDistinct("card_brand").alias("total_card_brands"),
            countDistinct("card_type").alias("total_card_types"),
            round(avg("credit_limit"), 2).alias("average_credit_limit"),
            round(sum("credit_limit"), 2).alias("total_credit_limit")
        )
    )

    transactions_gold = (
        transactions_data
        .groupBy("client_id")
        .agg(
            count("id").alias("total_transactions"),
            round(sum("amount"), 2).alias("total_spent"),
            round(avg("amount"), 2).alias("average_transaction"),
            round(min("amount"), 2).alias("minimum_transaction"),
            round(max("amount"), 2).alias("maximum_transaction"),
            countDistinct("merchant_city").alias("total_merchant_cities")
        )
    )

    users_gold = (
        users_data
        .select(
            "id",
            "current_age",
            "retirement_age",
            "gender",
            "yearly_income",
            "per_capita_income",
            "total_debt",
            "credit_score",
            "num_credit_cards"
        )
    )

    customer_gold = (
        users_gold
        .join(
            cards_gold,
            users_gold.id == cards_gold.client_id,
            "left"
        )
        .drop(cards_gold.client_id)
        .join(
            transactions_gold,
            users_gold.id == transactions_gold.client_id,
            "left"
        )
        .drop(transactions_gold.client_id)
    )

    assert customer_gold.count() == 3

    client = customer_gold.filter(
        col("id") == 1
    ).first()

    assert client["total_cards"] == 2
    assert client["total_transactions"] == 3
    assert client["total_spent"] == 300.0


def test_financial_metrics(spark, users_data, cards_data, transactions_data):
    cards_gold = (
        cards_data
        .groupBy("client_id")
        .agg(
            count("id").alias("total_cards"),
            countDistinct("card_brand").alias("total_card_brands"),
            countDistinct("card_type").alias("total_card_types"),
            round(avg("credit_limit"), 2).alias("average_credit_limit"),
            round(sum("credit_limit"), 2).alias("total_credit_limit")
        )
    )

    transactions_gold = (
        transactions_data
        .groupBy("client_id")
        .agg(
            count("id").alias("total_transactions"),
            round(sum("amount"), 2).alias("total_spent"),
            round(avg("amount"), 2).alias("average_transaction"),
            round(min("amount"), 2).alias("minimum_transaction"),
            round(max("amount"), 2).alias("maximum_transaction"),
            countDistinct("merchant_city").alias("total_merchant_cities")
        )
    )

    users_gold = (
        users_data
        .select(
            "id",
            "current_age",
            "retirement_age",
            "gender",
            "yearly_income",
            "per_capita_income",
            "total_debt",
            "credit_score",
            "num_credit_cards"
        )
    )

    customer_gold = (
        users_gold
        .join(
            cards_gold,
            users_gold.id == cards_gold.client_id,
            "left"
        )
        .drop(cards_gold.client_id)
        .join(
            transactions_gold,
            users_gold.id == transactions_gold.client_id,
            "left"
        )
        .drop(transactions_gold.client_id)
    )

    customer_gold = (
        customer_gold
        .withColumn(
            "spending_to_income_ratio",
            round(
                col("total_spent") / col("yearly_income"),
                4
            )
        )
        .withColumn(
            "debt_to_income_ratio",
            round(
                col("total_debt") / col("yearly_income"),
                4
            )
        )
    )

    client = customer_gold.filter(
        col("id") == 1
    ).first()

    assert client["spending_to_income_ratio"] == 0.005
    assert client["debt_to_income_ratio"] == 0.1667
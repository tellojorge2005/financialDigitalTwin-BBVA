import pytest

from src.gold_analysis import (
    create_cards_summary,
    create_transactions_summary,
    create_users_summary,
    create_customer_summary,
    run_pipeline
)


@pytest.fixture
def users_data(spark):
    return spark.createDataFrame(
        [
            (1, 30, 65, "male", 60000.0, 30000.0, 10000.0, 750, 3),
            (2, 40, 67, "female", 0.0, 40000.0, 20000.0, 720, 4),
            (3, 50, 68, "male", 50000.0, 25000.0, 15000.0, 700, 2),
            (4, 35, 65, "female", None, 20000.0, 5000.0, 710, 1)
        ],
        """
        id long,
        current_age long,
        retirement_age long,
        gender string,
        yearly_income double,
        per_capita_income double,
        total_debt double,
        credit_score long,
        num_credit_cards long
        """
    )


@pytest.fixture
def cards_data(spark):
    return spark.createDataFrame(
        [
            (1, 1, "visa", "credit", 10000.0, False),
            (None, 1, "mastercard", "debit", 20000.0, True),
            (3, 2, "visa", "credit", None, False)
        ],
        """
        id long,
        client_id long,
        card_brand string,
        card_type string,
        credit_limit double,
        credit_limit_imputed boolean
        """
    )


@pytest.fixture
def transactions_data(spark):
    return spark.createDataFrame(
        [
            (1, 1, 50.0, "new york"),
            (2, 1, 100.0, "chicago"),
            (3, 1, 150.0, "new york"),
            (4, 2, -20.0, "miami"),
            (5, 2, 100.0, "miami")
        ],
        """
        id long,
        client_id long,
        amount double,
        merchant_city string
        """
    )


def test_cards_summary(cards_data):
    rows = {
        row.client_id: row
        for row in create_cards_summary(cards_data).collect()
    }

    assert rows[1].total_cards == 2
    assert rows[1].total_cards_with_id == 1
    assert rows[1].total_cards_without_id == 1
    assert rows[1].total_card_brands == 2
    assert rows[1].total_card_types == 2
    assert rows[1].average_credit_limit == 15000.0
    assert rows[1].total_credit_limit == 30000.0
    assert rows[1].total_credit_limits_imputed == 1
    assert rows[1].has_imputed_credit_limit is True

    assert rows[2].total_cards == 1
    assert rows[2].total_credit_limit is None
    assert rows[2].average_credit_limit is None
    assert rows[2].total_credit_limits_imputed == 0
    assert rows[2].has_imputed_credit_limit is False


def test_cards_summary_without_imputation_information(spark):
    df = spark.createDataFrame(
        [(1, 1, "visa", "credit", 1000.0, None)],
        """
        id long,
        client_id long,
        card_brand string,
        card_type string,
        credit_limit double,
        credit_limit_imputed boolean
        """
    )

    row = create_cards_summary(df).first()

    assert row.total_credit_limits_imputed is None
    assert row.has_imputed_credit_limit is None


def test_transactions_summary(transactions_data):
    rows = {
        row.client_id: row
        for row in create_transactions_summary(
            transactions_data
        ).collect()
    }

    assert rows[1].total_transactions == 3
    assert rows[1].total_spent == 300.0
    assert rows[1].average_transaction == 100.0
    assert rows[1].minimum_transaction == 50.0
    assert rows[1].maximum_transaction == 150.0
    assert rows[1].total_merchant_cities == 2

    assert rows[2].total_spent == 80.0
    assert rows[2].minimum_transaction == -20.0
    assert rows[2].average_transaction == 40.0


def test_users_summary(users_data):
    users_gold = create_users_summary(users_data)

    assert users_gold.count() == 4

    assert users_gold.columns == [
        "id",
        "current_age",
        "retirement_age",
        "gender",
        "yearly_income",
        "per_capita_income",
        "total_debt",
        "credit_score",
        "num_credit_cards"
    ]


def test_customer_summary(users_data, cards_data, transactions_data):
    customer_gold = create_customer_summary(
        create_users_summary(users_data),
        create_cards_summary(cards_data),
        create_transactions_summary(transactions_data)
    )

    rows = {
        row.id: row
        for row in customer_gold.collect()
    }

    assert len(rows) == 4
    assert "client_id" not in customer_gold.columns

    assert rows[1].total_cards == 2
    assert rows[1].num_credit_cards == 3
    assert rows[1].total_transactions == 3
    assert rows[1].has_card_records is True
    assert rows[1].has_transaction_records is True
    assert rows[1].has_imputed_credit_limit is True
    assert rows[1].spending_to_income_ratio == pytest.approx(0.005)
    assert rows[1].debt_to_income_ratio == pytest.approx(0.1667)

    assert rows[2].spending_to_income_ratio is None
    assert rows[2].debt_to_income_ratio is None

    assert rows[3].has_card_records is False
    assert rows[3].has_transaction_records is False
    assert rows[3].total_cards is None
    assert rows[3].total_spent is None
    assert rows[3].has_imputed_credit_limit is None
    assert rows[3].spending_to_income_ratio is None
    assert rows[3].debt_to_income_ratio == pytest.approx(0.3)

    assert rows[4].spending_to_income_ratio is None
    assert rows[4].debt_to_income_ratio is None


def test_run_pipeline(
    spark,
    tmp_path,
    users_data,
    cards_data,
    transactions_data
):
    input_path = (tmp_path / "silver").as_posix()
    output_path = (tmp_path / "gold").as_posix()

    users_data.write.parquet(
        f"{input_path}/users.parquet"
    )

    cards_data.write.parquet(
        f"{input_path}/cards.parquet"
    )

    transactions_data.write.parquet(
        f"{input_path}/transactions.parquet"
    )

    run_pipeline(
        spark,
        input_path=input_path,
        output_path=output_path
    )

    customer_gold = spark.read.parquet(
        f"{output_path}/customer_financial_summary.parquet"
    )

    cards_gold = spark.read.parquet(
        f"{output_path}/cards_summary.parquet"
    )

    transactions_gold = spark.read.parquet(
        f"{output_path}/transactions_summary.parquet"
    )

    assert customer_gold.count() == 4
    assert cards_gold.count() == 2
    assert transactions_gold.count() == 2

    assert "total_cards_without_id" in customer_gold.columns
    assert "total_credit_limits_imputed" in customer_gold.columns
    assert "has_imputed_credit_limit" in customer_gold.columns
    assert "has_card_records" in customer_gold.columns
    assert "has_transaction_records" in customer_gold.columns
import pytest

from pyspark.sql import SparkSession

from src.data_cleaning import clean_users_data


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("FinancialDigitalTwinTests")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def users_data(spark):
    data = [
        (1, 25, "Male", 720, 2),
        (2, None, "Female", 680, 3),
        (3, 40, None, 750, 4),
        (4, None, "Female", 700, 2),
        (5, 35, "Female", 710, 5),
    ]

    columns = [
        "id",
        "current_age",
        "gender",
        "credit_score",
        "num_credit_cards"
    ]

    return spark.createDataFrame(data, columns)


def test_users_dataset_loaded(users_data):
    assert users_data.count() == 5


def test_required_columns(users_data):
    required_columns = {
        "id",
        "current_age",
        "gender",
        "credit_score",
        "num_credit_cards"
    }

    assert required_columns.issubset(set(users_data.columns))


def test_current_age_is_imputed(users_data):
    cleaned_data = clean_users_data(users_data)

    null_count = (
        cleaned_data
        .filter(cleaned_data.current_age.isNull())
        .count()
    )

    assert null_count == 0


def test_gender_is_imputed(users_data):
    cleaned_data = clean_users_data(users_data)

    null_count = (
        cleaned_data
        .filter(cleaned_data.gender.isNull())
        .count()
    )

    assert null_count == 0


def test_row_count_is_preserved(users_data):
    cleaned_data = clean_users_data(users_data)

    assert cleaned_data.count() == users_data.count()
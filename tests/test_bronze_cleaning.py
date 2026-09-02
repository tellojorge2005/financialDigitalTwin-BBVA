import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import Imputer


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestBronzeCleaning")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def users_data(spark):
    data = [
        (1, 25, "Male", "New York", "$72000", "$80000", "$10000"),
        (2, None, "Female", "  Los Angeles  ", "$68000", "$75000", "$20000"),
        (3, 40, None, "Chicago", "$75000", "$90000", "$15000"),
        (4, 10, "Male", "Houston", "$60000", "$70000", "$5000"),
        (5, 130, "Female", "Miami", "$65000", "$72000", "$8000"),
    ]

    columns = [
        "id",
        "current_age",
        "gender",
        "address",
        "per_capita_income",
        "yearly_income",
        "total_debt",
    ]

    return spark.createDataFrame(data, columns)


def test_clean_string(spark, users_data):
    from pyspark.sql.functions import trim, lower

    def clean_string(df, column):
        return df.withColumn(
            column,
            trim(lower(col(column)))
        )

    cleaned = clean_string(users_data, "gender")

    assert cleaned.filter(col("gender") == "male").count() == 2
    assert cleaned.filter(col("gender") == "female").count() == 2


def test_clean_address(spark, users_data):
    from pyspark.sql.functions import trim, lower

    def clean_string(df, column):
        return df.withColumn(
            column,
            trim(lower(col(column)))
        )

    cleaned = clean_string(users_data, "address")

    assert (
        cleaned.filter(col("address") == "los angeles").count()
        == 1
    )


def test_clean_currency(spark, users_data):
    from pyspark.sql.functions import regexp_replace

    def clean_currency(df, column):
        return df.withColumn(
            column,
            regexp_replace(
                regexp_replace(col(column), r"\$", ""),
                ",",
                ""
            ).cast("float")
        )

    cleaned = clean_currency(
        users_data,
        "yearly_income"
    )

    assert (
        cleaned.filter(col("yearly_income") == 80000.0).count()
        == 1
    )


def test_clean_currency_with_comma(spark):
    from pyspark.sql.functions import regexp_replace

    data = [
        ("$80,000",),
        ("$100,000",)
    ]

    df = spark.createDataFrame(
        data,
        ["income"]
    )

    def clean_currency(df, column):
        return df.withColumn(
            column,
            regexp_replace(
                regexp_replace(col(column), r"\$", ""),
                ",",
                ""
            ).cast("float")
        )

    cleaned = clean_currency(df, "income")

    assert cleaned.filter(col("income") == 80000.0).count() == 1
    assert cleaned.filter(col("income") == 100000.0).count() == 1


def test_impute_median(spark, users_data):
    imputer = (
        Imputer(
            inputCols=["current_age"],
            outputCols=["current_age"]
        )
        .setStrategy("median")
    )

    cleaned = (
        imputer
        .fit(users_data)
        .transform(users_data)
    )

    assert cleaned.filter(
        col("current_age").isNull()
    ).count() == 0


def test_impute_mode(spark):
    data = [
        ("male",),
        ("male",),
        ("female",),
        (None,),
    ]

    df = spark.createDataFrame(
        data,
        ["gender"]
    )

    def impute_mode(df, column):
        mode_value = (
            df.filter(col(column).isNotNull())
            .groupBy(column)
            .count()
            .orderBy(col("count").desc())
            .first()[column]
        )

        return df.fillna({column: mode_value})

    cleaned = impute_mode(df, "gender")

    assert cleaned.filter(
        col("gender").isNull()
    ).count() == 0

    assert cleaned.filter(
        col("gender") == "male"
    ).count() == 3


def test_age_limits(spark, users_data):
    from pyspark.sql.functions import when

    imputer = (
        Imputer(
            inputCols=["current_age"],
            outputCols=["current_age"]
        )
        .setStrategy("median")
    )

    cleaned = (
        imputer
        .fit(users_data)
        .transform(users_data)
    )

    cleaned = cleaned.withColumn(
        "current_age",
        when(col("current_age") < 18, 18)
        .when(col("current_age") > 120, 120)
        .otherwise(col("current_age"))
    )

    ages = [
        row["current_age"]
        for row in cleaned.select("current_age").collect()
    ]

    assert all(age >= 18 for age in ages)
    assert all(age <= 120 for age in ages)
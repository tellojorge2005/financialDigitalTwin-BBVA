from datetime import date
from decimal import Decimal

from src.gold_analysis import create_monthly_summaries

import pytest

from pyspark.sql import functions as F

from src.gold_analysis import (
    ErrorCalidadDatos,
    create_cards_summary,
    create_transactions_summary,
    create_users_summary,
    create_customer_summary,
    validar_salida_gold,
    run_pipeline
)


@pytest.fixture
def users_data(spark):
    return spark.createDataFrame(
        [
            (
                1, 30, 65, "male",
                60000.0, 30000.0, 10000.0, 750, 3
            ),
            (
                2, 40, 67, "female",
                0.0, 40000.0, 20000.0, 720, 4
            ),
            (
                3, 50, 68, "male",
                50000.0, 25000.0, 15000.0, 700, 2
            ),
            (
                4, 35, 65, "female",
                None, 20000.0, 5000.0, 710, 1
            )
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


# Preparar datos válidos para el pipeline completo
@pytest.fixture
def valid_pipeline_data(
    users_data,
    cards_data,
    transactions_data
):
    cards_valid = (
        cards_data
        .withColumn(
            "id",
            F.when(
                F.col("id").isNull(),
                F.lit(2).cast("long")
            ).otherwise(F.col("id"))
        )
        .fillna({"credit_limit": 1000.0})
    )

    transactions_valid = transactions_data.withColumn(
        "date",
        F.lit("2026-09-01 10:00:00").cast("timestamp")
    )

    return users_data, cards_valid, transactions_valid


# Validar resumen de tarjetas
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
        [
            (1, 1, "visa", "credit", 1000.0, None)
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

    row = create_cards_summary(df).first()

    assert row.total_credit_limits_imputed is None
    assert row.has_imputed_credit_limit is None


# Validar resumen de transacciones
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


# Validar información de usuarios
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


# Validar unión y métricas financieras
def test_customer_summary(
    users_data,
    cards_data,
    transactions_data
):
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
    assert rows[1].spending_to_income_ratio == pytest.approx(
        0.005
    )
    assert rows[1].debt_to_income_ratio == pytest.approx(
        0.1667
    )

    assert rows[2].spending_to_income_ratio is None
    assert rows[2].debt_to_income_ratio is None

    assert rows[3].has_card_records is False
    assert rows[3].has_transaction_records is False
    assert rows[3].total_cards is None
    assert rows[3].total_spent is None
    assert rows[3].has_imputed_credit_limit is None
    assert rows[3].spending_to_income_ratio is None
    assert rows[3].debt_to_income_ratio == pytest.approx(
        0.3
    )

    assert rows[4].spending_to_income_ratio is None
    assert rows[4].debt_to_income_ratio is None


# Validar ejecución completa con datos válidos


# Validar bloqueo de datos incorrectos


# Validar coherencia del resumen final
def test_gold_rechaza_conteos_inconsistentes(
    valid_pipeline_data
):
    users, cards, transactions = valid_pipeline_data

    customer_gold = create_customer_summary(
        create_users_summary(users),
        create_cards_summary(cards),
        create_transactions_summary(transactions)
    )

    customer_gold = customer_gold.withColumn(
        "total_cards",
        F.when(
            F.col("id") == 1,
            F.col("total_cards") + 1
        ).otherwise(F.col("total_cards"))
    )

    with pytest.raises(
        ErrorCalidadDatos,
        match="conteos inconsistentes"
    ):
        validar_salida_gold(
            customer_gold,
            users.count()
        )

# Validar importes mensuales contra resultados calculados a mano
def test_monthly_summaries(silver_bundle):
    movements = silver_bundle["transactions"].filter(
        F.col("amount_convention") == "signed_cashflow"
    )
    account, monthly, latest = create_monthly_summaries(
        silver_bundle["accounts"], silver_bundle["obligations_payments"], movements
    )
    rows = {(r.client_id, r.month): r for r in monthly.collect()}
    assert len(rows) == 6
    jan = rows[(1, date(2010, 1, 1))]
    feb = rows[(1, date(2010, 2, 1))]
    mar = rows[(1, date(2010, 3, 1))]

    assert jan.monthly_income == Decimal("1000")
    assert jan.monthly_expenses == Decimal("300")
    assert jan.monthly_debt_payments == Decimal("100")
    assert jan.monthly_savings == Decimal("600")
    assert jan.transfers_out == Decimal("50")
    assert jan.monthly_net_flow == Decimal("550")
    assert jan.monthly_transactions == 4
    assert jan.closing_balance == Decimal("650")
    assert feb.closing_balance == Decimal("1450")
    assert mar.closing_balance == Decimal("2100")

    # No utilizar en febrero el pago parcial que ocurre en marzo
    assert jan.outstanding_due == Decimal("0")
    assert feb.outstanding_due == Decimal("150")
    assert feb.max_days_past_due == 8
    assert mar.outstanding_due == Decimal("100")
    assert mar.max_days_past_due == 39
    assert mar.installments_30_days_past_due == 1

    # Mantener meses sin movimientos y arrastrar el saldo
    quiet = rows[(2, date(2010, 2, 1))]
    assert quiet.monthly_transactions == 0
    assert quiet.monthly_income == Decimal("0")
    assert quiet.closing_balance == Decimal("250")
    assert quiet.outstanding_due == Decimal("75")

    # Ventanas históricas sin información de meses posteriores
    assert jan.income_lag_1 is None
    assert jan.income_mean_3 == Decimal("1000")
    assert feb.income_lag_1 == Decimal("1000")
    assert mar.income_lag_1 == Decimal("1200")
    assert mar.income_lag_3 is None
    assert float(mar.income_mean_3) == pytest.approx(3100 / 3)
    assert mar.history_months == 3
    assert account.count() == 6
    last = {r.id: r for r in latest.collect()}
    assert last[1].financial_cutoff_date == date(2010, 3, 31)
    assert last[1].account_balance == Decimal("2100")


def test_incomplete_last_month_is_excluded(silver_bundle):
    accounts = silver_bundle["accounts"].withColumn(
        "balance_as_of", F.lit("2010-03-15 23:59:00").cast("timestamp")
    )
    movements = silver_bundle["transactions"].filter(F.col("amount_convention") == "signed_cashflow")
    _, monthly, latest = create_monthly_summaries(
        accounts, silver_bundle["obligations_payments"], movements
    )
    assert {r.month for r in monthly.select("month").distinct().collect()} == {
        date(2010, 1, 1), date(2010, 2, 1)
    }
    assert latest.filter(F.col("id") == 1).first().account_balance == Decimal("1450")


def test_partial_opening_month_requires_explicit_handling(silver_bundle):
    accounts = silver_bundle["accounts"].withColumn(
        "opened_at", F.lit("2010-01-02 00:00:00").cast("timestamp")
    )
    with pytest.raises(ValueError, match="mitad de mes"):
        create_monthly_summaries(
            accounts, silver_bundle["obligations_payments"], silver_bundle["transactions"]
        )


def test_run_pipeline(spark, tmp_path, silver_bundle, escribir_parquet):
    entrada = escribir_parquet(silver_bundle, tmp_path / "silver")
    salida = tmp_path / "gold"
    run_pipeline(spark, input_path=entrada, output_path=salida.as_uri())

    expected = {
        "customer_financial_summary": 3, "transactions_summary": 2,
        "cards_summary": 2, "customer_monthly_summary": 6,
        "account_monthly_summary": 6, "accounts": 2,
        "obligations_payments": 3, "movements": 10
    }
    for nombre, cantidad in expected.items():
        assert spark.read.parquet((salida / f"{nombre}.parquet").as_uri()).count() == cantidad

    customer = spark.read.parquet((salida / "customer_financial_summary.parquet").as_uri())
    rows = {r.id: r for r in customer.collect()}
    assert rows[1].total_spent == Decimal("50")
    assert rows[1].total_transactions == 1
    assert rows[1].monthly_income == Decimal("900")
    assert rows[1].account_balance == Decimal("2100")
    assert rows[1].outstanding_due == Decimal("100")
    assert rows[3].total_spent is None
    assert rows[3].account_balance is None
    assert rows[3].monthly_income is None
    assert rows[3].has_transaction_records is False


@pytest.mark.parametrize(
    "caso,mensaje",
    [("id_nulo", "id: nulo o negativo"),
     ("duplicados", "duplicados adicionales"),
     ("tipo_incorrecto", "esquema incorrecto"),
     ("cliente_inexistente", "sin un cliente existente"),
     ("fechas_nulas", "date: nula")]
)
def test_pipeline_rechaza_datos_invalidos(
    spark, tmp_path, silver_bundle, escribir_parquet, caso, mensaje
):
    datos = dict(silver_bundle)
    cards = datos["cards"]
    if caso == "id_nulo":
        datos["cards"] = cards.withColumn("id", F.lit(None).cast("long"))
    elif caso == "duplicados":
        datos["cards"] = cards.unionByName(cards)
    elif caso == "tipo_incorrecto":
        datos["cards"] = cards.withColumn("id", F.col("id").cast("string"))
    elif caso == "cliente_inexistente":
        datos["cards"] = cards.withColumn("client_id", F.lit(999).cast("long"))
    elif caso == "fechas_nulas":
        datos["transactions"] = datos["transactions"].withColumn("date", F.lit(None).cast("timestamp"))

    entrada = escribir_parquet(datos, tmp_path / "silver")
    salida = tmp_path / "gold"
    with pytest.raises(ErrorCalidadDatos, match=mensaje):
        run_pipeline(spark, input_path=entrada, output_path=salida.as_uri())
    assert not salida.exists()

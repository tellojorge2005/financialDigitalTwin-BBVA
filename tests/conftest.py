import os
import sys

import pytest


# Configurar pruebas locales antes de importar src
os.environ["S3_BUCKET"] = "financial-twin-tests"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if os.name == "nt":
    HADOOP_HOME = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["hadoop.home.dir"] = HADOOP_HOME
    os.environ["PATH"] = (
        os.path.join(HADOOP_HOME, "bin")
        + os.pathsep
        + os.environ.get("PATH", "")
    )

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    directorio = tmp_path_factory.mktemp("spark")

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestFinancialDigitalTwin")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.warehouse.dir", (directorio / "warehouse").as_uri())
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# Preparar cinco datasets pequeños con resultados conocidos
@pytest.fixture
def raw_bundle(spark):
    users = spark.createDataFrame(
        [
            (1, 30.0, 65, " Male ", " City ", "$24,000", "$36,000", "$250", 750, 1),
            (2, 40.0, 65, " Female ", " City ", "$24,000", "$48,000", "$75", 700, 1),
            (3, 50.0, 65, " Male ", " City ", "$24,000", "$60,000", "$0", 720, 0)
        ],
        """
        id long, current_age double, retirement_age long,
        gender string, address string, per_capita_income string,
        yearly_income string, total_debt string, credit_score long,
        num_credit_cards long
        """
    )

    cards = spark.createDataFrame(
        [
            (1, 1, " VISA ", " Credit ", " YES ", " NO ", 1.0, "$1,000"),
            (2, 2, " VISA ", " Debit ", " YES ", " NO ", 1.0, "$2,000"),
            (3, 999, "visa", "credit", "yes", "no", 1.0, "$500"),
            (None, 1, "visa", "credit", "yes", "no", 1.0, "$500")
        ],
        """
        id long, client_id long, card_brand string, card_type string,
        has_chip string, card_on_dark_web string, num_cards_issued double,
        credit_limit string
        """
    )

    campos = [
        "id", "date", "client_id", "card_id", "amount", "use_chip",
        "merchant_id", "merchant_city", "merchant_state", "zip", "mcc",
        "errors", "account_id", "movement_type", "category", "direction",
        "currency", "balance_before", "balance_after", "transfer_id",
        "counterparty_account_id", "obligation_id", "income_source",
        "is_recurring", "amount_convention"
    ]

    filas = []
    for tid, cid, fecha, importe in [
        (1, 1, "15/12/2009 10:00", "$50"),
        (2, 2, "20/12/2009 10:00", "$80"),
        (3, 1, "fecha incorrecta", None),
        (4, 999, "15/12/2009 10:00", "$25")
    ]:
        fila = dict.fromkeys(campos)
        fila.update(
            id=str(tid), client_id=str(cid), date=fecha, amount=importe,
            merchant_city=" City ", merchant_state=" NY ",
            use_chip=" Chip ", amount_convention="legacy_unchanged"
        )
        filas.append(fila)

    movimientos = [
        (10, 1, "2010-01-05", "1000", "income", "100", "1100", None, None, None),
        (11, 1, "2010-01-06", "-300", "expense", "1100", "800", None, None, None),
        (12, 1, "2010-01-07", "-50", "transfer_out", "800", "750", "T-1", "A-2", None),
        (13, 2, "2010-01-07", "50", "transfer_in", "200", "250", "T-1", "A-1", None),
        (14, 1, "2010-01-20", "-100", "debt_payment", "750", "650", None, None, "O-1"),
        (15, 1, "2010-02-05", "1200", "income", "650", "1850", None, None, None),
        (16, 1, "2010-02-06", "-400", "expense", "1850", "1450", None, None, None),
        (17, 1, "2010-03-05", "-50", "debt_payment", "1450", "1400", None, None, "O-2"),
        (18, 1, "2010-03-06", "900", "income", "1400", "2300", None, None, None),
        (19, 1, "2010-03-07", "-200", "expense", "2300", "2100", None, None, None)
    ]

    for tid, cid, fecha, importe, tipo, antes, despues, transferencia, otra, cuota in movimientos:
        fila = dict.fromkeys(campos)
        fila.update(
            id=str(tid), client_id=str(cid), date=fecha + " 10:00:00",
            amount=importe, account_id=f"A-{cid}", movement_type=tipo,
            category=tipo, direction="inflow" if float(importe) > 0 else "outflow",
            currency="USD", balance_before=antes, balance_after=despues,
            transfer_id=transferencia, counterparty_account_id=otra,
            obligation_id=cuota, is_recurring="false",
            amount_convention="signed_cashflow"
        )
        filas.append(fila)

    transactions = spark.createDataFrame(
        [tuple(fila[c] for c in campos) for fila in filas],
        ", ".join(f"{c} string" for c in campos)
    )

    accounts = spark.createDataFrame(
        [
            ("A-1", "1", "USD", "2010-01-01 00:00:00", "100", "cash_account", "true", "2100", "2010-03-31 23:59:00"),
            ("A-2", "2", "USD", "2010-01-01 00:00:00", "200", "cash_account", "true", "250", "2010-03-31 23:59:00")
        ],
        """
        account_id string, client_id string, currency string,
        opened_at string, opening_balance string, account_type string,
        overdraft_allowed string, closing_balance string, balance_as_of string
        """
    )

    obligations = spark.createDataFrame(
        [
            ("O-1", "L-1", "1", "A-1", "1", "2009-12-31", "250", "0", "2010-01-20", "100", "2010-01-20", "100", "0", "14", "2010-03-31", "0", "0", "USD"),
            ("O-2", "L-1", "1", "A-1", "2", "2009-12-31", "250", "0", "2010-02-20", "150", "2010-03-05", "50", "100", "17", "2010-03-31", "39", None, "USD"),
            ("O-3", "L-2", "2", "A-2", "1", "2009-12-31", "75", "0", "2010-01-15", "75", None, "0", "75", None, "2010-03-31", "75", None, "USD")
        ],
        """
        obligation_id string, contract_id string, client_id string,
        account_id string, installment_number string, contract_start_date string,
        contract_principal string, annual_interest_rate string, due_date string,
        amount_due string, payment_date string, amount_paid string,
        outstanding_amount string, payment_transaction_id string,
        as_of_date string, days_past_due_as_of string,
        days_to_full_payment string, currency string
        """
    )

    return {
        "users": users, "cards": cards, "transactions": transactions,
        "accounts": accounts, "obligations_payments": obligations
    }


# Construir entradas tipadas sin ejecutar las funciones que se prueban
@pytest.fixture
def bronze_bundle(raw_bundle):
    datos = dict(raw_bundle)
    importes = {
        "users": ["per_capita_income", "yearly_income", "total_debt"],
        "cards": ["credit_limit"],
        "transactions": ["amount", "balance_before", "balance_after"],
        "accounts": ["opening_balance", "closing_balance"],
        "obligations_payments": ["contract_principal", "amount_due", "amount_paid", "outstanding_amount"]
    }

    for nombre, columnas in importes.items():
        for columna in columnas:
            tipo = "double" if nombre in ("users", "cards") else "decimal(18,2)"
            datos[nombre] = datos[nombre].withColumn(
                columna,
                F.regexp_replace(F.col(columna), r"[$,]", "").cast(tipo)
            )

    enteros = {
        "transactions": ["id", "client_id", "card_id", "merchant_id", "mcc"],
        "accounts": ["client_id"],
        "obligations_payments": ["client_id", "installment_number", "payment_transaction_id", "days_past_due_as_of", "days_to_full_payment"]
    }

    for nombre, columnas in enteros.items():
        for columna in columnas:
            datos[nombre] = datos[nombre].withColumn(
                columna, F.col(columna).cast("long")
            )

    datos["accounts"] = datos["accounts"].withColumn(
        "overdraft_allowed", F.col("overdraft_allowed").cast("boolean")
    )
    datos["transactions"] = datos["transactions"].withColumn(
        "is_recurring", F.col("is_recurring").cast("boolean")
    )
    datos["obligations_payments"] = datos["obligations_payments"].withColumn(
        "annual_interest_rate", F.col("annual_interest_rate").cast("decimal(12,6)")
    )
    return datos


@pytest.fixture
def silver_bundle(bronze_bundle):
    datos = dict(bronze_bundle)
    datos["users"] = datos["users"].withColumn("gender", F.lower(F.trim("gender")))
    datos["cards"] = (
        datos["cards"]
        .filter(F.col("id").isin(1, 2))
        .withColumn("credit_limit_imputed", F.lit(False))
        .withColumn("credit_limit_imputation_method", F.lit(None).cast("string"))
    )
    datos["transactions"] = (
        datos["transactions"]
        .filter(~F.col("id").isin(3, 4))
        .withColumn(
            "date",
            F.coalesce(
                F.try_to_timestamp("date", F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp("date", F.lit("dd/MM/yyyy HH:mm"))
            )
        )
    )
    for columna in ["opened_at", "balance_as_of"]:
        datos["accounts"] = datos["accounts"].withColumn(
            columna, F.to_timestamp(columna)
        )
    for columna in ["contract_start_date", "due_date", "payment_date", "as_of_date"]:
        datos["obligations_payments"] = datos["obligations_payments"].withColumn(
            columna, F.to_date(columna)
        )
    return datos


@pytest.fixture
def escribir_parquet():
    def escribir(datos, carpeta):
        for nombre, df in datos.items():
            df.write.parquet((carpeta / f"{nombre}.parquet").as_uri())
        return carpeta.as_uri()
    return escribir

import os
import sys

import pytest

from pyspark.sql import SparkSession


# Configurar variables de entorno para Hadoop en Windows
if os.name == "nt":
    HADOOP_HOME = os.environ.get("HADOOP_HOME", r"C:\hadoop")

    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["hadoop.home.dir"] = HADOOP_HOME
    os.environ["PATH"] = (
        os.path.join(HADOOP_HOME, "bin")
        + os.pathsep
        + os.environ.get("PATH", "")
    )


# Configurar Python para Spark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestFinancialDigitalTwin")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )

    yield spark

    spark.stop()
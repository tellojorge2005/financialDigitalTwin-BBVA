from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


DEFAULT_ARGS = {
    "owner": "jorge_tello",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 9, 1),
}


OUTPUT_PATH = "/opt/project/data/processed/bronze"

SPARK_MASTER = "spark://spark-master:7077"


def setup_bronze():
    import os

    os.makedirs(
        OUTPUT_PATH,
        exist_ok=True
    )

    print("Directorio BRONZE listo")


with DAG(
    dag_id="bronze_cleaning_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline de limpieza para capa BRONZE",
    schedule="@daily",
    catchup=False,
    tags=["etl", "spark", "bronze"],
) as dag:

    setup_task = PythonOperator(
        task_id="setup_bronze_directory",
        python_callable=setup_bronze,
    )

    clean_task = SparkSubmitOperator(
        task_id="run_bronze_cleaning",

        application="/opt/project/src/bronze_cleaning.py",

        name="Bronze_Cleaning",

        conn_id="spark_default",

        conf={
            "spark.master": SPARK_MASTER,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.hadoop.fs.permissions.umask-mode": "000",
        },
    )

    setup_task >> clean_task
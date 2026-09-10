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


INPUT_PATH = "/opt/project/data/processed/bronze"

OUTPUT_PATH = "/opt/project/data/processed/silver"

SPARK_MASTER = "spark://spark-master:7077"


def setup_silver():
    import os

    os.makedirs(
        OUTPUT_PATH,
        exist_ok=True
    )

    print("Directorio SILVER listo")


with DAG(
    dag_id="silver_cleaning_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline de limpieza para capa SILVER",
    schedule="@daily",
    catchup=False,
    tags=["etl", "spark", "silver"],
) as dag:

    setup_task = PythonOperator(
        task_id="setup_silver_directory",
        python_callable=setup_silver,
    )

    clean_task = SparkSubmitOperator(
        task_id="run_silver_cleaning",

        application="/opt/project/src/silver_cleaning.py",

        name="Silver_Cleaning",

        conn_id="spark_default",

        conf={
            "spark.master": SPARK_MASTER,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.hadoop.fs.permissions.umask-mode": "000",
        },
    )

    setup_task >> clean_task
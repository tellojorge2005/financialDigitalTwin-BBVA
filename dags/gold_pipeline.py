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


INPUT_PATH = "/opt/project/data/processed/silver"

OUTPUT_PATH = "/opt/project/data/processed/gold"

SPARK_MASTER = "spark://spark-master:7077"


def setup_gold():
    import os

    os.makedirs(
        OUTPUT_PATH,
        exist_ok=True
    )

    print("Directorio GOLD listo")


with DAG(
    dag_id="gold_analysis_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline de análisis para capa GOLD",
    schedule="@daily",
    catchup=False,
    tags=["etl", "spark", "gold"],
) as dag:

    setup_task = PythonOperator(
        task_id="setup_gold_directory",
        python_callable=setup_gold,
    )

    analysis_task = SparkSubmitOperator(
        task_id="run_gold_analysis",

        application="/opt/project/src/gold_analysis.py",

        name="Gold_Analysis",

        conn_id="spark_default",

        conf={
            "spark.master": SPARK_MASTER,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.hadoop.fs.permissions.umask-mode": "000",
        },
    )

    setup_task >> analysis_task
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


DEFAULT_ARGS = {
    "owner": "jorge_tello",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 9, 1),
}


SPARK_MASTER = "spark://spark-master:7077"


with DAG(
    dag_id="gold_analysis_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline de análisis para capa GOLD en S3",
    schedule="@daily",
    catchup=False,
    tags=["etl", "spark", "gold"],
) as dag:

    clean_task = SparkSubmitOperator(
        task_id="run_gold_analysis",
        application="/opt/project/src/gold_analysis.py",
        name="Gold_Analysis",
        conn_id="spark_default",
        conf={
            "spark.master": SPARK_MASTER,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
    )
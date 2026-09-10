from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id="silver_cleaning_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline de limpieza para capa SILVER en S3",
    schedule="@daily",
    catchup=False,
    tags=["etl", "spark", "silver"],
) as dag:

    clean_task = SparkSubmitOperator(
        task_id="run_silver_cleaning",
        application="/opt/project/src/silver_cleaning.py",
        name="Silver_Cleaning",
        conn_id="spark_default",
        conf={
            "spark.master": SPARK_MASTER,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_pipeline",
        trigger_dag_id="gold_analysis_pipeline",
        wait_for_completion=False,
    )

    clean_task >> trigger_gold
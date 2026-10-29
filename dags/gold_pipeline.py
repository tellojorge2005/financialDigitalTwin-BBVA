from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator
)


DEFAULT_ARGS = {
    "owner": "jorge_tello",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 9, 1),
}

SPARK_MASTER = "spark://spark-master:7077"

SPARK_CONF = {
    "spark.master": SPARK_MASTER,
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
}


# Silver ya dispara este DAG: conservar su identificador
with DAG(
    dag_id="gold_analysis_pipeline",
    default_args=DEFAULT_ARGS,
    description="GOLD, modelaje de ingresos e integración automática en S3",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "spark", "gold", "xgboost"],
) as dag:

    create_gold = SparkSubmitOperator(
        task_id="run_gold_analysis",
        application="/opt/project/src/gold_analysis.py",
        application_args=["--stage", "base"],
        name="Gold_Analysis",
        conn_id="spark_default",
        deploy_mode="client",
        conf=dict(SPARK_CONF),
    )

    predict_income = SparkSubmitOperator(
        task_id="predict_income",
        application="/opt/project/src/income_model.py",
        name="Income_Model",
        conn_id="spark_default",
        deploy_mode="client",
        conf=dict(SPARK_CONF),
    )

    enrich_gold = SparkSubmitOperator(
        task_id="incorporate_income_predictions",
        application="/opt/project/src/gold_analysis.py",
        application_args=["--stage", "predictions"],
        name="Gold_Predictions",
        conn_id="spark_default",
        deploy_mode="client",
        conf=dict(SPARK_CONF),
    )

    create_gold >> predict_income >> enrich_gold

    
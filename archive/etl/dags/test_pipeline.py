from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_data():
    print("Extrayendo datos...")


def transform_data():
    print("Transformando datos...")


def validate_data():
    print("Validando datos...")


with DAG(
    dag_id="financial_digital_twin_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "spark", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    extract >> transform >> validate
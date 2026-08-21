from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# Configuracion
DEFAULT_ARGS = {
    "owner": "jorge_tello",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

# Rutas de los datos
INPUT_PATH = "/workspace/github__tellojorge2005__financialDigitalTwin-BBVA/data/modified"
OUTPUT_PATH = "/workspace/github__tellojorge2005__financialDigitalTwin-BBVA/data/processed/bronze"
SPARK_MASTER = "spark://spark-master:7077"

# Funcion para crear directorio
def setup_bronze():
    import os
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    print("Directorio BRONZE listo")

# Definir el DAG
with DAG(
    dag_id="bronze_cleaning_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline de limpieza para capa BRONZE",
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "spark", "bronze"],
) as dag:

    # Crear directorio
    setup_task = PythonOperator(
        task_id="setup_bronze_directory",
        python_callable=setup_bronze,
    )

    # Ejecutar limpieza con Spark en nodos
    clean_task = SparkSubmitOperator(
        task_id="run_bronze_cleaning",
        application="../src/bronze_cleaning.py",  # Script de limpieza
        name="Bronze_Cleaning",
        conn_id="spark_default",
        conf={
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": "cluster",  # Ejecuta en nodos
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
    )

    # Flujo: setup -> clean
    setup_task >> clean_task
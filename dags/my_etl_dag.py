# dags/my_etl_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Adjust paths if necessary (e.g., environment variables or Airflow Variables)
BASE_PATH = "/path/to/my_etl_project"
BRONZE_CONFIG = os.path.join(BASE_PATH, "configs", "bronze.conf")
SILVER_CONFIG = os.path.join(BASE_PATH, "configs", "silver.conf")
GOLD_CONFIG   = os.path.join(BASE_PATH, "configs", "gold.conf")

with DAG(
    dag_id="my_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_bronze = SparkSubmitOperator(
        task_id="run_bronze_layer",
        application=os.path.join(BASE_PATH, "main_etl_job.py"),
        name="BronzeLayerJob",
        conn_id="spark_default",    # Your Airflow Spark connection ID
        application_args=[BRONZE_CONFIG],
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )

    run_silver = SparkSubmitOperator(
        task_id="run_silver_layer",
        application=os.path.join(BASE_PATH, "main_etl_job.py"),
        name="SilverLayerJob",
        conn_id="spark_default",
        application_args=[SILVER_CONFIG],
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )

    run_gold = SparkSubmitOperator(
        task_id="run_gold_layer",
        application=os.path.join(BASE_PATH, "main_etl_job.py"),
        name="GoldLayerJob",
        conn_id="spark_default",
        application_args=[GOLD_CONFIG],
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )

    # Create a simple linear dependency: Bronze -> Silver -> Gold
    run_bronze >> run_silver >> run_gold

from datetime import datetime, timedelta
import sys
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import run_extraction
from src.transform import run_transformation

default_args = {
    "owner": "allan",
    "depends_on_past": False,
    "retries" : 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="nasa_neows_pyspark_pipeline",
    description="Extract and transform NASA NeoWs data with PySpark",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["nasa", "neows", "pyspark", "etl"],
) as dag:
    
    extract_task = PythonOperator(
        task_id = "extract_nasa_neows_data",
        python_callable = run_extraction,
    )

    transform_task = PythonOperator(
        task_id = "transform_nasa_neows_data",
        python_callable = run_transformation
    )

    extract_task >> transform_task
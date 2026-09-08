from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "Shiva",
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="superstore_etl_pipeline",
    default_args=default_args,
    description="Kafka ETL Pipeline using Flask, Kafka, SQLite",
    start_date=datetime(2026, 7, 28),
    schedule_interval=None,
    catchup=False,
)

project_path = "/opt/airflow/project"

from airflow.operators.python import PythonOperator
import subprocess

def run_script(script_name):
    subprocess.run(["python", script_name], cwd=project_path, check=True)

producer = PythonOperator(
    task_id="run_producer",
    python_callable=run_script,
    op_args=["producer.py"],
    dag=dag,
)

consumer = PythonOperator(
    task_id="run_consumer",
    python_callable=run_script,
    op_args=["consumer.py"],
    dag=dag,
)

staging = PythonOperator(
    task_id="run_staging",
    python_callable=run_script,
    op_args=["staging.py"],
    dag=dag,
)

validation = PythonOperator(
    task_id="run_validation",
    python_callable=run_script,
    op_args=["validation.py"],
    dag=dag,
)

database = PythonOperator(
    task_id="run_database",
    python_callable=run_script,
    op_args=["database.py"],
    dag=dag,
)

producer >> consumer >> staging >> validation >> database
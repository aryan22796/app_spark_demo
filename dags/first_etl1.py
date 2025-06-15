from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

# Dynamically resolve script path
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
spark_script_path = os.path.join(base_dir, 'spark_jobs', 'basic_etl.py')

with DAG(
    dag_id='etl_sales_local_pipeline1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['local', 'spark', 'etl'],
) as dag:

    etl_task = BashOperator(
        task_id='run_sales_etl',
        bash_command=f'spark-submit {spark_script_path}'
    )

    etl_task

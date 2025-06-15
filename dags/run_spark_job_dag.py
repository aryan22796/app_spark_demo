# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 1, 1),
#     'retries': 0,
# }

# with DAG(
#     dag_id='spark_job_local_runner',
#     default_args=default_args,
#     schedule_interval=None,  # Manual trigger
#     catchup=False,
#     tags=['spark', 'local'],
# ) as dag:

#     run_spark_job = BashOperator(
#         task_id='run_pyspark_script',
#         bash_command='spark-submit /Users/aryan/Desktop/app_spark_demo/spark_jobs/simple_job.py'
#     )

#     run_spark_job


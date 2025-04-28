from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 25),
    'retries': 1
}

# Define the DAG
dag = DAG(
    dag_id='upload_sales_data_to_gcs',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Define the task
upload_task = BashOperator(
    task_id='upload_csv_to_gcs',
    bash_command=' bash upload_data.sh',
    dag=dag
)

upload_task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='employee_data',
    default_args=default_args,
    description='Extract employee data and trigger Data Fusion pipeline',
    start_date=datetime(2025, 11, 2),
#    schedule_interval="*/5 * * * *",   # ✅ runs every 5 minutes
    schedule_interval=None,
    catchup=False,
    tags=['gcp', 'datafusion']
) as dag:
    print("Dag Started running!!")

    # Task 1: Run your Python extraction script
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/webfile/loaddatafromgcstobigquery.py',
    )


    run_script_task
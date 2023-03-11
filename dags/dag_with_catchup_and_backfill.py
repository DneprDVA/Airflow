from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Dnepr',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag_with_catchup_and_backfill_v02',
    default_args=default_args,
    start_date=datetime(2023, 2, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!'
    )
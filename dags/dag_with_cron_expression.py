from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Dnepr',
    'retry': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_cron_expression_v04",
    start_date=datetime(2023, 2, 13),
    schedule_interval='0 3 * * Tue-Fri'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo dag with cron expression!"
    )
    task1 
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'coder2j',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2023, 2, 17, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello Dnepr, this is your first task!!!!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo second task is on the way!!!!"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo I am third task and i m on my way!!!!"
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    # task1 >> task2
    # task1 >> task3

    task1 >> [task2, task3]
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Dnepr',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    start_date=datetime(2023, 3, 7),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )
    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id}}')
        """
    )
    task1 >> task2

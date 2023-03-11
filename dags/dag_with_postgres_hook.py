import csv
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'Dnepr',
    'retry': 5,
    'retry_delay': timedelta(minutes=10)
}

def postgres_to_s3():
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <= '20220501'")
    with open("dags/get_orders.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
    cursor.close()
    conn.close()
    logging.info("Saved orders data in text file get_orders.txt")

with DAG(
    default_args=default_args,
    dag_id='dag_with_postgres_hooks_v01',
    start_date=datetime(2023, 2, 21),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1 
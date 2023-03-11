from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.decorators import dag, task

default_args = {
    'owner': 'Dnepr',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def get_sqlalchemy():
    import sqlalchemy
    print(f"sqlalchemy with version:{sqlalchemy.__version__}")

def load_dbtest():
    from sqlalchemy import create_engine
    engine = create_engine('postgresql+psycopg2://postgres:@localhost/bdtest')
    words = pd.read_csv("dags/OnlineNewsPopularity.csv", header=0)
    words.to_sql('words_db', engine)

with DAG(
    default_args=default_args,
    dag_id='load_db_to_sql_v03',
    description='Loading data to Postgres!',
    start_date=datetime(2023, 2, 23),
    schedule='@daily'
) as dag:
    get_sqlalchemy = PythonOperator(
        task_id='sqlalchemy',
        python_callable=get_sqlalchemy
    )
    task1 = PythonOperator(
        task_id='load_dbtest',
        python_callable=load_dbtest       
    )
    get_sqlalchemy >> task1

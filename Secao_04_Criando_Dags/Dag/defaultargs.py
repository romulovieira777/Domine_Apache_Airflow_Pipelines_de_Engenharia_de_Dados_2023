from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 3),
    'email': ['teste@teste.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='default_args',
    description='Default Args',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2023, 7, 3),
    catchup=False,
    default_view='graph',
    tags=['default_args', 'processo', 'tag', 'pipeline']
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 5',
        retries=3,
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='sleep 5'
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='sleep 5'
    )

    chain(
        task3,
        task2,
        task1
    )

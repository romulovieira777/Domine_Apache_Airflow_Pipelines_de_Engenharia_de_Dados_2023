from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime


def task_write(**kwarg):
    kwarg['ti'].xcom_push(key='valorxcom1', value=10200)

def task_read(**kwarg):
    value = kwarg['ti'].xcom_pull(key='valorxcom1')
    print(f"Value Retrieved: {value}")

with DAG(
    dag_id='xcom',
    description='XCom',
    schedule_interval=None,
    start_date=datetime(2023, 7, 3),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=task_write,
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=task_read,
    )

    chain (
        task1,
        task2
    )

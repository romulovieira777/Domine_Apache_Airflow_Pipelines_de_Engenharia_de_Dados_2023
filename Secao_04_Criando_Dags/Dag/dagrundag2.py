from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime


with DAG(
    dag_id='dagrundag2',
    description='Dag Run Dag',
    schedule_interval=None,
    start_date=datetime(2023, 7, 3),
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 5'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='sleep 5'
    )
    
    chain (
        task1, 
        task2
    )
    
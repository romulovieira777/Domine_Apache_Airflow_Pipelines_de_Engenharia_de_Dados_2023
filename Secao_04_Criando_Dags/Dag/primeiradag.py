from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime


dag = DAG(
    dag_id='primeira_dag',
    description='Primeira DAG',
    schedule_interval=None,
    start_date=datetime(2023, 7, 2),
    catchup=False
)

task1 = BashOperator(
    task_id='task1',
    bash_command='sleep 5',
    dag=dag
)

task2 = BashOperator(
    task_id='task2',
    bash_command='sleep 5',
    dag=dag
)

task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 5',
    dag=dag
)

chain (
    task1,
    task2,
    task3
)
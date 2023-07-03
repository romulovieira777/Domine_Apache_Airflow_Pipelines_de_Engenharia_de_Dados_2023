from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime


with DAG(
    dag_id='trigger_dag_03',
    description='Trigger Dag',
    schedule_interval=None,
    start_date=datetime(2023, 7, 2),
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='exit 1'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='exit 1'
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='sleep 5',
        trigger_rule='all_failed'
    )

    chain(
        [task1,
        task2],
        task3
    )

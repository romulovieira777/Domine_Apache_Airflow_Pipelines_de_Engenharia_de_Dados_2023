from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime


with DAG(
    dag_id='dagrundag1',
    description='Dag Run Dag',
    schedule_interval=None,
    start_date=datetime(2023, 7, 3),
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 5'
    )

    task2 = TriggerDagRunOperator(
        task_id='task2',
        trigger_dag_id='dagrundag2',
    )
    
    chain (
        task1, 
        task2
    )

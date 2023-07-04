from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from datetime import datetime


with DAG(
    dag_id='daggroup',
    description='Dag Group',
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

    task3 = BashOperator(
        task_id='task3',
        bash_command='sleep 5'
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='sleep 5'
    )

    task5 = BashOperator(
        task_id='task5',
        bash_command='sleep 5'
    )

    task6 = BashOperator(
        task_id='task6',
        bash_command='sleep 5'
    )

    tsk_group = TaskGroup("task_group")

    task7 = BashOperator(
        task_id='task7',
        bash_command='sleep 5',
        task_group=tsk_group
    )

    task8 = BashOperator(
        task_id='task8',
        bash_command='sleep 5',
        task_group=tsk_group
    )

    task9 = BashOperator(
        task_id='task9',
        bash_command='sleep 5',
        trigger_rule='all_failed',
        task_group=tsk_group
    )
    
    chain (
        task1, 
        task2
    )
    
    chain (
        task3, 
        task4
    )
    
    chain (
        [task2, task4], 
        task5, 
        task6
    )
    
    chain (
        task6, 
        tsk_group
    )

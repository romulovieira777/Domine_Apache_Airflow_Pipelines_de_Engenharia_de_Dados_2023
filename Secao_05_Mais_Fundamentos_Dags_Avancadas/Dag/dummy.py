from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


with DAG (
    dag_id="dummy",
    description="Dummy DAG",
    schedule_interval=None,
    start_date=datetime(2023, 7, 9),
    catchup=False,
) as dag:
  
    task1 = BashOperator(
        task_id="task1",
        bash_command="sleep 3"
    )

    task2 = BashOperator(
        task_id="task2",
        bash_command="sleep 3"
    )

    task3 = BashOperator(
        task_id="task3",
        bash_command="sleep 3"
    )

    task4 = BashOperator(
        task_id="task4",
        bash_command="sleep 3"
    )

    task5 = BashOperator(
        task_id="task5",
        bash_command="sleep 3",
    )

    taskdummy = DummyOperator(
        task_id="taskdummy"
    )

    [task1, task2, task3] >> taskdummy >> [task4, task5]

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime

import random


with DAG(
    dag_id="branchs",
    description="Branchs",
    schedule_interval=None,
    start_date=datetime(2023, 7, 9),
    catchup=False,
) as dag:
    
    def gera_numero_aleatorio():
        return random.randint(1, 100)
    
    gera_numero_aleatorio_task = PythonOperator(
        task_id="gera_numero_aleatorio_task",
        python_callable=gera_numero_aleatorio
    )
    
    def avalia_numero_aleatorio(**context):
        number = context['task_instance'].xcom_pull(task_ids='gera_numero_aleatorio_task')
        if number % 2 == 0:
            return 'par_task'
        else:
            return 'impar_task'

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=avalia_numero_aleatorio,
        provide_context=True
    )

    par_task= BashOperator(
        task_id="par_task",
        bash_command="echo 'O Número é Par'"
    )

    impar_task= BashOperator(
        task_id="impar_task",
        bash_command="echo 'O Número é Impar'"
    )
    
    gera_numero_aleatorio_task >> branch_task
    branch_task >> par_task
    branch_task >> impar_task

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime


with DAG(
    dag_id="variaveis",
    description="Variaveis",
    schedule_interval=None,
    start_date=datetime(2023, 7, 9),
    catchup=False,
) as dag:
    
    def print_variable(**context):
        minha_var = Variable.get("minhavar")
        print(f"O valor da variável é: {minha_var}")
    
    taks1 = PythonOperator(
        task_id="task1",
        python_callable=print_variable
    )

    taks1

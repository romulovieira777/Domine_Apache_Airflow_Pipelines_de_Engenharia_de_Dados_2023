from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 4),
    'email': ['aws@evoluth.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    dag_id='email_test',
    description='Email',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    default_view='graph',
    tags=['email', 'processo', 'tag', 'pipeline']
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

    task4 = BashOperator(
        task_id='task4',
        bash_command='sleep 5'
    )

    task5 = BashOperator(
        task_id='task5',
        bash_command='sleep 5',
        trigger_rule='none_failed'
    )

    task6 = BashOperator(
        task_id='task6',
        bash_command='sleep 5',
        trigger_rule='none_failed'
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='aws@evoluth.com.br',
        subject='Airflow Error',
        html_content="""
            <h3> Ocorreu um erro na Dag. </h3> 
            <p> Dag: {{ dag.dag_id }} </p>
        """,
        trigger_rule='one_failed'
    )

    [task1, task2] >> task3 >> task4
    task4 >> [task5, task6, send_email]

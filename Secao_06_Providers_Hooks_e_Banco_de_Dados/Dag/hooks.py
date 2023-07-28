from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain
from datetime import datetime


with DAG(
    dag_id='Postgres_Hook',
    description='Postgres Hook',
    schedule_interval=None,
    start_date=datetime(2023, 7, 26),
    catchup=False
) as dag:
    
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        pg_hook.run("CREATE TABLE IF NOT EXISTS public.testes (id int, name varchar);")
    
    def insert_data():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        pg_hook.run("INSERT INTO public.testes VALUES (1, 'teste');", autocommit=True)
    
    def select_data(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        records = pg_hook.get_records("SELECT * FROM public.testes;")
        kwargs['ti'].xcom_push(key='query_result', value=records)
    
    def print_data():
        task_instance = ti.xcom_pull(task_ids='query_result', key='select_data_task')
        print("Dados da Tabela:")
        for row in task_instance:
            print(row)

    create_table_task = PythonOperator(
        task_id='create_table_task',
        python_callable=create_table
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_data
    )

    select_data_task = PythonOperator(
        task_id='select_data_task',
        python_callable=select_data,
        provide_context=True
    )

    print_data_task = PythonOperator(
        task_id='print_data_task',
        python_callable=print_data,
        provide_context=True
    )

    chain(
        create_table_task,
        insert_data_task,
        select_data_task,
        print_data_task
    )

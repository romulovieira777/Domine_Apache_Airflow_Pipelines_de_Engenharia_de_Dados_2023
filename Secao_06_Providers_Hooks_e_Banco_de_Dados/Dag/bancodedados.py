from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import chain
from datetime import datetime

import requests


with DAG(
    dag_id='Banco_de_Dados',
    description='Banco de Dados',
    schedule_interval=None,
    start_date=datetime(2023, 7, 26),
    catchup=False
) as dag:
    
    def print_result(ti):
        task_instance = ti.xcom_pull(task_ids='select_table')
        print("Resultado da Consulta:")
        for row in task_instance:
            print(row)

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql='''CREATE TABLE IF NOT EXISTS public.teste (
            id serial NOT NULL,
            CONSTRAINT teste_pkey PRIMARY KEY (id)
        );'''
    )

    insert_table = PostgresOperator(
        task_id='insert_table',
        postgres_conn_id='postgres_default',
        sql='''INSERT INTO public.teste (id) VALUES (1);'''
    )

    select_table = PostgresOperator(
        task_id='select_table',
        postgres_conn_id='postgres_default',
        sql='''SELECT * FROM public.teste;'''
    )

    print_result_task = PythonOperator(
        task_id='print_result_task',
        python_callable=print_result,
        provide_context=True
    )
    
    chain(
        create_table,
        insert_table,
        select_table,
        print_result_task
    )

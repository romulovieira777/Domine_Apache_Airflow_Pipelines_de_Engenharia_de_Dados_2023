from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime

import requests


with DAG(
    dag_id='HttpSensor',
    description='Http Sensor DAG',
    schedule_interval=None,
    start_date=datetime(2023, 7, 20),
    catchup=False
) as dag:
    
    def query_api():
        response = requests.get('https://api.publicapis.org/entries')
        print(response.text)

    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='connection',
        endpoint='entries',
        poke_interval=5,
        timeout=20
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=query_api
    )

    chain(
        check_api, 
        process_data
    )

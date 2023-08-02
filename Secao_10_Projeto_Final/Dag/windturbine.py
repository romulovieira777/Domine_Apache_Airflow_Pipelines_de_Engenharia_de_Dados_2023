from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

import json
import os


defaul_args = {
    'owner': 'airflow',
    'dependes_on_past': False,
    'email': ['aws@evoluth.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    dag_id='windturbine',
    description='Dados da Turbina',
    schedule_interval=None,
    start_date=datetime(2023, 8, 1),
    catchup=False,
    default_args=defaul_args,
    default_view='graph',
    doc_md="## Dag para registrar dados de turbina eólica",
    tags=['windturbine'],
) as dag:

    group_check_temp = TaskGroup(
        group_id='group_check_temp'
    )

    group_database = TaskGroup(
        group_id='group_database'
    )

    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath=Variable.get('path_file'),
        fs_conn_id='fs_default',
        poke_interval=10,
    )

    def process_file(**kwarg):
        with open(Variable.get('path_file')) as file:
            data = json.load(file)
            kwarg['ti'].xcom_push(key='idtemp', value=data['idtemp'])
            kwarg['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
            kwarg['ti'].xcom_push(key='hydraulicpressure', value=data['hydraulicpressure'])
            kwarg['ti'].xcom_push(key='temperature', value=data['temperature'])
            kwarg['ti'].xcom_push(key='timestamp', value=data['timestamp'])
        os.remove(Variable.get('path_file'))

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=process_file,
        provide_context=True,
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS sensors (
                idtemp VARCHAR(50),
                powerfactor VARCHAR(50),
                hydraulicpressure VARCHAR(50),
                temperature VARCHAR(50),
                timestamp VARCHAR(50)
            );
        """,
        task_group=group_database
    )

    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres_default',
        parameters=(
            '{{ ti.xcom_pull(task_ids="get_data", key="idtemp") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="powerfactor") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="hydraulicpressure") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="temperature") }}',
            '{{ ti.xcom_pull(task_ids="get_data", key="timestamp") }}'
        ),
        sql="""
            INSERT INTO sensors VALUES (
                idtemps,
                powerfactor,
                hydraulicpressure,
                temperature,
                timestamp
            VALUES(
                %s, %s, %s, %s, %s
            );
        """,
        task_group=group_database
    )

    send_email_alert = EmailOperator(
        task_id='send_email_alert',
        to='aws@evoluth.com.br',
        subject='Airflow alert',
        html_content=""" <h3> Alerta de Temperatura. </h3> 
        <p> Dag: windturbine </p>
        """,
        task_group=group_check_temp
    )

    send_email_normal = EmailOperator(
        task_id='send_email_normal',
        to='aws@evoluth.com.br',
        subject='Airflow advise',
        html_content=""" <h3> Temperatura normal. </h3>
        <p> Dag: windturbine </p>
        """,
        task_group=group_check_temp
    )

    def avalia_temp(**context):
        number = float(context['ti'].xcom_pull(task_ids='get_data', key='temperature'))
        if number >= 24:
            return 'group_check_temp.send_email_alert'
        else:
            return 'group_check_temp.send_email_normal'

    check_temp_branc = BranchPythonOperator(
        task_id='check_temp_branc',
        python_callable=avalia_temp,
        provide_context=True,
        task_group=group_check_temp
    )

    with group_check_temp:
        check_temp_branc >> [send_email_alert, send_email_normal]

    with group_database:
        create_table >> insert_data


    file_sensor_task >> get_data
    get_data >> group_check_temp
    get_data >> group_database

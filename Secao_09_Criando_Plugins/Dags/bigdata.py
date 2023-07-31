from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator


with DAG(
    dag_id='big_data_dag',
    schedule_interval=None,
    start_date=datetime(2023, 7, 30),
    catchup=False,
) as dag:

    big_data_task = BigDataOperator(
        task_id='big_data_task',
        path_to_csv_file='/opt/airflow/data/Churn.csv',
        path_to_save_file='/opt/airflow/data/Churn.parquet',
        file_type='parquet',
    )

    big_data_task_01 = BigDataOperator(
        task_id='big_data_task_01',
        path_to_csv_file='/opt/airflow/data/Churn.csv',
        path_to_save_file='/opt/airflow/data/Churn.json',
        file_type='json',
    )

    big_data_task >> big_data_task_01

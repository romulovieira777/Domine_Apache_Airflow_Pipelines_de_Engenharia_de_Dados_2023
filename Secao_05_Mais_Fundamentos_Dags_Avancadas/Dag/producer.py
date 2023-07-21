from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd


with DAG(
    dag_id='Producer',
    description='Producer DAG',
    schedule_interval=None,
    start_date=datetime(2023, 7, 20),
    catchup=False
) as dag:
    
    mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

    def my_file():
        dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
        dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=";")
    
    t1 = PythonOperator(
        task_id='my_file',
        python_callable=my_file,
        outlets=[mydataset]
    )

    t1

from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd


mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

with DAG(
    dag_id='Consumer',
    description='Consumer DAG',
    schedule=[mydataset],
    start_date=datetime(2023, 7, 20),
    catchup=False
) as dag:

    def my_file():
        dataset = pd.read_csv("/opt/airflow/data/Churn_new.csv", sep=";")
        dataset.to_csv("/opt/airflow/data/Churn_new2.csv", sep=";")
    
    t1 = PythonOperator(
        task_id='my_file',
        python_callable=my_file,
        provide_context=True
    )

    t1

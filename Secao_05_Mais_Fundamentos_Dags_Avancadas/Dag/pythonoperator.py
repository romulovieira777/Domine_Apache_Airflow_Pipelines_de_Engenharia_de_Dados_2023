from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as sts


with DAG(
    dag_id='PythonOperator',
    description='PythonOperator',
    schedule_interval=None,
    start_date=datetime(2023, 7, 20),
    catchup=False
) as dag:
    
    def data_cleaner():
        dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
        dataset.columns = ['Id', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio', 'Saldo', 'Produtos', 'TemCartCredito', 'Ativo', 'Salario', 'Saiu']

        mediana = sts.median(dataset['Salario'])
        dataset['Salario'].fillna(mediana,inplace=True)

        dataset['Genero'].fillna('Masculino', inplace=True)

        mediana = sts.median(dataset['Idade'])
        dataset.loc[(dataset['Idade']<0) | (dataset['Idade']> 120), 'Idade'] = mediana

        dataset.drop_duplicates(subset="Id", keep="first", inplace=True)

        dataset.to_csv("/opt/airflow/data/Churn_Clean.csv", sep=";", index=False)

    t1 = PythonOperator(
        task_id='data_cleaner',
        python_callable=data_cleaner
    )

    t1

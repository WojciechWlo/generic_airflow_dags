from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os

JSON_PATH = os.path.join(os.path.dirname(__file__), 'transfers.json')

def read_transfers_file():
    with open(JSON_PATH, 'r') as f:
        data = json.load(f)
    print("Transfers data:", data)


with DAG(
    dag_id='read_transfers_json',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example'],
) as dag:

    read_json = PythonOperator(
        task_id='read_transfers_file',
        python_callable=read_transfers_file,
    )

    read_json
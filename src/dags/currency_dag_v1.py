from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def save_json_file():
    url = "https://economia.awesomeapi.com.br/json/last/USD-BRL"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload).json()
    with open('data.json', 'w') as f:
        json.dump(response, f)


with DAG("currency_dag_v1", start_date=datetime(2024, 1 ,14), 
    schedule_interval="* * * * *", default_args=default_args, catchup=False) as dag:

    currency_api = HttpSensor(
        task_id="is_currecy_availabe",
        http_conn_id="currency_api",
        endpoint="json/last/USD-BRL",
        response_check=lambda response: "USDBRL" in response.text,
        poke_interval=5,
        timeout=1
    )

    save_json = PythonOperator(
        task_id = "save_json",
        python_callable = save_json_file
    )


currency_api >> save_json
import json
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

def _get_records(endpoint, api_table_token, ti):
    headers={"Authorization": "Bearer {api_table_token}".format(api_table_token = api_table_token)}
    res = requests.get(url = endpoint, headers = headers)
    records = json.loads(res.text)['data']['records']
    ti.xcom_push(key = 'api_table_records_of_dataset', value =records)

with DAG(
        dag_id='example_apitable_get_records_of_dataset',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['example', 'apitable']
) as dag:
    access_key = Variable.get("s3_access_secret_key")
    secret_key = Variable.get("s3_secret_key")
    api_table_token = Variable.get("api_table_token_secret")
    api_table_base_url = Variable.get("api_table_base_url")

    get_records = PythonOperator(
        task_id = 'get_records_of_dataset',
        python_callable= _get_records,
        op_kwargs={
            'endpoint': "{api_table_base_url}/fusion/v1/datasheets/dsteNLuhzWM8T1mf1g/records".format(api_table_base_url = api_table_base_url),
            'api_table_token': api_table_token
        },
    )

    get_records
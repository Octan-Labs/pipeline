import json
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

def escape_for_sql(input_string):
    # Replace single quotes with two single quotes
    escaped_string = str(input_string).replace("'", "''")
    return escaped_string

def _get_records(endpoint, api_table_token, ti):
    headers={"Authorization": "Bearer {api_table_token}".format(api_table_token = api_table_token)}
    res = requests.get(url = endpoint, headers = headers)
    records = json.loads(res.text)['data']['records']
    value = ','.join(f"('{e['recordId']}', {e['createdAt']}, {e['updatedAt']}, '{e['fields']['address']}', '{escape_for_sql(e['fields'].get('identity', ''))}', '{escape_for_sql(e['fields'].get('category', ''))}', '{escape_for_sql(e['fields'].get('version', ''))}', {escape_for_sql(e['fields'].get('update', ''))}, '{escape_for_sql(e['fields'].get('explorer_link').get('text', ''))}', '{escape_for_sql(e['fields'].get('explorer_tagname', ''))}', '{escape_for_sql(e['fields'].get('explorer_contractname', ''))}', '{escape_for_sql(e['fields'].get('crawled_oklink', ''))}', '{escape_for_sql(e['fields'].get('note', ''))}', '{escape_for_sql(e['fields'].get('safety', ''))}', '{escape_for_sql(e['fields'].get('assign', ''))}', '{escape_for_sql(e['fields'].get('project_name', [''])[0])}')" for e in records)
    ti.xcom_push(key = 'api_table_records_of_dataset', value=value)

with DAG(
        dag_id='import_records_of_dataset_from_apitable_to_clickhouse',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['apitable', "clickhouse"]
) as dag:
    api_table_token = Variable.get("api_table_token_secret")
    api_table_base_url = Variable.get("api_table_base_url")

    PythonOperator(
        task_id = 'get_records_of_dataset',
        python_callable= _get_records,
        op_kwargs={
            'endpoint': "{api_table_base_url}/fusion/v1/datasheets/{dataset}/records?pageNum={pageNum}"
                .format(
                    api_table_base_url = api_table_base_url, 
                    dataset = "{{ dag_run.conf['dataset'] }}",
                    pageNum = "{{ dag_run.conf['pageNum'] }}"
                ),
            'api_table_token': api_table_token
        },
    ) >> ClickHouseOperator(
        task_id='import_to_clickhouse',
        database='default',
        sql=(
            '''
                INSERT INTO {table_name}
                VALUES {values}
            '''.format(
                    table_name = "{{ dag_run.conf['table_name'] }}",
                    values = "{{ task_instance.xcom_pull(task_ids='get_records_of_dataset', key='api_table_records_of_dataset') }}"
                )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )
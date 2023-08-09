from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
        dag_id='import_from_s3_to_clickhouse_by_hour',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['clickhouse']
) as dag:
    access_key = Variable.get("s3_access_secret_key")
    secret_key = Variable.get("s3_secret_key")

    ClickHouseOperator(
        task_id='import_from_s3_to_clickhouse_by_hour',
        database='default',
        sql=(
            '''
                INSERT INTO {table_name}
                SELECT *
                FROM
                s3(
                '{base_s3_url}/{schema}/date%3D{date}/*/*.parquet',
                '{access_key}', 
                '{secret_key}', 
                'Parquet'
                )
                SETTINGS parallel_distributed_insert_select=1, async_insert=1, wait_for_async_insert=1,
                max_threads=4, max_insert_threads=4, input_format_parallel_parsing=0;
            '''.format(
                    table_name = "{{ dag_run.conf['table_name'] }}",
                    schema = "{{ dag_run.conf['schema'] }}",
                    date = "{{ dag_run.conf['date'] }}",
                    base_s3_url = "{{ dag_run.conf['base_s3_url'] }}", 
                    access_key = access_key, 
                    secret_key = secret_key
                )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import task

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
        tags=['clickhouse'],
        params={
            "is_public": Param(default=False, type="boolean"),
            "table_name": Param(default="", type="string"),
            "base_s3_url": Param(default="", type="string"),
            "schema": Param(default="", type="string"),
            "date": Param(default=f"{date.today()}", type="string", format="date",),
            "table_fields": Param(default="", type="string"),
            "source_fields": Param(default="", type="string"),
        },
) as dag:
    @task(task_id="import_from_s3_to_clickhouse_by_hour")
    def import_from_s3_to_clickhouse_by_hour(ds=None, **context):
        access_key = Variable.get("s3_access_secret_key")
        secret_key = Variable.get("s3_secret_key")
        credentials = "'NOSIGN'" if context["params"].get('is_public') else "'{access_key}', '{secret_key}'".format(access_key=access_key, secret_key=secret_key)
        command = '''
                    INSERT INTO {table_name} ({table_fields})
                    SELECT {source_fields}
                    FROM
                    s3(
                    '{base_s3_url}/{schema}/date%3D{date}/*/*.parquet',
                    {credentials}, 
                    'Parquet')
                    SETTINGS async_insert=1, wait_for_async_insert=1,
                    max_threads=4, max_insert_threads=4, input_format_parallel_parsing=0;
                '''.format(
                        table_name=context['params'].get('table_name'),
                        schema=context['params'].get('schema'),
                        date=context['params'].get('date'),
                        base_s3_url=context['params'].get('base_s3_url'),
                        table_fields=context['params'].get('table_fields'),
                        source_fields=context['params'].get('source_fields'),
                        credentials=credentials
                    )

        ClickHouseOperator(
            task_id='import_from_s3_to_clickhouse_by_hour',
            database='default',
            sql=command,
            clickhouse_conn_id="clickhouse_conn"
        )

    import_from_s3_to_clickhouse_by_hour_task = import_from_s3_to_clickhouse_by_hour()
    import_from_s3_to_clickhouse_by_hour_task
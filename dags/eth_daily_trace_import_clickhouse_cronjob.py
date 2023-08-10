from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
        dag_id='eth_daily_trace_import_clickhouse',
        default_args=default_args,
        schedule="@daily",
        catchup=False,
        tags=['eth', 'clickhouse']
) as dag:
    wait_for_eth_daily_trace_indexing = ExternalTaskSensor(
        task_id='wait_for_eth_daily_trace_indexing',
        external_dag_id='eth_daily_trace_indexing',
        external_task_id='eth_indexer',
        failed_states=["failed", "skipped"]
    )

    base_s3_url = Variable.get("eth_s3_url")

    trigger_import_trace = TriggerDagRunOperator(
        task_id='trigger_import_trace',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_trace",
            "schema": "traces",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
            "base_s3_url": base_s3_url
        },
        wait_for_completion=True,
        failed_states=["false"]
    )

    trigger_import_token = TriggerDagRunOperator(
        task_id='trigger_import_token',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_token",
            "schema": "tokens",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
            "base_s3_url": base_s3_url
        },
        reset_dag_run=True,
        wait_for_completion=True,
        failed_states=["false"]
    )

    trigger_import_contract = TriggerDagRunOperator(
        task_id='trigger_import_contract',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_contract",
            "schema": "contracts",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
            "base_s3_url": base_s3_url
        },
        wait_for_completion=True,
        failed_states=["false"]
    )

    wait_for_eth_daily_trace_indexing >> [trigger_import_trace, trigger_import_token, trigger_import_contract]
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
        dag_id='eth_daily_non_trace_import_clickhouse',
        default_args=default_args,
        schedule="@daily",
        catchup=False,
        tags=['eth', 'clickhouse']
) as dag:
    wait_for_eth_daily_non_trace_indexing = ExternalTaskSensor(
        task_id='wait_for_eth_daily_non_trace_indexing',
        external_dag_id='eth_daily_trace_indexing',
        external_task_id='eth_indexer',
        failed_states=["failed", "skipped"]
    )

    trigger_import_block = TriggerDagRunOperator(
        task_id='trigger_import_block',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_block",
            "schema": "blocks",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
        },
        wait_for_completion=True,
        failed_states=["false"]
    )

    trigger_import_transaction = TriggerDagRunOperator(
        task_id='trigger_import_transaction',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_transaction",
            "schema": "transactions",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
        },
        wait_for_completion=True,
        failed_states=["false"]
    )

    trigger_import_log = TriggerDagRunOperator(
        task_id='trigger_import_log',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_log",
            "schema": "logs",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
        },
        wait_for_completion=True,
        failed_states=["false"]
    )

    trigger_import_token_transfer = TriggerDagRunOperator(
        task_id='trigger_import_token_transfer',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_token_transfer",
            "schema": "token_transfers",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
        },
        wait_for_completion=True,
        failed_states=["false"]
    )

    wait_for_eth_daily_non_trace_indexing >> [trigger_import_block, trigger_import_transaction, trigger_import_log, trigger_import_token_transfer]
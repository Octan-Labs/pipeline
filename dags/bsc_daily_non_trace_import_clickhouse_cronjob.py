# from airflow import DAG
# from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.models import Variable
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 8, 4),
#     'retries': 1,
#     'retry_delay': timedelta(seconds=15)
# }

# with DAG(
#         dag_id='bsc_daily_non_trace_import_clickhouse',
#         default_args=default_args,
#         schedule="10 0 * * *",
#         catchup=False,
#         tags=['bsc', 'clickhouse']
# ) as dag:
#     wait_for_bsc_daily_non_trace_indexing = ExternalTaskSensor(
#         task_id='wait_for_bsc_daily_non_trace_indexing',
#         external_dag_id='bsc_daily_non_trace_indexing',
#         failed_states=["failed"]
#     )

#     base_s3_url = Variable.get("bsc_s3_url")

#     trigger_import_block = TriggerDagRunOperator(
#         task_id='trigger_import_block',
#         trigger_dag_id='import_from_s3_to_clickhouse_by_hour',
#         conf={
#             "table_name": "bsc_block",
#             "schema": "blocks",
#             "date": "{{ data_interval_start | ds }}",
#             "base_s3_url": base_s3_url
#         },
#         reset_dag_run=True,
#         wait_for_completion=True,
#         failed_states=["false"]
#     )

#     trigger_import_transaction = TriggerDagRunOperator(
#         task_id='trigger_import_transaction',
#         trigger_dag_id='import_from_s3_to_clickhouse_by_hour',
#         conf={
#             "table_name": "bsc_transaction",
#             "schema": "transactions",
#             "date": "{{ data_interval_start | ds }}",
#             "base_s3_url": base_s3_url
#         },
#         reset_dag_run=True,
#         wait_for_completion=True,
#         failed_states=["false"]
#     )

#     trigger_import_log = TriggerDagRunOperator(
#         task_id='trigger_import_log',
#         trigger_dag_id='import_from_s3_to_clickhouse_by_hour',
#         conf={
#             "table_name": "bsc_log",
#             "schema": "logs",
#             "date": "{{ data_interval_start | ds }}",
#             "base_s3_url": base_s3_url
#         },
#         reset_dag_run=True,
#         wait_for_completion=True,
#         failed_states=["false"]
#     )

#     trigger_import_token_transfer = TriggerDagRunOperator(
#         task_id='trigger_import_token_transfer',
#         trigger_dag_id='import_from_s3_to_clickhouse_by_hour',
#         conf={
#             "table_name": "bsc_token_transfer",
#             "schema": "token_transfers",
#             "date": "{{ data_interval_start | ds }}",
#             "base_s3_url": base_s3_url
#         },
#         reset_dag_run=True,
#         wait_for_completion=True,
#         failed_states=["false"]
#     )

#     wait_for_bsc_daily_non_trace_indexing >> [trigger_import_block, trigger_import_transaction, trigger_import_log, trigger_import_token_transfer]
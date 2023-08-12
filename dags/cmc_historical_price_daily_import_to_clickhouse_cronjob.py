# from airflow import DAG
# from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.models import Variable

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 8, 4),
#     'retries': 1,
#     'retry_delay': timedelta(seconds=15)
# }

# with DAG(
#         dag_id='cmc_historical_price_daily_import_to_clickhouse',
#         default_args=default_args,
#         schedule="@daily",
#         catchup=False,
#         tags=['cmc', 'clickhouse']
# ) as dag:
#     wait_for_cmc_historical_price_daily_indexing = ExternalTaskSensor(
#         task_id='wait_for_cmc_historical_price_daily_indexing',
#         external_dag_id='cmc_historical_price_daily_indexing',
#         external_task_id='cmc_daily_historical_price_indexer',
#         failed_states=["failed", "skipped"]
#     )

#     base_s3_url = Variable.get("cmc_s3_url")

#     access_key = Variable.get("s3_access_secret_key")
#     secret_key = Variable.get("s3_secret_key")

#     import_from_s3_to_clickhouse = ClickHouseOperator(
#         task_id='import_from_s3_to_clickhouse',
#         database='default',
#         sql=(
#             '''
#                 INSERT INTO cmc_historical
#                 SELECT *
#                 FROM
#                 s3(
#                 '{base_s3_url}/cmc_historicals/{date}.csv',
#                 '{access_key}', 
#                 '{secret_key}', 
#                 'CSV'
#                 )
#                 SETTINGS parallel_distributed_insert_select=1, async_insert=1, wait_for_async_insert=1,
#                 max_threads=4, max_insert_threads=4, input_format_parallel_parsing=0;
#             '''.format(
#                     base_s3_url = base_s3_url, 
#                     access_key = access_key, 
#                     secret_key = secret_key,
#                     date = "{{ data_interval_start.subtract(days=1) | ds }}"
#                 )
#         ),
#         clickhouse_conn_id="clickhouse_conn"
#     )

#     wait_for_cmc_historical_price_daily_indexing >> import_from_s3_to_clickhouse

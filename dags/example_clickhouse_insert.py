# from airflow import DAG
# from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 8, 4),
#     'retries': 1,
#     'retry_delay': timedelta(seconds=15)
# }

# with DAG(
#         dag_id='example_clickhouse_insert',
#         default_args=default_args,
#         schedule_interval=None,
#         catchup=False,
#         tags=['example', 'clickhouse']
# ) as dag:
#     ClickHouseOperator(
#         task_id='test_insert_clickhouse',
#         database='default',
#         sql=(
#             '''
#                 INSERT INTO my_first_table (user_id, message, timestamp, metric) VALUES
#                     (101, 'Hello, ClickHouse!',                                 now(),       -1.0    ),
#                     (102, 'Insert a lot of rows per batch',                     yesterday(), 1.41421 ),
#                     (102, 'Sort your data based on your commonly-used queries', today(),     2.718   ),
#                     (101, 'Granules are the smallest chunks of data read',      now() + 5,   3.14159 )
#             ''','''
#                  SELECT * FROM my_first_table ORDER BY timestamp LIMIT 1000
#             '''

#             # result of the last query is pushed to XCom
#         ),
#         clickhouse_conn_id="clickhouse_conn"
#     ) >> PythonOperator(
#         task_id='print_result',
#         provide_context=True,
#         python_callable=lambda task_instance, **_:
#             # pulling XCom value and printing it
#             print(task_instance.xcom_pull(task_ids='test_insert_clickhouse')),
#     )
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
#         dag_id='example_clickhouse_operator',
#         default_args=default_args,
#         schedule_interval=None,
#         catchup=False,
#         tags=['example', 'clickhouse']
# ) as dag:
#     ClickHouseOperator(
#         task_id='test_select_clickhouse',
#         database='system',
#         sql=(
#             '''
#               SELECT * from processes
#             '''
#             # result of the last query is pushed to XCom
#         ),
#         clickhouse_conn_id="clickhouse_conn"
#     ) >> PythonOperator(
#         task_id='print_month_income',
#         provide_context=True,
#         python_callable=lambda task_instance, **_:
#             # pulling XCom value and printing it
#             print(task_instance.xcom_pull(task_ids='test_select_clickhouse')),
#     )
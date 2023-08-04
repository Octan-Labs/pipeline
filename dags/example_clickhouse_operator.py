from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023, 8, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
        dag_id='example_clickhouse_operator',
        default_args=default_args
) as dag:
    ClickHouseOperator(
        task_id='test_select_clickhouse',
        database='system',
        sql=(
            '''
              SELECT * from processes
            '''
            # result of the last query is pushed to XCom
        ),
        clickhouse_conn_id="clickhouse_conn"
    )
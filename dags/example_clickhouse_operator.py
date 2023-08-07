from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023, 8, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
    "schedule_interval": None,
}

with DAG(
        dag_id='example_clickhouse_operator',
        default_args=default_args,
        tags=["example"]
) as dag:
    ClickHouseOperator(
        task_id='test_clickhouse_operator',
        database='system',
        sql=(
            '''
                SELECT * from `system`.processes p LIMIT 1000
            '''
            # result of the last query is pushed to XCom
        ),
        clickhouse_conn_id="clickhouse_conn"
    ) >> PythonOperator(
        task_id='print_result',
        provide_context=True,
        python_callable=lambda task_instance, **_:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids='test_clickhouse_operator')),
    )
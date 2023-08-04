from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
        dag_id='example_clickhouse_operator',
        'start_date': datetime(2023, 8, 4),
) as dag:
    ClickHouseOperator(
        task_id='test_select_clickhouse',
        database='default',
        sql=(
            '''
              SELECT * from system.processes
            '''
            # result of the last query is pushed to XCom
        ),
        clickhouse_conn_id="clickhouse_conn"
    )
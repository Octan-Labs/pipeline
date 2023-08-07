from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023, 8, 7),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
    "schedule_interval": None,
}

with DAG(
        dag_id='example_clickhouse_query',
        default_args=default_args,
        tags=["example"]
) as dag:
    database = '{{ dag_run.conf["database"] }}'

    query = ClickHouseOperator(
        task_id='test_query',
        database=database,
        sql=(
            '''
                {{ dag_run.conf['query'] }}
            '''
        ),
        clickhouse_conn_id="clickhouse_conn"
    ) 

    print_result = PythonOperator(
        task_id='print_result',
        provide_context=True,
        python_callable=lambda task_instance, **_:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids='test_clickhouse_operator')),
    )

    query >> print_result

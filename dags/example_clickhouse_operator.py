from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
        dag_id='update_income_aggregate',
        # start_date=days_ago(2),
) as dag:
    ClickHouseOperator(
        task_id='update_income_aggregate',
        database='default',
        sql=(
            '''
              SELECT * from system.processes
            '''
            # result of the last query is pushed to XCom
        ),
        host='chi-simple-01-simple-0-0-0.chi-simple-01-simple-0-0.clickhouse-operator.svc.cluster.local',
        port="8123",
        login="clickhouse_operator",
        password='clickhouse_operator_password'
    )
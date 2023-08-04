from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
        dag_id='update_income_aggregate',
        start_date=days_ago(2),
) as dag:
    ClickHouseOperator(
        task_id='update_income_aggregate',
        database='default',
        sql=(
            '''
                INSERT INTO aggregate
                SELECT eventDt, sum(price * qty) AS income FROM sales
                WHERE eventDt = '{{ ds }}' GROUP BY eventDt
            ''', '''
                OPTIMIZE TABLE aggregate ON CLUSTER {{ var.value.cluster_name }}
                PARTITION toDate('{{ execution_date.format('%Y-%m-01') }}')
            ''', '''
                SELECT sum(income) FROM aggregate
                WHERE eventDt BETWEEN
                    '{{ execution_date.start_of('month').to_date_string() }}'
                    AND '{{ execution_date.end_of('month').to_date_string() }}'
            ''',
            # result of the last query is pushed to XCom
        ),
        clickhouse_conn_id='chi-simple-01-simple-0-0-0.chi-simple-01-simple-0-0.clickhouse-operator.svc.cluster.local',
    ) >> PythonOperator(
        task_id='print_month_income',
        provide_context=True,
        python_callable=lambda task_instance, **_:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids='update_income_aggregate')),
    )
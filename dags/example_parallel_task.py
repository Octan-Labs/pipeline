import time
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

def test(**context):
    time.sleep(100)

default_args = {
    "owner": 'Airflow',
    "start_date": datetime(2021, 1, 1),
}

dag = DAG(
    dag_id='test_24_task_1',
    schedule_interval="0 * * * *",
    default_args=default_args,
    catchup=False,
    tags=['example']
)

with dag:
    for i in range(24):
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=test,
            provide_context=True
        )
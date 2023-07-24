from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time

# Define the DAG
dag = DAG(
    'my_dag',
    description='My DAG with custom sequence',
    schedule_interval=timedelta(days=1),  # Adjust the interval as per your requirement
    start_date=datetime(2023, 7, 24),
    catchup=False
)

# Define the tasks
def task_waiting_for_cmc_historical_index():
    time.sleep(60)
    # Your implementation for waiting_for_cmc_historical_index
    pass

def task_eth_daily_indexing_cronjob():
    time.sleep(180)
    # Your implementation for eth_daily_indexing_cronjob
    pass

def task_trigger_pre_tx_and_volume_job():
    time.sleep(300)
    # Your implementation for trigger_pre_tx_and_volume_job
    pass

def task_trigger_uaw_job():
    time.sleep(900)
    # Your implementation for trigger_uaw_job
    pass

# Define the DAG tasks and set up the dependencies
wait_task = PythonOperator(
    task_id='waiting_for_cmc_historical_index',
    python_callable=task_waiting_for_cmc_historical_index,
    dag=dag
)

eth_index_task = PythonOperator(
    task_id='eth_daily_indexing_cronjob',
    python_callable=task_eth_daily_indexing_cronjob,
    dag=dag
)

pre_tx_volume_task = PythonOperator(
    task_id='trigger_pre_tx_and_volume_job',
    python_callable=task_trigger_pre_tx_and_volume_job,
    dag=dag
)

uaw_task = PythonOperator(
    task_id='trigger_uaw_job',
    python_callable=task_trigger_uaw_job,
    dag=dag
)

# Set up the dependencies
[wait_task, eth_index_task] >> pre_tx_volume_task >> uaw_task

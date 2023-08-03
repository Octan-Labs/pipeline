from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 8, 29),
    'retries': 5,
    'retry_delay': timedelta(minutes=30),
    'depends_on_past': False,
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

with DAG(
    'eth_pre_tx_volume_uaw_daily_cronjob',
    default_args=default_args,
    description='Run eth pre-tx & volume & UAW daily',
    schedule="@daily",
    catchup=False,
    tags=['eth']
) as dag:

    waiting_for_eth_non_trace_daily_indexing = ExternalTaskSensor(
            task_id='waiting_for_eth_non_trace_daily_indexing',
            external_dag_id='eth_daily_non_trace_indexing',
            external_task_id='eth_indexer',
            failed_states=["failed"]
        )
    
    waiting_for_eth_trace_daily_indexing = ExternalTaskSensor(
            task_id='waiting_for_eth_trace_daily_indexing',
            external_dag_id='eth_daily_trace_indexing',
            external_task_id='eth_indexer',
            failed_states=["failed"]
        )

    waiting_for_cmc_historical_daily_indexing = ExternalTaskSensor(
            task_id='waiting_for_cmc_historical_daily_indexing',
            external_dag_id='cmc_historical_price_daily_indexing',
            external_task_id='cmc_daily_historical_price_indexer',
            failed_states=["failed"]
        )

    base_path = "s3a://octan-labs-ethereum/export-by-date"

    trigger_pre_tx_and_volume_job = TriggerDagRunOperator(
            task_id='trigger_pre_tx_and_volume_calculation_job',
            trigger_dag_id='pre_tx_and_volume_calculation',
            conf={
                "base_path": base_path,
                "start": "{{ data_interval_start.subtract(days=1) | ds }}",
                "end": "{{ data_interval_start.subtract(days=1) | ds }}",
                "name": "eth",
                "cmc_id": "1027"
            },
            wait_for_completion=True,
            failed_states=["false"]
        )

    trigger_uaw_job = TriggerDagRunOperator(
            task_id='trigger_UAW_calculation_job',
            trigger_dag_id='UAW_calculation',
            conf={
                "base_path": base_path,
                "start": "{{ data_interval_start.subtract(days=1) | ds }}",
                "end": "{{ data_interval_start.subtract(days=1) | ds }}",
            },
            wait_for_completion=True,
            failed_states=["false"]
        )

    [waiting_for_eth_non_trace_daily_indexing, waiting_for_eth_trace_daily_indexing, waiting_for_cmc_historical_daily_indexing] >> trigger_pre_tx_and_volume_job >> trigger_uaw_job

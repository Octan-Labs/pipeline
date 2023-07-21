from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2015, 7, 30),
    'retries': 5,
    'retry_delay': timedelta(minutes=30),
    'depends_on_past': False,
    'max_active_runs': 5,
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

with DAG(
    "eth_daily_indexing",
    default_args=default_args,
    description='Run eth indexer daily',
    schedule="@daily",
    catchup=False
) as dag:

    env_vars = [
        k8s.V1EnvVar(name='START', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(name='END', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(name='PARTITION_TO_HOUR', value='false'), 
        k8s.V1EnvVar(name='ENTITY_TYPES', value='block, transaction, log, token_transfer, trace, contract, token')
    ]
    secrets = [
        Secret(
            deploy_type='env',
            deploy_target='PROVIDER_URI',
            secret='eth-indexer-secret',
            key='provider-uri'
        ),
        Secret(
            deploy_type='env',
            deploy_target='OUTPUT_DIR',
            secret='eth-indexer-secret',
            key='output-dir'
        ),
        Secret(
            deploy_type='env',
            deploy_target='AWS_ACCESS_KEY_ID',
            secret='indexer-aws-key',
            key='aws_access_key_id'
        ),
        Secret(
            deploy_type='env',
            deploy_target='AWS_SECRET_ACCESS_KEY',
            secret='indexer-aws-key',
            key='aws_secret_access_key'
        ),
    ]

    eth_daily_indexing_cronjob = KubernetesPodOperator(
                image='octanlabs/ethereumetl:0.0.4',
                arguments=['export_all'],
                env_vars=env_vars,
                secrets=secrets,
                # container_resources=k8s.V1ResourceRequirements(
                #   request={"memory": "4Gi"},
                # ),
                name='eth_indexer',
                task_id='eth_indexer',
                retries=5,
                retry_delay=timedelta(minutes=5),
            )


    waiting_for_cmc_historical_index = ExternalTaskSensor(
            task_id='waiting_for_cmc_historical_index',
            external_dag_id='cmc_historical_price_daily_indexing',
            external_task_id='cmc_daily_historical_price_indexer'
        )

    base_path = "s3a://octan-labs-ethereum/export-by-date"

    trigger_pre_tx_and_volume_job = TriggerDagRunOperator(
            task_id='trigger_pre_tx_and_volume_calculation_job',
            trigger_dag_id='pre_tx_and_volume_calculation',
            conf={
                "base_path": base_path,
                "start": "{{ data_interval_start.subtract(days=1) | ds }}",
                "end": "{{ data_interval_start.subtract(days=1) | ds }}",
                "name": "bsc",
                "symbol": "BNB"
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

    waiting_for_cmc_historical_index >> eth_daily_indexing_cronjob >> trigger_pre_tx_and_volume_job >> trigger_uaw_job
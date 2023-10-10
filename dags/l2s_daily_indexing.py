from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'depends_on_past': False,
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

with DAG(
    'l2s_daily_indexing',
    default_args=default_args,
    description='Run l2s indexer daily',
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['l2s']
) as dag:

    secrets = [
        Secret(
            deploy_type='env',
            deploy_target='ETH_RPC_URL',
            secret='l2s-indexer-secret',
            key='provider-uri'
        ),
        Secret(
            deploy_type='env',
            deploy_target='MULTICALL_CONTRACT_ADDRESS',
            secret='l2s-indexer-secret',
            key='multicall-contract-address'
        ),
        Secret(
            deploy_type='env',
            deploy_target='POSTGRES_HOST',
            secret='l2s-indexer-secret',
            key='postgres-host'
        ),
        Secret(
            deploy_type='env',
            deploy_target='POSTGRES_PORT',
            secret='l2s-indexer-secret',
            key='postgres-port'
        ),
        Secret(
            deploy_type='env',
            deploy_target='POSTGRES_DATABASE',
            secret='l2s-indexer-secret',
            key='postgres-database'
        ),
        Secret(
            deploy_type='env',
            deploy_target='POSTGRES_USER',
            secret='l2s-indexer-secret',
            key='postgres-user'
        ),
        Secret(
            deploy_type='env',
            deploy_target='POSTGRES_PASSWORD',
            secret='l2s-indexer-secret',
            key='postgres-password'
        ),
        Secret(
            deploy_type='env',
            deploy_target='CLICKHOUSE_HOST',
            secret='l2s-indexer-secret',
            key='clickhouse-host'
        ),
        Secret(
            deploy_type='env',
            deploy_target='CLICKHOUSE_PASSWORD',
            secret='l2s-indexer-secret',
            key='clickhouse-password'
        ),
        Secret(
            deploy_type='env',
            deploy_target='CLICKHOUSE_DATABASE',
            secret='l2s-indexer-secret',
            key='clickhouse-database'
        ),
        Secret(
            deploy_type='env',
            deploy_target='CLICKHOUSE_USER',
            secret='l2s-indexer-secret',
            key='clickhouse-user'
        ),
    ]

    waiting_for_eth_non_trace_daily_indexing = ExternalTaskSensor(
        task_id='waiting_for_eth_non_trace_daily_indexing',
        external_dag_id='eth_daily_non_trace_indexing',
        external_task_id='eth_non_trace_index',
        failed_states=["failed"]
    )
    waiting_for_cmc_historical_price_daily_indexing = ExternalTaskSensor(
        task_id='waiting_for_cmc_historical_price_daily_indexing',
        external_dag_id='cmc_historical_price_daily_indexing',
        external_task_id='import_from_s3_to_clickhouse',
        failed_states=["failed"]
    )
    max_block_number_on_date = ClickHouseOperator(
        task_id='select_blocknumber',
        database='default',
        sql=(
            '''
                SELECT MAX(number) FROM eth_block 
                WHERE formatDateTime(timestamp ,'%Y-%m-%d') = '{{ data_interval_start.subtract(days=1) | ds }}'
            '''
            # result of the last query is pushed to XCom
        ),
        clickhouse_conn_id="clickhouse_conn"
    ) 
    l2s_indexing = KubernetesPodOperator(
        image='octanlabs/l2s-indexer:0.0.2',
        env_vars=[
            k8s.V1EnvVar(
                name='BLOCK_NUMBER',
                value="{{ task_instance.xcom_pull(task_ids='select_blocknumber')[0][0] }}"),
            k8s.V1EnvVar(
                name='BLOCK_DATE',
                value="{{ data_interval_start.subtract(days=1) | ds }}"),
            k8s.V1EnvVar(
                name='POSTGRES_SSL_ENABLED',
                value='true'),
            k8s.V1EnvVar(
                name='PGSSLMODE',
                value='require'),
        ],
        secrets=secrets,
        container_resources=k8s.V1ResourceRequirements(),
        name='l2s_index',
        task_id='l2s_index',
        random_name_suffix=True,
    )

    [waiting_for_eth_non_trace_daily_indexing, waiting_for_cmc_historical_price_daily_indexing] >> max_block_number_on_date >> l2s_indexing

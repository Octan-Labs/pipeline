from airflow import DAG
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 7, 31),
    'retry_delay': timedelta(minutes=30),
    'depends_on_past': False,
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

with DAG(
    "eth_daily_non_trace_indexing",
    default_args=default_args,
    description='Run eth indexer daily',
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['eth']
) as dag:

    env_vars = [
        k8s.V1EnvVar(
            name='START', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(
            name='END', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(name='PARTITION_TO_HOUR', value='false'),
        k8s.V1EnvVar(name='ENTITY_TYPES',
                     value='block, transaction, log')
    ]
    secrets = [
        Secret(
            deploy_type='env',
            deploy_target='PROVIDER_URI',
            secret='eth-indexer-secret',
            key='nontrace-provider-uri'
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

    eth_block, eth_transaction, eth_log = Variable.get('eth_block_table_name'), Variable.get(
        'eth_transaction_table_name'), Variable.get('eth_log_table_name')

    KubernetesPodOperator(
        image='octanlabs/ethereumetl:0.0.13',
        arguments=['export_all'],
        env_vars=env_vars,
        secrets=secrets,
        container_resources=k8s.V1ResourceRequirements(),
        name='eth_non_trace_index',
        task_id='eth_non_trace_index',
        random_name_suffix=True,
    ) >> [
        ClickHouseOperator(
            task_id='optimize_eth_block',
            database='default',
            sql=(f'OPTIMIZE TABLE {eth_block} FINAL DEDUPLICATE SETTINGS alter_sync = 0, optimize_skip_merged_partitions = 1'),
            clickhouse_conn_id="clickhouse_conn"
        ),
        ClickHouseOperator(
            task_id='optimize_eth_transaction',
            database='default',
            sql=(f'OPTIMIZE TABLE {eth_transaction} FINAL DEDUPLICATE SETTINGS alter_sync = 0, optimize_skip_merged_partitions = 1'),
            clickhouse_conn_id="clickhouse_conn"
        ),
        ClickHouseOperator(
            task_id='optimize_eth_log',
            database='default',
            sql=(f'OPTIMIZE TABLE {eth_log} FINAL DEDUPLICATE SETTINGS alter_sync = 0, optimize_skip_merged_partitions = 1'),
            clickhouse_conn_id="clickhouse_conn"
        )
    ]

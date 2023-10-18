from airflow import DAG
from airflow.models import Variable
from airflow.kubernetes.secret import Secret
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 8, 30),
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
    'bsc_daily_trace_indexing',
    default_args=default_args,
    description='Run bsc indexer daily',
    schedule="10 0 * * *",
    catchup=False,
    tags=['bsc']
) as dag:

    secrets = [
        Secret(
            deploy_type='env',
            deploy_target='PROVIDER_URI',
            secret='bsc-indexer-secret',
            key='provider-uri'
        ),
        Secret(
            deploy_type='env',
            deploy_target='OUTPUT_DIR',
            secret='bsc-indexer-secret',
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

    bsc_trace = Variable.get('bsc_trace_table_name')

    env_vars = [
        k8s.V1EnvVar(
            name='START',
            value="{{ data_interval_start | ds }}"),
        k8s.V1EnvVar(
            name='END',
            value="{{ data_interval_start | ds }}"),
        k8s.V1EnvVar(
            name='PARTITION_TO_HOUR',
            value='false'),
        k8s.V1EnvVar(
            name='ENTITY_TYPES',
            value='trace')
    ]

    KubernetesPodOperator(
        image='octanlabs/ethereumetl:0.0.13',
        arguments=['export_all'],
        env_vars=env_vars,
        secrets=secrets,
        container_resources=k8s.V1ResourceRequirements(),
        name='bsc_trace_index',
        task_id='bsc_trace_index',
        random_name_suffix=True,
    ) >> ClickHouseOperator(
        task_id='optimize_bsc_trace',
        database='default',
        sql=(f'OPTIMIZE TABLE {bsc_trace} FINAL DEDUPLICATE SETTINGS alter_sync = 0, optimize_skip_merged_partitions = 1'),
        clickhouse_conn_id="clickhouse_conn"
    )

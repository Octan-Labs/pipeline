from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
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
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['bsc']
) as dag:

    default_bsc_indexer_secrets = [
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
        )
    ]

    indexer_aws_secrets = [
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

    for hour in range(24):
        env_vars = [
            k8s.V1EnvVar(
                name='START',
                value="{{ data_interval_start.subtract(days=1) | ds }}"),
            k8s.V1EnvVar(
                name='END',
                value="{{ data_interval_start.subtract(days=1) | ds }}"),
            k8s.V1EnvVar(
                name='JOB_COMPLETION_INDEX',
                value=str(hour)),
            k8s.V1EnvVar(
                name='ENTITY_TYPES',
                value='trace, contract, token')
        ]

        KubernetesPodOperator(
            image='octanlabs/ethereumetl:0.0.5',
            arguments=['export_all'],
            env_vars=env_vars,
            secrets=default_bsc_indexer_secrets + indexer_aws_secrets,
            name='bsc_indexer_{}'.format(hour),
            task_id='bsc_indexer_{}'.format(hour)
        )
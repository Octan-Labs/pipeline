from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
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
    ]

    env_vars = [
        k8s.V1EnvVar(
            name='CALCULATE_DATE',
            value="{{ data_interval_start.subtract(days=1) | ds }}"),
    ]

    KubernetesPodOperator(
        image='octanlabs/l2s-indexer:0.0.1',
        env_vars=env_vars,
        secrets=secrets,
        container_resources=k8s.V1ResourceRequirements(),
        name='l2s_index',
        task_id='l2s_index',
        random_name_suffix=True,
    )

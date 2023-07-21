from airflow import DAG, macros
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2015, 7, 30),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('eth_daily_indexing',
          default_args=default_args,
          description='Run eth indexer daily',
          schedule="@daily",
          catchup=False)

env_vars = [
    k8s.V1EnvVar(name='START', value="{{ macros.ds_add(data_interval_start, -1) | ds }}"),
    k8s.V1EnvVar(name='END', value="{{ macros.ds_add(data_interval_start, -1) | ds }}"),
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
            dag=dag,
        )

eth_daily_indexing_cronjob
from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 8, 29),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('bsc_daily_indexing',
          default_args=default_args,
          description='Run bsc indexer daily',
          schedule="@daily",
          catchup=False)


yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
env_vars = [
    k8s.V1EnvVar(name='START', value=yesterday),
    k8s.V1EnvVar(name='END', value=yesterday),
    k8s.V1EnvVar(name='PARTITION_TO_HOUR', value='false'), 
    k8s.V1EnvVar(name='ENTITY_TYPES', value='block, transaction, log, token_transfer, trace, contract, token')
]
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

bsc_daily_indexing_cronjob = KubernetesPodOperator(
            image='octanlabs/ethereumetl:0.0.3',
            arguments=['export_all'],
            env_vars=env_vars,
            secrets=secrets,
            # container_resources=k8s.V1ResourceRequirements(
            #   request={"memory": "4Gi"},
            # ),
            name='bsc_indexer',
            task_id='bsc_indexer',
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )

bsc_daily_indexing_cronjob
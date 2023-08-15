from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 20),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('top_cmc_weekly_indexing',
          default_args=default_args,
          description='Run top cmc indexer weekly',
          schedule="@weekly",
          catchup=False)

env_vars = [
    k8s.V1EnvVar(name='S3_REGION', value='ap-southeast-1')
]
secrets = [
    Secret(
        deploy_type='env',
        deploy_target='AWS_S3_BUCKET_TOP_CMC',
        secret='cmc-indexer-secret',
        key='bucket_top_cmc'
    ),
    Secret(
        deploy_type='env',
        deploy_target='CMC_API_KEY',
        secret='cmc-indexer-secret',
        key='cmc_api_key'
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

top_cmc_weekly_indexing_cronjob = KubernetesPodOperator(
            image='octanlabs/top-cmc-crawl:latest',
            cmds=['npm', "run", "top_cmc"],
            env_vars=env_vars,
            secrets=secrets,
            name='top_cmc_indexer',
            task_id='top_cmc_indexer',
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )

top_cmc_weekly_indexing_cronjob
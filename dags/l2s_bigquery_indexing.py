from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
    'l2s_bigquery_indexing',
    default_args=default_args,
    description='Run l2s bigquery indexer daily',
    schedule="10 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=['l2s_bigquery']
) as dag:

    secrets = [
        Secret(
            deploy_type='env',
            deploy_target='GOOGLE_CLOUD_PROJECT',
            secret='l2s-bigquery-indexer-secret',
            key='project-id'
        ),
        Secret(
            deploy_type='env',
            deploy_target='GOOGLE_APPLICATION_CREDENTIALS',
            secret='l2s-bigquery-indexer-secret',
            key='credentials-path'
        ),
        Secret(
            deploy_type='env',
            deploy_target='POSTGRES_URL',
            secret='l2s-bigquery-indexer-secret',
            key='postgres-url'
        ),
        Secret(
            deploy_type='env',
            deploy_target='BEGIN_DATE',
            secret='l2s-bigquery-indexer-secret',
            key='begin-date'
        ),
        Secret(
            deploy_type='env',
            deploy_target='END_DATE',
            secret='l2s-bigquery-indexer-secret',
            key='end-date'
        ),
    ]

    l2s_bigquery_indexing = KubernetesPodOperator(
        image='octanlabs/l2s-bigquery-indexer:0.0.1',
        secrets=secrets,
        container_resources=k8s.V1ResourceRequirements(),
        name='l2s_index',
        task_id='l2s_index',
        random_name_suffix=True,
    )
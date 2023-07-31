from airflow.decorators import dag, task
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

@dag(
    'bsc_daily_indexing',
    description='Run bsc indexer daily',
    schedule="@daily",
    catchup=True,
    max_active_runs=5,
    concurrency=5,
    start_date=datetime(2020, 8, 29),
    retry_delay=timedelta(minutes=30),
    owner='airflow',
    depends_on_past=True,
    start_date=datetime(2020, 8, 29),
    retries=1,
    retry_delay=timedelta(minutes=30),
    depends_on_past=False,
    namespace='spark',
    in_cluster=True,  # if set to true, will look in the cluster, if false, looks for file
    get_logs=True,
    is_delete_operator_pod=True
)
def bsc_daily_indexing_cronjob_fun():
    @task()
    def index_all_by_hour(hour):
        env_vars = [
            k8s.V1EnvVar(name='START', value="{{ data_interval_start.subtract(days=1) | ds }}"),
            k8s.V1EnvVar(name='END', value="{{ data_interval_start.subtract(days=1) | ds }}"),
            k8s.V1EnvVar(name='JOB_COMPLETION_INDEX', value=str(hour)),
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
            image='octanlabs/ethereumetl:0.0.4',
            arguments=['export_all'],
            env_vars=env_vars,
            secrets=secrets,
            container_resources=k8s.V1ResourceRequirements(
                requests={
                    'memory': '16G',
                },
            ),
            name='bsc_indexer_hourly_{}'.format(hour),
            task_id='bsc_indexer_hourly_{}'.format(hour),
            retries=5,
            retry_delay=timedelta(minutes=5),
        )

    for hour in range(24):
        index_all_by_hour(hour)

bsc_daily_indexing_cronjob_fun()
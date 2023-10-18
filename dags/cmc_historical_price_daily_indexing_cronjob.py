from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.models import Variable

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('cmc_historical_price_daily_indexing',
          default_args=default_args,
          description='Run cmc historical price daily indexer daily',
          schedule="10 0 * * *",
          max_active_runs=1,
          concurrency=1,
          tags=['cmc'],
          catchup=False)

env_vars = [
    k8s.V1EnvVar(name='START_DATE',
                 value="{{ data_interval_start | ds }}"),
    k8s.V1EnvVar(name='END_DATE',
                 value="{{ data_interval_start | ds }}"),
    k8s.V1EnvVar(name='S3_REGION', value='ap-southeast-1')
]
secrets = [
    Secret(
        deploy_type='env',
        deploy_target='AWS_S3_BUCKET_HISTORICAL',
        secret='cmc-indexer-secret',
        key='bucket_historical'
    ),
    Secret(
        deploy_type='env',
        deploy_target='AWS_S3_BUCKET_TOP_CMC',
        secret='cmc-indexer-secret',
        key='bucket_top_cmc'
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

cmc_historical_price_daily_indexing_cronjob = KubernetesPodOperator(
    image='octanlabs/cmc-historical-crawl:latest',
    cmds=['npm', "run", "historical"],
    env_vars=env_vars,
    secrets=secrets,
    name='cmc_daily_historical_price_indexer',
    task_id='cmc_daily_historical_price_indexer',
    retries=5,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

base_s3_url = Variable.get("cmc_s3_url")

access_key = Variable.get("s3_access_secret_key")
secret_key = Variable.get("s3_secret_key")

import_from_s3_to_clickhouse = ClickHouseOperator(
    task_id='import_from_s3_to_clickhouse',
    database='default',
    sql=(
        '''
            INSERT INTO cmc_historical
            SELECT *
            FROM
            s3(
            '{base_s3_url}/cmc_historicals/start={date}_end={date}.csv',
            '{access_key}', 
            '{secret_key}', 
            'CSV'
            )
            SETTINGS async_insert=1, wait_for_async_insert=1,
            max_threads=4, max_insert_threads=4, input_format_parallel_parsing=0;
        '''.format(
            base_s3_url=base_s3_url,
            access_key=access_key,
            secret_key=secret_key,
            date="{{ data_interval_start | ds }}"
        )
    ),
    clickhouse_conn_id="clickhouse_conn",
    dag=dag,
)

cmc_historical_price_daily_indexing_cronjob >> import_from_s3_to_clickhouse

from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 7, 31),
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
    "eth_daily_trace_indexing",
    default_args=default_args,
    description='Run eth indexer daily',
    schedule="@daily",
    catchup=False,
    max_active_runs=4,
    tags=['eth']
) as dag:

    env_vars = [
        k8s.V1EnvVar(
            name='START', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(
            name='END', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(name='PARTITION_TO_HOUR', value='false'),
        k8s.V1EnvVar(name='ENTITY_TYPES', value='trace, contract, token'),
        k8s.V1EnvVar(name='MAX_WORKERS', value='1'),
        k8s.V1EnvVar(name='MAX_WORKERS', value='500')
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

    eth_daily_trace_index_task = KubernetesPodOperator(
        image='octanlabs/ethereumetl:0.0.10',
        arguments=['export_all'],
        env_vars=env_vars,
        secrets=secrets,
        container_resources=k8s.V1ResourceRequirements(
            requests={
                'memory': '24G',
            },
        ),
        name='eth_trace_index',
        task_id='eth_trace_index',
        random_name_suffix=True,
    )

    base_s3_url = Variable.get("eth_s3_url")
    access_key = Variable.get("s3_access_secret_key")
    secret_key = Variable.get("s3_secret_key")
    import_tasks = []

    for entity_type in ['trace', 'contract', 'token']:
        import_tasks.append(ClickHouseOperator(
            task_id='import_{}_from_s3_to_clickhouse_by_date'.format(entity_type),
            database='default',
            sql=(
                '''
                    INSERT INTO {table_name}
                    SELECT *
                    FROM
                    s3(
                    '{base_s3_url}/{schema}/date%3D{date}/*.parquet',
                    '{access_key}', 
                    '{secret_key}', 
                    'Parquet'
                    )
                    SETTINGS parallel_distributed_insert_select=1, async_insert=1, wait_for_async_insert=1,
                    max_threads=4, max_insert_threads=4, input_format_parallel_parsing=0;
                '''.format(
                        table_name = "ethereum_{}".format(entity_type),
                        schema = "{}s".format(entity_type),
                        date = "{{ data_interval_start.subtract(days=1) | ds }}",
                        base_s3_url = base_s3_url, 
                        access_key = access_key, 
                        secret_key = secret_key
                    )
            ),
            clickhouse_conn_id="clickhouse_conn"
        ))

    eth_daily_trace_index_task >> import_tasks
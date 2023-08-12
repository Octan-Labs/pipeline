from airflow import DAG
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
    tags=['eth']
) as dag:

    env_vars = [
        k8s.V1EnvVar(
            name='START', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(
            name='END', value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(name='PARTITION_TO_HOUR', value='false'),
        k8s.V1EnvVar(name='ENTITY_TYPES', value='trace, contract, token')
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

    trigger_import_trace = TriggerDagRunOperator(
        task_id='trigger_import_trace',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_trace",
            "schema": "traces",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
            "base_s3_url": base_s3_url
        },
        reset_dag_run=True,
        wait_for_completion=True,
        failed_states=["false"]
    )

    trigger_import_token = TriggerDagRunOperator(
        task_id='trigger_import_token',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_token",
            "schema": "tokens",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
            "base_s3_url": base_s3_url
        },
        reset_dag_run=True,
        wait_for_completion=True,
        failed_states=["false"]
    )

    trigger_import_contract = TriggerDagRunOperator(
        task_id='trigger_import_contract',
        trigger_dag_id='import_from_s3_to_clickhouse_by_date',
        conf={
            "table_name": "ethereum_contract",
            "schema": "contracts",
            "date": "{{ data_interval_start.subtract(days=1) | ds }}",
            "base_s3_url": base_s3_url
        },
        reset_dag_run=True,
        wait_for_completion=True,
        failed_states=["false"]
    )

    eth_daily_trace_index_task >> [trigger_import_trace, trigger_import_token, trigger_import_contract]
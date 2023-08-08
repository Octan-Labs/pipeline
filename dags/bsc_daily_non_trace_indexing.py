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
    'bsc_daily_non_trace_indexing',
    default_args=default_args,
    description='Run bsc indexer daily',
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    concurrency=3,
    tags=['bsc']
) as dag:

    default_bsc_indexer_secrets = [
        Secret(
            deploy_type='env',
            deploy_target='PROVIDER_URI',
            secret='bsc-indexer-secret',
            key='nontrace-provider-uri'
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

    spark_aws_secrets = [
        Secret(
            deploy_type='env',
            deploy_target='AWS_ACCESS_KEY_ID',
            secret='spark-aws-key',
            key='aws_access_key_id'
        ),
        Secret(
            deploy_type='env',
            deploy_target='AWS_SECRET_ACCESS_KEY',
            secret='spark-aws-key',
            key='aws_secret_access_key'
        ),
    ]

    default_merge_task_secrets = [
        Secret(
            deploy_type='env',
            deploy_target='BASE_PATH',
            secret='bsc-indexer-secret',
            key='output-dir'
        ),
    ]

    bsc_daily_non_trace_indexing_tasks = []

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
                value='block, transaction, log, token_transfer')
        ]

        bsc_daily_non_trace_indexing_tasks.append(KubernetesPodOperator(
            image='octanlabs/ethereumetl:0.0.4',
            arguments=['export_all'],
            env_vars=env_vars,
            secrets=default_bsc_indexer_secrets + indexer_aws_secrets,
            container_resources=k8s.V1ResourceRequirements(
                requests={
                    'memory': '8G',
                },
            ),
            name='bsc_indexer_{}'.format(hour),
            task_id='bsc_indexer_{}'.format(hour)
        ))

    env_vars = [
        k8s.V1EnvVar(
            name='DATE',
            value="{{ data_interval_start.subtract(days=1) | ds }}"),
        k8s.V1EnvVar(
            name='ENTITIES',
            value='block, transaction, log, token_transfer')
    ]

    merge_objects = KubernetesPodOperator(
        image="171092530978.dkr.ecr.ap-southeast-1.amazonaws.com/octan/sparkonk8s:0.0.19",
        cmds=[
            "/usr/bin/tini",
              "-s",
              "--",
              "/opt/spark/bin/spark-submit",
              "--conf",
              "spark.eventLog.dir=s3a://datateam-spark/logs",
              "--conf",
              "spark.eventLog.enabled=true",
              "--conf",
              "spark.history.fs.inProgressOptimization.enabled=true",
              "--conf",
              "spark.history.fs.update.interval=5s",
              "--conf",
              "spark.kubernetes.container.image=171092530978.dkr.ecr.ap-southeast-1.amazonaws.com/octan/sparkonk8s:0.0.19",
              "--conf",
              "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
              "--conf",
              "spark.kubernetes.driver.podTemplateFile=s3a://datateam-spark/templates/driver_pod_template.yml",
              "--conf",
              "spark.kubernetes.executor.podTemplateFile=s3a://datateam-spark/templates/executor_pod_template.yml",
              "--conf",
              "spark.dynamicAllocation.enabled=true",
              "--conf",
              "spark.dynamicAllocation.shuffleTracking.enabled=true",
              "--conf",
              "spark.dynamicAllocation.maxExecutors=10",
              "--conf",
              "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=30",
              "--conf",
              "spark.dynamicAllocation.executorIdleTimeout=60s",
              "--conf",
              "spark.driver.memory=4g",
              "--conf",
              "spark.kubernetes.driver.request.cores=2",
              "--conf",
              "spark.kubernetes.driver.limit.cores=4",
              "--conf",
              "spark.executor.memory=8g",
              "--conf",
              "spark.kubernetes.executor.request.cores=2",
              "--conf",
              "spark.kubernetes.executor.limit.cores=4",
              "--conf",
              "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
              "--conf",
              "fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
              "--conf",
              "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
              "--conf",
              "spark.hadoop.fs.s3a.fast.upload=true",
              "--conf",
              "spark.serializer=org.apache.spark.serializer.KryoSerializer",
              "--conf",
              "spark.sql.sources.ignoreDataLocality.enabled=true",
              "--conf",
              "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
              "s3a://datateam-spark/jobs/merge_hour_partition.py",
        ],
        name=f"merge_hour_partition_objects",
        task_id=f"merge_hour_partition_objects",
        secrets=spark_aws_secrets + default_merge_task_secrets,
        env_vars=env_vars,
    )

    rename_merged_object = KubernetesPodOperator(
        image='tuannm106/merge-hour-partition:latest',
        env_vars=env_vars,
        secrets=indexer_aws_secrets + default_merge_task_secrets,
        name='rename_merged_object',
        task_id='rename_merged_object',
    )

    bsc_daily_non_trace_indexing_tasks >> merge_objects >> rename_merged_object

from datetime import datetime, timedelta
from airflow import DAG
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('merge_hour_parition',
          default_args=default_args,
          schedule_interval=None)

secrets = [
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

spark_secrets = [
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

merge_files = KubernetesPodOperator(
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
              "-b={{ dag_run.conf['base_path'] }}",
              "-d={{ dag_run.conf['date'] }}",
              "-e={{ dag_run.conf['entities'] }}",
            ],
            name=f"merge_hour_partition_files",
            task_id=f"merge_hour_partition_files",
            secrets=spark_secrets,
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )

env_vars = [
    k8s.V1EnvVar(name='BASE_PATH', value="{{ dag_run.conf['base_path'] }}"),
    k8s.V1EnvVar(name='DATE', value="{{ dag_run.conf['date'] }}"),
    k8s.V1EnvVar(name='ENTITIES', value="{{ dag_run.conf['entities'] }}")
]

rename_merged_object = KubernetesPodOperator(
            image='tuannm106/merge-hour-partition:latest',
            cmds=["main"],
            env_vars=env_vars,
            secrets=secrets,
            name='rename_merged_object',
            task_id='rename_merged_object',
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )

merge_files >> rename_merged_object
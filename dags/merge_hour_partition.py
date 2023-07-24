from datetime import datetime, timedelta
import boto3
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret

class EntityType:
    BLOCK = 'block'
    TRANSACTION = 'transaction'
    LOG = 'log'
    TOKEN_TRANSFER = 'token_transfer'
    TRACE = 'trace'
    CONTRACT = 'contract'
    TOKEN = 'token'


def execute(bucket_name, bucket, entity, date, sub_path):
    block_numbers = []
    for obj in bucket.objects.filter(Prefix="{sub_path}{entity}s/date={date}/hour".format(entity=entity, date=date, sub_path=sub_path)):
        block_numbers.append(re.findall(r'\d+', obj.key.split("/")[-1]))
        obj.delete()

    print(block_numbers)

    start_block = min((min(block_number)
                      for block_number in block_numbers), default="")
    end_block = max((max(block_number)
                    for block_number in block_numbers), default="")
    file_name = "{entity}s_{start_block}_{end_block}.parquet".format(
        entity=entity, start_block=start_block, end_block=end_block, sub_path=sub_path)

    for obj in bucket.objects.filter(Prefix="{sub_path}{entity}s/date={date}/repartition".format(entity=entity, date=date, sub_path=sub_path)):
        if ("/part" not in obj.key):
            obj.delete()
        else:
            print(obj.key)
            s3.Object(bucket_name, '{sub_path}{entity}s/date={date}/{file_name}'.format(entity=entity, file_name=file_name,
                      date=date, sub_path=sub_path)).copy_from(CopySource='{bucket_name}/{key}'.format(bucket_name=bucket_name, key=obj.key))
            s3.Object(bucket_name, obj.key).delete()


def main(**kwargs):
    conf = kwargs['dag_run'].conf
    path = conf['base_path'].split("/")
    bucket_name = path[0]
    sub_path = "/".join(path[1:]) + "/" if len(path) > 1 else ""
    entities = conf['entities']
    date = conf['date']
    
    aws_access_key_id=Secret(deploy_type='env',
                             deploy_target='AWS_ACCESS_KEY_ID',
                             secret='indexer-aws-key',
                             key='aws_access_key_id')
    aws_secret_access_key=Secret(deploy_type='env',
                                 deploy_target='AWS_SECRET_ACCESS_KEY',
                                 secret='indexer-aws-key',
                                 key='aws_secret_access_key')
    
    # Create connection to S3
    global s3
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Get bucket object
    bucket = s3.Bucket(bucket_name)

    if EntityType.BLOCK in entities:
        execute(bucket_name, bucket, EntityType.BLOCK, date, sub_path)

    if EntityType.TRANSACTION in entities:
        execute(bucket_name, bucket, EntityType.TRANSACTION, date, sub_path)

    if EntityType.LOG in entities:
        execute(bucket_name, bucket, EntityType.LOG, date, sub_path)

    if EntityType.TOKEN_TRANSFER in entities:
        execute(bucket_name, bucket, EntityType.TOKEN_TRANSFER, date, sub_path)

    if EntityType.TRACE in entities:
        execute(bucket_name, bucket, EntityType.TRACE, date, sub_path)

    if EntityType.CONTRACT in entities:
        execute(bucket_name, bucket, EntityType.CONTRACT, date, sub_path)

    if EntityType.TOKEN in entities:
        execute(bucket_name, bucket, EntityType.TOKEN, date, sub_path)

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
              "spark.kubernetes.driver.podTemplateFile=s3a://datateam-spark/driver_pod_template.yml",
              "--conf",
              "spark.kubernetes.executor.podTemplateFile=s3a://datateam-spark/executor_pod_template.yml",
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
              "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
              "--conf",
              "spark.hadoop.fs.s3a.fast.upload=true",
              "--conf",
              "spark.serializer=org.apache.spark.serializer.KryoSerializer",
              "--conf",
              "spark.sql.sources.ignoreDataLocality.enabled=true",
              "--conf",
              "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
              "s3a://datateam-spark/jobs/megre_hour_partition_files.py",
              "-b={{ dag_run.conf['base_path'] }}",
              "-d={{ dag_run.conf['date'] }}",
              "-e={{ dag_run.conf['entities'] }}",
            ],
            name=f"megre_hour_partition_files",
            task_id=f"megre_hour_partition_files",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )

rename_and_delete_file = PythonOperator(
    task_id='rename_and_delete_file',
    python_callable=main,
    dag=dag
)

merge_files >> rename_and_delete_file
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'spark',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('UAW_calculation',
          default_args=default_args,
          description='Pre tx and Volume calculation spark job',
          schedule_interval=None,
          start_date=datetime(2023, 7, 10),
          catchup=False)

UAW_calculation = KubernetesPodOperator(
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
              "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
              "--conf",
              "spark.hadoop.fs.s3a.fast.upload=true",
              "--conf",
              "spark.serializer=org.apache.spark.serializer.KryoSerializer",
              "--conf",
              "spark.sql.sources.ignoreDataLocality.enabled=true",
              "--conf",
              "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
              "--conf",
              "spark.hadoop.parquet.enable.summary-metadata=false",
              "--conf",
              "spark.sql.parquet.mergeSchema=false",
              "--conf",
              "spark.sql.parquet.filterPushdown=true",
              "--conf",
              "spark.sql.hive.metastorePartitionPruning=true",
              "s3a://datateam-spark/jobs/UAW-calculation.py",
              "-b={{ dag_run.conf['base_path'] }}",
              "-s={{ dag_run.conf['start'] }}",
              "-e={{ dag_run.conf['end'] }}"
            ],
            name=f"uaw-calculation",
            task_id=f"uaw-calculation",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )


UAW_calculation
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

dag = DAG('Pre_txns_Spark_job',
          default_args=default_args,
          description='Kubernetes Pod Operator - Demonstration Dag',
          schedule_interval=None,
          start_date=datetime(2023, 7, 10),
          catchup=False)

# env_var = [k8s.V1EnvVar(name='FOO', value='foo'), k8s.V1EnvVar(name='BAR', value='bar')]
# configmaps = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='my-configs'))]

spark_pi = KubernetesPodOperator(
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
              "s3a://datateam-spark/jobs/pre-tx.py"
            ],
            # env_vars=env_var,
            # env_from=configmaps,
            name=f"pre-txns",
            task_id=f"pre-txns",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )



spark_pi
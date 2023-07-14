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

dag = DAG('Indexer_Daily_Cronjob',
          default_args=default_args,
          description='Run indexer daily',
          schedule_interval="0 12 * * *",
          start_date=datetime(2023, 7, 10),
          catchup=False)


yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
env_vars = [k8s.V1EnvVar(name='START', value=yesterday), k8s.V1EnvVar(name='END', value=yesterday)]
secrets = [k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='indexer-secret'))]

indexer_daily_cronjob = KubernetesPodOperator(
            image="octanlabs/ethereumetl:0.0.2",
            arguments=["export_all"],
            env_vars=env_vars,
            env_from=secrets,
            name=f"indexer",
            task_id=f"indexer",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )

indexer_daily_cronjob
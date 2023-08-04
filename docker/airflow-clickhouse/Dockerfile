# Ref: https://airflow.apache.org/docs/docker-stack/build.html#example-of-adding-pypi-package

ARG AIRFLOW_VERSION=2.6.3
FROM apache/airflow:${AIRFLOW_VERSION}
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" airflow-clickhouse-plugin==0.11.0

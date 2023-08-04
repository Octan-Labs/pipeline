# Ref: https://airflow.apache.org/docs/docker-stack/build.html#example-of-adding-pypi-package

ARG AIRFLOW_VERSION=2.6.3
ARG AIRFLOW_CLICKHOUSE_PLUGIN_VERSION=0.11.0
FROM apache/airflow:${AIRFLOW_VERSION}
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "airflow-clickhouse-plugin==${AIRFLOW_CLICKHOUSE_PLUGIN_VERSION}"

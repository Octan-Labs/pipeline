# Ref: https://airflow.apache.org/docs/docker-stack/build.html#example-of-adding-pypi-package

ARG AIRFLOW_VERSION=2.6.3
FROM apache/airflow:${AIRFLOW_VERSION}

# Set environment variables with default values from ARG
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}
ARG AIRFLOW_CLICKHOUSE_PLUGIN_VERSION=0.11.0
ENV AIRFLOW_CLICKHOUSE_PLUGIN_VERSION=${AIRFLOW_CLICKHOUSE_PLUGIN_VERSION}

# Install Apache Airflow and the ClickHouse plugin
RUN pip install --no-cache-dir "apache-airflow==$AIRFLOW_VERSION" "airflow-clickhouse-plugin==$AIRFLOW_CLICKHOUSE_PLUGIN_VERSION"

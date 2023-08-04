# Ref: https://airflow.apache.org/docs/docker-stack/build.html#example-of-adding-pypi-package

FROM apache/airflow:2.6.3
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" airflow-clickhouse-plugin==0.11.0
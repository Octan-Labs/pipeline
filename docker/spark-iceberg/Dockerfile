FROM tabulario/spark-iceberg

RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -o /opt/spark/jars/postgresql-42.2.18.jar

COPY spark-defaults.conf /opt/spark/conf

ENV AWS_REGION=us-east-1
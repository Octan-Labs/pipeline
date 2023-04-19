FROM eclipse-temurin:11-jre-focal

WORKDIR /opt

ENV HADOOP_VERSION=3.2.0
ENV HIVE_METASTORE_VERSION=3.0.0

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-metastore-${HIVE_METASTORE_VERSION}-bin

ENV DATABASE_DRIVER=com.mysql.jdbc.Driver
ENV DATABASE_TYPE_JDBC=mysql
ENV DATABASE_PORT=3306

RUN \
  echo "Install OS dependencies" && \
    build_deps="curl" && \
    apt-get update -y && \
    apt-get install -y $build_deps --no-install-recommends

RUN \
  echo "Download hadoop common jars" && \
    curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf -

RUN \
  echo "Download and install the standalone metastore binary" && \
    curl -L https://downloads.apache.org/hive/hive-standalone-metastore-${HIVE_METASTORE_VERSION}/hive-standalone-metastore-${HIVE_METASTORE_VERSION}-bin.tar.gz | tar zxf -

RUN \
    echo "Download and install the database connector" && \
    curl -L https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar -o mysql-connector-java-5.1.49.jar && \
    cp mysql-connector-java-5.1.49.jar ${HIVE_HOME}/lib/ && \
    rm -rf mysql-connector-java-5.1.49

RUN \
  echo "Purge build artifacts" && \
    apt-get purge -y --auto-remove $build_deps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
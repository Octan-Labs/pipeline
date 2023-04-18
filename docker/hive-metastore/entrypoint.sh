#!/bin/bash

export HIVE_METASTORE_VERSION=${HIVE_METASTORE_VERSION:-3.0.0}
export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION:-3.2.0}
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar

generate_database_config(){
  cat << XML
<property>
  <name>javax.jdo.option.ConnectionDriverName</name>
  <value>${DATABASE_DRIVER}</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:${DATABASE_TYPE_JDBC}://${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DB}</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionUserName</name>
  <value>${DATABASE_USER}</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionPassword</name>
  <value>${DATABASE_PASSWORD}</value>
</property>
XML
}

generate_hive_site_config(){
  database_config=$(generate_database_config)
  cat << XML > "$1"
<configuration>
$database_config
</configuration>
XML
}

generate_metastore_site_config(){
  database_config=$(generate_database_config)
  cat << XML > "$1"
<configuration>
  <property>
    <name>metastore.task.threads.always</name>
    <value>org.apache.hadoop.hive.metastore.events.EventCleanerTask</value>
  </property>
  <property>
    <name>metastore.expression.proxy</name>
    <value>org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy</value>
  </property>
  $database_config
  <property>
    <name>metastore.warehouse.dir</name>
    <value>s3a://${S3_BUCKET}/${S3_PREFIX}/</value>
  </property>
  <property>
    <name>metastore.thrift.port</name>
    <value>9083</value>
  </property>
</configuration>
XML
}

generate_s3_custom_endpoint(){
  if [ -z "$S3_ENDPOINT_URL" ]; then
    echo ""
    return 0
  fi

  cat << XML
<property>
  <name>fs.s3a.endpoint</name>
  <value>${S3_ENDPOINT_URL}</value>
</property>
<property>
  <name>fs.s3a.access.key</name>
  <value>${AWS_ACCESS_KEY_ID:-}</value>
</property>
<property>
  <name>fs.s3a.secret.key</name>
  <value>${AWS_SECRET_ACCESS_KEY:-}</value>
</property>
<property>
  <name>fs.s3a.connection.ssl.enabled</name>
  <value>false</value>
</property>
<property>
  <name>fs.s3a.path.style.access</name>
  <value>true</value>
</property>
XML
}

generate_core_site_config(){
  custom_endpoint_configs=$(generate_s3_custom_endpoint)
  cat << XML > "$1"
<configuration>
  <property>
      <name>fs.defaultFS</name>
      <value>s3a://${S3_BUCKET}</value>
  </property>
  <property>
      <name>fs.s3a.impl</name>
      <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
      <name>fs.s3a.fast.upload</name>
      <value>true</value>
  </property>
  $custom_endpoint_configs
</configuration>
XML
}

# Make sure mysql is ready
MAX_TRIES=8
CURRENT_TRY=1
SLEEP_BETWEEN_TRY=4
until [ "$(telnet mysql 3306 | sed -n 2p)" = "Connected to mysql." ] || [ "$CURRENT_TRY" -gt "$MAX_TRIES" ]; do
    echo "Waiting for mysql server..."
    sleep "$SLEEP_BETWEEN_TRY"
    CURRENT_TRY=$((CURRENT_TRY + 1))
done

if [ "$CURRENT_TRY" -gt "$MAX_TRIES" ]; then
  echo "WARNING: Timeout when waiting for mysql."
fi

# 
generate_hive_site_config ${HADOOP_HOME}/etc/hadoop/hive-site.xml
${HIVE_HOME}/bin/schematool -dbType ${DATABASE_TYPE_JDBC} -info 
# Run migration
if [ $? -eq 1 ]; then
  echo "Getting schema info failed. Probably not initialized. Initializing..."
  ${HIVE_HOME}/bin/schematool -initSchema -dbType ${DATABASE_TYPE_JDBC}
fi

# configure & start metastore (in foreground)
generate_metastore_site_config ${HIVE_HOME}/conf/metastore-site.xml
generate_core_site_config ${HADOOP_HOME}/etc/hadoop/core-site.xml

echo "Starting hive metastore ..."
${HIVE_HOME}/bin/start-metastore
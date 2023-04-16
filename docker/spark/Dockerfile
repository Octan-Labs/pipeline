FROM eclipse-temurin:11-jre-focal
ARG spark_uid=185
RUN groupadd --system --gid=${spark_uid} spark && \
    useradd --system --uid=${spark_uid} --gid=spark spark
RUN set -ex && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt install -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu && \
    apt install -y python3 python3-pip && \
    mkdir -p /opt/spark && \
    mkdir /opt/spark/python && \
    mkdir -p /opt/spark/examples && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    chown -R spark:spark /opt/spark && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* && \
    rm -rf /var/lib/apt/lists/*
# Install Apache Spark
# https://downloads.apache.org/spark/KEYS
ENV SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
    SPARK_TGZ_ASC_URL=https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz.asc \
    GPG_KEY=C56349D886F2B01F8CAE794C653C2301FEA493EE
RUN set -ex; \
    export SPARK_TMP="$(mktemp -d)"; \
    cd $SPARK_TMP; \
    wget -nv -O spark.tgz "$SPARK_TGZ_URL"; \
    wget -nv -O spark.tgz.asc "$SPARK_TGZ_ASC_URL"; \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --keyserver hkps://keys.openpgp.org --recv-key "$GPG_KEY" || \
    gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys "$GPG_KEY"; \
    gpg --batch --verify spark.tgz.asc spark.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" spark.tgz.asc; \
    \
    tar -xf spark.tgz --strip-components=1; \
    chown -R spark:spark .; \
    mv jars /opt/spark/; \
    mv bin /opt/spark/; \
    mv sbin /opt/spark/; \
    mv kubernetes/dockerfiles/spark/decom.sh /opt/; \
    mv examples /opt/spark/; \
    mv kubernetes/tests /opt/spark/; \
    mv data /opt/spark/; \
    mv python/pyspark /opt/spark/python/pyspark/; \
    mv python/lib /opt/spark/python/lib/; \
    cd ..; \
    rm -rf "$SPARK_TMP";

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar; \
    mv hadoop-aws-3.3.2.jar /opt/spark/jars/;
    
# https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.2
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar; \
    mv aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/;

# use spark + iceberg version
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.2.1/iceberg-spark-runtime-3.3_2.12-1.2.1.jar; \
    mv iceberg-spark-runtime-3.3_2.12-1.2.1.jar /opt/spark/jars/;

# https://github.com/apache/iceberg/blob/apache-iceberg-1.2.1/versions.props#L24
RUN wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar; \
    mv bundle-2.20.18.jar /opt/spark/jars/;

RUN wget https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.18/url-connection-client-2.20.18.jar; \
    mv url-connection-client-2.20.18.jar /opt/spark/jars/;
 
COPY entrypoint.sh /opt/
ENV SPARK_HOME /opt/spark
WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir
RUN chmod a+x /opt/decom.sh
RUN chmod a+x /opt/entrypoint.sh
ENTRYPOINT [ "/opt/entrypoint.sh" ]

FROM apache/airflow:2.10.2-python3.10

USER root

ENV SPARK_VERSION=3.5.7

RUN apt-get update && apt-get install -y curl

RUN curl -fL -o /tmp/spark.tgz \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars/ && \
    curl --http1.1 -fL --retry 5 --retry-delay 5 -o /opt/spark/jars/hadoop-aws-3.3.1.jar \
        https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl --http1.1 -fL --retry 5 --retry-delay 5 -o /opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
        https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    curl --http1.1 -fL --retry 5 --retry-delay 5 -o /opt/spark/jars/postgresql-42.2.23.jar \
        https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

RUN mkdir -p /opt/hadoop/etc/hadoop \
    && chown -R airflow:root /opt/hadoop \
    && chmod -R 775 /opt/hadoop

COPY ./variables_and_connections /init/variables_and_connections
COPY ./init_connections.sh       /init/init_connections.sh
RUN chmod +x /init/init_connections.sh

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir apache-airflow-providers-apache-spark

ENV SPARK_CLASSPATH=/opt/spark/jars/*
ENV PATH=$SPARK_HOME/bin:$PATH

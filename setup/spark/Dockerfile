FROM spark:3.5.7-scala2.12-java11-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Download JARs into the Spark jars directory on the worker.
# Required so executors can read S3A (MinIO) and write to PostgreSQL.
RUN curl -sL -o /opt/spark/jars/hadoop-aws-3.3.1.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -sL -o /opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    curl -sL -o /opt/spark/jars/postgresql-42.2.23.jar \
        https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

USER spark
ENV SPARK_NO_DAEMONIZE=true

ENTRYPOINT ["/opt/entrypoint.sh"]

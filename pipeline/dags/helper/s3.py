import os
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook

try:
    _s3 = BaseHook.get_connection('s3-conn')
    S3_ENDPOINT_URL = _s3.extra_dejson.get('endpoint_url', os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'))
    S3_ACCESS_KEY   = _s3.login    or os.environ.get('MINIO_ACCESS_KEY', 'minio')
    S3_SECRET_KEY   = _s3.password or os.environ.get('MINIO_SECRET_KEY', 'minio123')
except AirflowNotFoundException:
    S3_ENDPOINT_URL = os.environ.get('MINIO_ENDPOINT',   'http://minio:9000')
    S3_ACCESS_KEY   = os.environ.get('MINIO_ACCESS_KEY', 'minio')
    S3_SECRET_KEY   = os.environ.get('MINIO_SECRET_KEY', 'minio123')

BUCKET = os.environ.get('BUCKET', 'transport-bucket')

JAR_LIST = [
    '/opt/spark/jars/hadoop-aws-3.3.1.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
    '/opt/spark/jars/postgresql-42.2.23.jar',
]

SPARK_CONF = {
    'spark.submit.deployMode':                    'client',
    'spark.hadoop.fs.s3a.access.key':             S3_ACCESS_KEY,
    'spark.hadoop.fs.s3a.secret.key':             S3_SECRET_KEY,
    'spark.hadoop.fs.s3a.endpoint':               S3_ENDPOINT_URL,
    'spark.hadoop.fs.s3a.path.style.access':      'true',
    'spark.hadoop.fs.s3a.impl':                   'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.hadoop.fs.s3a.connection.maximum':     '100',
    'spark.hadoop.fs.s3a.fast.upload':            'true',
    'spark.executor.memory':                      '2g',
    'spark.driver.memory':                        '1g',
    'spark.cores.max':                            '2',
    'spark.executor.cores':                       '1',
    'spark.network.timeout':                      '300s',
    'spark.executor.heartbeatInterval':           '60s',
}

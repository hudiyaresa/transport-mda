import os
import sys

if __name__ == '__main__':
    sys.path.insert(0, '/opt/airflow/dags')
    from datetime import datetime
    from pyspark.sql import SparkSession
    from helper.db import PG_URL, PG_PROPS

    DATE_STR = datetime.today().strftime('%Y-%m-%d')
    BUCKET   = os.environ.get('BUCKET', 'transport-bucket')
    OUTPUT   = f's3a://{BUCKET}/staging/raw_trips_{DATE_STR}.parquet'

    spark = SparkSession.builder.appName('GIS_ExtractData').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.jdbc(
        url=PG_URL,
        table='(SELECT * FROM public.raw_trips WHERE DATE(created_at) = CURRENT_DATE) AS t',
        properties=PG_PROPS,
    )
    print(f'rows: {df.count():,}')
    df.write.mode('overwrite').parquet(OUTPUT)
    spark.stop()

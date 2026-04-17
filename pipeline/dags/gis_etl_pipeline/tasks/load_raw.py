import os

if __name__ == '__main__':
    from datetime import datetime
    from pyspark.sql import SparkSession

    DATE_STR = datetime.today().strftime('%Y-%m-%d')
    BUCKET   = os.environ.get('BUCKET', 'transport-bucket')
    INPUT    = f's3a://{BUCKET}/staging/trips_enriched_{DATE_STR}.parquet'
    OUTPUT   = f's3a://{BUCKET}/raw/parquet/trips_{DATE_STR}.parquet'

    spark = SparkSession.builder.appName('GIS_LoadRaw').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.parquet(INPUT)
    df.write.mode('overwrite').parquet(OUTPUT)
    print(f'saved {df.count():,} rows to {OUTPUT}')
    spark.stop()

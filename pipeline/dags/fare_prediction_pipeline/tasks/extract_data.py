import os

if __name__ == '__main__':
    import sys
    sys.path.insert(0, '/opt/airflow/dags')
    from pyspark.sql import SparkSession
    from helper.db import PG_URL, PG_PROPS

    BUCKET = os.environ.get('BUCKET', 'transport-bucket')
    OUTPUT = f's3a://{BUCKET}/ml/raw_training_data.parquet'

    spark = SparkSession.builder.appName('FarePred_ExtractData').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.jdbc(url=PG_URL, properties=PG_PROPS, table="""(
        SELECT trip_id, origin_lat, origin_lng, dest_lat, dest_lng,
               distance_km, vehicle_type, surge_multiplier, weather,
               hour_of_day, day_of_week, origin_zone, dest_zone, actual_fare
        FROM public.trips_enriched
        WHERE processed_at >= NOW() - INTERVAL '90 days'
          AND actual_fare IS NOT NULL AND actual_fare > 0
    ) AS t""")

    print(f'training samples: {df.count():,}')
    df.write.mode('overwrite').parquet(OUTPUT)
    spark.stop()

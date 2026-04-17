import os

if __name__ == '__main__':
    import sys
    sys.path.insert(0, '/opt/airflow/dags')
    from pyspark.ml import PipelineModel
    from pyspark.ml.regression import GBTRegressionModel
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, exp
    from helper.db import PG_URL, PG_PROPS

    BUCKET     = os.environ.get('BUCKET', 'transport-bucket')
    PIPE_PATH  = f's3a://{BUCKET}/ml/pipeline_model'
    MODEL_PATH = f's3a://{BUCKET}/ml/model/fare_model_new'

    spark = SparkSession.builder.appName('FarePred_BatchScore').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.jdbc(url=PG_URL, properties=PG_PROPS, table="""(
        SELECT te.* FROM public.trips_enriched te
        LEFT JOIN public.trip_predictions tp USING (trip_id)
        WHERE tp.trip_id IS NULL LIMIT 50000
    ) AS unscored""")

    n = df.count()
    print(f'unscored trips: {n:,}')
    if n == 0:
        spark.stop()
        exit(0)

    result = (GBTRegressionModel.load(MODEL_PATH)
              .transform(PipelineModel.load(PIPE_PATH).transform(df))
              .withColumn('predicted_fare', exp(col('prediction')) - 1)
              .select('trip_id', 'origin_lat', 'origin_lng', 'dest_lat', 'dest_lng',
                      'distance_km', 'vehicle_type', 'surge_multiplier', 'weather',
                      'hour_of_day', 'day_of_week', 'origin_zone', 'predicted_fare', 'actual_fare'))

    result.write.mode('append').jdbc(url=PG_URL, table='public.trip_predictions', properties=PG_PROPS)
    print(f'predictions written: {result.count():,}')
    spark.stop()

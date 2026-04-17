import os

if __name__ == '__main__':
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, log1p

    BUCKET    = os.environ.get('BUCKET', 'transport-bucket')
    INPUT     = f's3a://{BUCKET}/ml/raw_training_data.parquet'
    OUT_TRAIN = f's3a://{BUCKET}/ml/preprocessed/train.parquet'
    OUT_TEST  = f's3a://{BUCKET}/ml/preprocessed/test.parquet'
    OUT_PIPE  = f's3a://{BUCKET}/ml/pipeline_model'

    CAT_COLS = ['vehicle_type', 'weather', 'origin_zone', 'dest_zone']
    NUM_COLS = ['distance_km', 'surge_multiplier', 'hour_of_day', 'day_of_week',
                'origin_lat', 'origin_lng', 'dest_lat', 'dest_lng']

    spark = SparkSession.builder.appName('FarePred_Preprocess').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.parquet(INPUT).withColumn('label', log1p(col('actual_fare')))

    indexers  = [StringIndexer(inputCol=c, outputCol=f'{c}_idx', handleInvalid='keep') for c in CAT_COLS]
    assembler = VectorAssembler(inputCols=NUM_COLS + [f'{c}_idx' for c in CAT_COLS],
                                outputCol='features_raw', handleInvalid='skip')
    scaler    = StandardScaler(inputCol='features_raw', outputCol='features', withMean=True, withStd=True)

    fitted = Pipeline(stages=[*indexers, assembler, scaler]).fit(df)
    df_t   = fitted.transform(df).select('features', 'label', 'trip_id')

    train_df, test_df = df_t.randomSplit([0.8, 0.2], seed=42)
    print(f'train: {train_df.count():,} | test: {test_df.count():,}')

    train_df.write.mode('overwrite').parquet(OUT_TRAIN)
    test_df.write.mode('overwrite').parquet(OUT_TEST)
    fitted.write().overwrite().save(OUT_PIPE)
    spark.stop()

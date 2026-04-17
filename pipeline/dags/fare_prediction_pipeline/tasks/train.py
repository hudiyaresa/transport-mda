import os

if __name__ == '__main__':
    from pyspark.ml.regression import GBTRegressor
    from pyspark.sql import SparkSession

    BUCKET     = os.environ.get('BUCKET', 'transport-bucket')
    TRAIN_PATH = f's3a://{BUCKET}/ml/preprocessed/train.parquet'
    MODEL_PATH = f's3a://{BUCKET}/ml/model/fare_model_new'

    spark = SparkSession.builder.appName('FarePred_Train').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    model = GBTRegressor(featuresCol='features', labelCol='label',
                         maxIter=100, maxDepth=5, stepSize=0.1,
                         subsamplingRate=0.8, seed=42).fit(spark.read.parquet(TRAIN_PATH))
    model.write().overwrite().save(MODEL_PATH)
    print(f'model saved to {MODEL_PATH}')
    spark.stop()

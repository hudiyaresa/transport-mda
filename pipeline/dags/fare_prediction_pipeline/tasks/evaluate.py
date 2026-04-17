import os

if __name__ == '__main__':
    import math
    import sys
    sys.path.insert(0, '/opt/airflow/dags')
    
    from datetime import datetime
    from pyspark.ml.regression import GBTRegressionModel
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, exp, abs
    from helper.db import PG_URL, PG_PROPS

    BUCKET     = os.environ.get('BUCKET', 'transport-bucket')
    TEST_PATH  = f's3a://{BUCKET}/ml/preprocessed/test.parquet'
    MODEL_PATH = f's3a://{BUCKET}/ml/model/fare_model_new'

    spark = SparkSession.builder.appName('FarePred_Evaluate').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    preds = (GBTRegressionModel.load(MODEL_PATH)
             .transform(spark.read.parquet(TEST_PATH))
             .withColumn('pred_fare',   exp(col('prediction')) - 1)
             .withColumn('actual_fare', exp(col('label')) - 1))

    n      = preds.count()
    mse    = preds.withColumn('sq', (col('pred_fare') - col('actual_fare')) ** 2).agg({'sq': 'mean'}).collect()[0][0]
    mae = preds.withColumn('ab', abs(col('pred_fare') - col('actual_fare'))) \
           .agg({'ab': 'mean'}).collect()[0][0]
    rmse   = math.sqrt(mse)
    mean_a = preds.agg({'actual_fare': 'mean'}).collect()[0][0]
    ss_res = preds.withColumn('sq', (col('pred_fare') - col('actual_fare')) ** 2).agg({'sq': 'sum'}).collect()[0][0]
    ss_tot = preds.withColumn('sq', (col('actual_fare') - mean_a) ** 2).agg({'sq': 'sum'}).collect()[0][0]
    r2     = 1 - (ss_res / ss_tot) if ss_tot else 0.0

    version = f"gbt_fare_{datetime.today().strftime('%Y%m%d')}"
    print(f'version={version} n={n:,} RMSE=Rp{rmse:,.0f} MAE=Rp{mae:,.0f} R2={r2:.4f}')

    spark.createDataFrame([{'model_version': version, 'rmse': rmse, 'mae': mae,
                            'r2': r2, 'num_samples': n, 'notes': 'GBT Regressor'}]) \
         .write.mode('append').jdbc(url=PG_URL, table='public.model_metrics', properties=PG_PROPS)
    spark.stop()

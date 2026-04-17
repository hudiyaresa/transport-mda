import os

if __name__ == '__main__':
    import sys
    sys.path.insert(0, '/opt/airflow/dags')

    import psycopg2
    from datetime import datetime
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, abs as spark_abs, lit, current_timestamp
    from pyspark.sql.types import StringType, TimestampType

    from helper.db import PG_URL, PG_PROPS

    # -----------------------------
    # PostgreSQL connection config
    # -----------------------------
    PG_HOST = os.environ.get('PG_HOST', 'sources')
    PG_PORT = int(os.environ.get('PG_PORT', '5432'))
    PG_DB   = os.environ.get('PG_DB', 'transport_db')
    PG_USER = os.environ.get('PG_USER', 'postgres')
    PG_PASS = os.environ.get('PG_PASSWORD', 'postgres')

    def pg_conn():
        return psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )

    # -----------------------------
    # Spark session
    # -----------------------------
    spark = SparkSession.builder.appName('AddTrainingDataset').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # -----------------------------
    # Load valid predictions
    # -----------------------------
    df = spark.read.jdbc(
        url=PG_URL,
        properties=PG_PROPS,
        table="""
        (
            SELECT *
            FROM public.trip_predictions
            WHERE actual_fare IS NOT NULL
              AND predicted_fare > 0
              AND actual_fare > 0
        ) AS preds
        """
    )

    df_valid = df.filter(
        spark_abs(col('predicted_fare') - col('actual_fare')) / col('actual_fare') <= 0.20
    )

    n = df_valid.count()
    print(f'valid samples: {n:,}')

    if n == 0:
        spark.stop()
        exit(0)

    # -----------------------------
    # Transform to match target schema
    # -----------------------------
    df_out = (
        df_valid
        .select(
            'trip_id',
            lit(None).cast(StringType()).alias('driver_id'),
            lit(None).cast(StringType()).alias('passenger_id'),
            'origin_lat', 'origin_lng', 'dest_lat', 'dest_lng',
            lit(None).cast(TimestampType()).alias('request_time'),
            'hour_of_day', 'day_of_week', 'distance_km',
            'origin_zone',
            lit(None).cast(StringType()).alias('dest_zone'),
            'vehicle_type', 'surge_multiplier', 'weather', 'actual_fare',
        )
        # ✅ correct timestamp type (NOT string)
        .withColumn('processed_at', current_timestamp())
    )

    # -----------------------------
    # Write to staging table
    # -----------------------------
    df_out.write.mode('overwrite').jdbc(
        url=PG_URL,
        table='public.staging_trips_enriched',
        properties={**PG_PROPS, 'batchsize': '1000'}
    )

    print(f'staging loaded: {n:,} rows')

    # -----------------------------
    # Upsert into target table
    # -----------------------------
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.trips_enriched (
                    trip_id, driver_id, passenger_id,
                    origin_lat, origin_lng, dest_lat, dest_lng,
                    request_time, hour_of_day, day_of_week, distance_km,
                    origin_zone, dest_zone, vehicle_type,
                    surge_multiplier, weather, actual_fare, processed_at
                )
                SELECT
                    trip_id, driver_id, passenger_id,
                    origin_lat, origin_lng, dest_lat, dest_lng,
                    request_time, hour_of_day, day_of_week, distance_km,
                    origin_zone, dest_zone, vehicle_type,
                    surge_multiplier, weather, actual_fare, processed_at
                FROM public.staging_trips_enriched
                ON CONFLICT (trip_id) DO NOTHING
            """)

            inserted = cur.rowcount

            # cleanup staging
            cur.execute("DROP TABLE IF EXISTS public.staging_trips_enriched")

        conn.commit()

    print(f'trips_enriched: {inserted:,} new rows (duplicates skipped)')

    spark.stop()
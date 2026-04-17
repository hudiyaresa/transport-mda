import os

if __name__ == '__main__':
    import sys
    import psycopg2
    from datetime import datetime, date
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, avg, sum as _sum, current_timestamp
    sys.path.insert(0, '/opt/airflow/dags')
    from helper.db import PG_URL, PG_PROPS

    DATE_STR = datetime.today().strftime('%Y-%m-%d')
    DATE_OBJ = date.today()
    BUCKET   = os.environ.get('BUCKET', 'transport-bucket')
    INPUT    = f's3a://{BUCKET}/staging/trips_enriched_{DATE_STR}.parquet'

    PG_HOST = os.environ.get('PG_HOST', 'sources')
    PG_PORT = int(os.environ.get('PG_PORT', '5432'))
    PG_DB   = os.environ.get('PG_DB',   'transport_db')
    PG_USER = os.environ.get('PG_USER', 'postgres')
    PG_PASS = os.environ.get('PG_PASSWORD', 'postgres')

    def pg_conn():
        return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                user=PG_USER, password=PG_PASS)

    spark = SparkSession.builder.appName('GIS_LoadWarehouse').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.parquet(INPUT)
    if 'processed_at' not in df.columns:
        df = df.withColumn('processed_at', current_timestamp())

    # Write to staging (no PK, plain INSERT is safe)
    df.write.mode('overwrite').jdbc(
        url=PG_URL,
        table='public.staging_trips_enriched',
        properties={**PG_PROPS, 'batchsize': '1000'},
    )
    print(f'staging loaded: {df.count():,} rows')

    # Upsert trips_enriched — DO NOTHING on duplicate trip_id (trips are immutable)
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
            cur.execute('DROP TABLE IF EXISTS public.staging_trips_enriched')
        conn.commit()
    print(f'trips_enriched: {inserted:,} new rows (duplicates skipped)')

    # Aggregate zone stats and upsert — DO UPDATE because stats refresh during the day
    stats = (
        df.groupBy('origin_zone')
          .agg(
              count('trip_id').alias('trip_count'),
              avg('actual_fare').alias('avg_fare'),
              avg('distance_km').alias('avg_distance_km'),
              avg('surge_multiplier').alias('avg_surge'),
              _sum((col('weather').isin('rain', 'heavy_rain')).cast('int')).alias('rain_trip_count'),
          )
          .withColumnRenamed('origin_zone', 'zone')
          .toPandas()
    )

    with pg_conn() as conn:
        with conn.cursor() as cur:
            for _, row in stats.iterrows():
                cur.execute("""
                    INSERT INTO public.zone_daily_stats
                        (stat_date, zone, trip_count, avg_fare, avg_distance_km, avg_surge, rain_trip_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (stat_date, zone) DO UPDATE SET
                        trip_count      = EXCLUDED.trip_count,
                        avg_fare        = EXCLUDED.avg_fare,
                        avg_distance_km = EXCLUDED.avg_distance_km,
                        avg_surge       = EXCLUDED.avg_surge,
                        rain_trip_count = EXCLUDED.rain_trip_count,
                        updated_at      = now()
                """, (DATE_OBJ, row['zone'], int(row['trip_count']),
                         float(row['avg_fare'] or 0), float(row['avg_distance_km'] or 0),
                         float(row['avg_surge'] or 0), int(row['rain_trip_count'] or 0)))
        conn.commit()
    print(f'zone_daily_stats upserted for {DATE_STR}')
    spark.stop()

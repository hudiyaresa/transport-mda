import os

if __name__ == '__main__':
    import math
    import sys
    from datetime import datetime
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, hour, dayofweek, udf
    from pyspark.sql.types import DoubleType, StringType

    DATE_STR = datetime.today().strftime('%Y-%m-%d')
    BUCKET   = os.environ.get('BUCKET', 'transport-bucket')
    INPUT    = f's3a://{BUCKET}/staging/raw_trips_{DATE_STR}.parquet'
    OUTPUT   = f's3a://{BUCKET}/staging/trips_enriched_{DATE_STR}.parquet'

    ZONES = {
        'Jakarta Pusat':   {'lat': (-6.22, -6.16), 'lng': (106.82, 106.87)},
        'Jakarta Utara':   {'lat': (-6.15, -6.07), 'lng': (106.73, 106.97)},
        'Jakarta Selatan': {'lat': (-6.37, -6.24), 'lng': (106.77, 106.91)},
        'Jakarta Barat':   {'lat': (-6.22, -6.10), 'lng': (106.68, 106.82)},
        'Jakarta Timur':   {'lat': (-6.35, -6.13), 'lng': (106.87, 107.00)},
    }

    def haversine_km(lat1, lng1, lat2, lng2):
        if any(v is None for v in [lat1, lng1, lat2, lng2]):
            return None
        R = 6371.0
        a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
             + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
             * math.sin(math.radians(lng2 - lng1) / 2) ** 2)
        return round(R * 2 * math.asin(math.sqrt(a)), 4)

    def assign_zone(lat, lng):
        if lat is None or lng is None:
            return 'Unknown'
        for zone, b in ZONES.items():
            if b['lat'][0] <= lat <= b['lat'][1] and b['lng'][0] <= lng <= b['lng'][1]:
                return zone
        return 'Lainnya'

    haversine_udf   = udf(haversine_km, DoubleType())
    assign_zone_udf = udf(assign_zone,  StringType())

    spark = SparkSession.builder.appName('GIS_EnrichGeo').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    df = (spark.read.parquet(INPUT)
          .withColumn('distance_km', haversine_udf(col('origin_lat'), col('origin_lng'), col('dest_lat'), col('dest_lng')))
          .withColumn('origin_zone', assign_zone_udf(col('origin_lat'), col('origin_lng')))
          .withColumn('dest_zone',   assign_zone_udf(col('dest_lat'),   col('dest_lng')))
          .withColumn('hour_of_day', hour(col('request_time')))
          .withColumn('day_of_week', dayofweek(col('request_time')))
          .filter(col('distance_km') > 0.3)
          .dropna(subset=['origin_lat', 'origin_lng', 'dest_lat', 'dest_lng']))

    print(f'enriched rows: {df.count():,}')
    df.write.mode('overwrite').parquet(OUTPUT)
    spark.stop()

import json, logging, os
import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'trip_events',
    bootstrap_servers=os.getenv('KAFKA_BROKERS', 'localhost:29092'),
    group_id='trip-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda b: json.loads(b.decode('utf-8')),
)
conn   = psycopg2.connect(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', 5433)),
    dbname=os.getenv('PG_DB', 'transport_db'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASS', 'postgres'),
)
cursor = conn.cursor()

SQL = """
INSERT INTO public.raw_trips (
    trip_id, driver_id, passenger_id,
    origin_lat, origin_lng, dest_lat, dest_lng,
    request_time, distance_km, vehicle_type,
    surge_multiplier, weather, actual_fare
) VALUES (
    %(trip_id)s, %(driver_id)s, %(passenger_id)s,
    %(origin_lat)s, %(origin_lng)s, %(dest_lat)s, %(dest_lng)s,
    %(request_time)s, %(distance_km)s, %(vehicle_type)s,
    %(surge_multiplier)s, %(weather)s, %(actual_fare)s
) ON CONFLICT (trip_id) DO NOTHING;
"""

n = 0
for msg in consumer:
    try:
        cursor.execute(SQL, msg.value)
        conn.commit()
        n += 1
        e = msg.value
        log.info('[%d] %s | %s→%s | %.1fkm | Rp%s',
                 n, e['trip_id'][:8], e.get('origin_zone','?'), e.get('dest_zone','?'),
                 e.get('distance_km', 0), f"{e.get('actual_fare',0):,.0f}")
    except Exception as exc:
        conn.rollback()
        log.error(exc)

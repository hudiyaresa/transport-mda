import json, math, random, time, uuid
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

BBOX  = {'lat_min': -6.37, 'lat_max': -6.07, 'lng_min': 106.68, 'lng_max': 107.00}
ZONES = {
    'Jakarta Pusat':   {'lat': (-6.22, -6.16), 'lng': (106.82, 106.87)},
    'Jakarta Utara':   {'lat': (-6.15, -6.07), 'lng': (106.73, 106.97)},
    'Jakarta Selatan': {'lat': (-6.37, -6.24), 'lng': (106.77, 106.91)},
    'Jakarta Barat':   {'lat': (-6.22, -6.10), 'lng': (106.68, 106.82)},
    'Jakarta Timur':   {'lat': (-6.35, -6.13), 'lng': (106.87, 107.00)},
}

def haversine(lat1, lng1, lat2, lng2):
    R = 6371.0
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(math.radians(lng2 - lng1) / 2) ** 2)
    return round(R * 2 * math.asin(math.sqrt(a)), 3)

def get_zone(lat, lng):
    for zone, b in ZONES.items():
        if b['lat'][0] <= lat <= b['lat'][1] and b['lng'][0] <= lng <= b['lng'][1]:
            return zone
    return 'Lainnya'

def compute_fare(dist, vtype, surge, weather):
    base  = {'motor': 5000, 'car': 10000, 'premium': 25000}
    perkm = {'motor': 2500, 'car':  4000, 'premium':  8000}
    wf    = {'sunny': 1.0,  'rain': 1.15, 'heavy_rain': 1.3}
    return round((base[vtype] + perkm[vtype] * dist) * surge * wf[weather] * random.gauss(1.0, 0.05) / 500) * 500

def generate():
    olat = random.uniform(BBOX['lat_min'], BBOX['lat_max'])
    olng = random.uniform(BBOX['lng_min'], BBOX['lng_max'])
    dlat = random.uniform(BBOX['lat_min'], BBOX['lat_max'])
    dlng = random.uniform(BBOX['lng_min'], BBOX['lng_max'])
    dist = haversine(olat, olng, dlat, dlng)
    if dist < 0.5:
        return None
    vtype   = random.choice(['motor', 'car', 'premium'])
    weather = random.choice(['sunny', 'sunny', 'sunny', 'rain', 'rain', 'heavy_rain'])
    surge   = random.choice([1.0, 1.0, 1.0, 1.2, 1.5, 2.0])
    now     = datetime.utcnow()
    return {
        'trip_id': str(uuid.uuid4()),
        'driver_id': f'D{random.randint(1000,9999):04d}',
        'passenger_id': f'P{random.randint(1000,9999):04d}',
        'origin_lat': round(olat, 6), 'origin_lng': round(olng, 6),
        'dest_lat':   round(dlat, 6), 'dest_lng':   round(dlng, 6),
        'origin_zone': get_zone(olat, olng), 'dest_zone': get_zone(dlat, dlng),
        'request_time': now.isoformat(), 'hour_of_day': now.hour, 'day_of_week': now.weekday(),
        'distance_km': dist, 'vehicle_type': vtype,
        'surge_multiplier': surge, 'weather': weather,
        'actual_fare': compute_fare(dist, vtype, surge, weather),
    }

sent = 0
while True:
    event = generate()
    if not event:
        continue
    producer.send('trip_events', value=event)
    sent += 1
    print(f'[{sent:>5}] {event["trip_id"][:8]} | {event["origin_zone"]} → {event["dest_zone"]} '
          f'| {event["distance_km"]:.1f}km | Rp{event["actual_fare"]:,.0f}')
    time.sleep(2)

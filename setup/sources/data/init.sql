-- GIS Transport MDA — Database Schema

CREATE TABLE IF NOT EXISTS public.raw_trips (
    trip_id            VARCHAR(50) PRIMARY KEY,
    driver_id          VARCHAR(50),
    passenger_id       VARCHAR(50),
    origin_lat         DOUBLE PRECISION,
    origin_lng         DOUBLE PRECISION,
    dest_lat           DOUBLE PRECISION,
    dest_lng           DOUBLE PRECISION,
    request_time       TIMESTAMP,
    distance_km        DOUBLE PRECISION,
    vehicle_type       VARCHAR(20),
    surge_multiplier   DOUBLE PRECISION,
    weather            VARCHAR(20),
    actual_fare        DOUBLE PRECISION,
    created_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.trips_enriched (
    trip_id            VARCHAR(50) PRIMARY KEY,
    driver_id          VARCHAR(50),
    passenger_id       VARCHAR(50),
    origin_lat         DOUBLE PRECISION,
    origin_lng         DOUBLE PRECISION,
    dest_lat           DOUBLE PRECISION,
    dest_lng           DOUBLE PRECISION,
    request_time       TIMESTAMP,
    hour_of_day        INTEGER,
    day_of_week        INTEGER,
    distance_km        DOUBLE PRECISION,
    origin_zone        VARCHAR(30),
    dest_zone          VARCHAR(30),
    vehicle_type       VARCHAR(20),
    surge_multiplier   DOUBLE PRECISION,
    weather            VARCHAR(20),
    actual_fare        DOUBLE PRECISION,
    processed_at       TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.model_metrics (
    id              SERIAL PRIMARY KEY,
    model_version   VARCHAR(50),
    train_date      TIMESTAMP DEFAULT now(),
    rmse            DOUBLE PRECISION,
    mae             DOUBLE PRECISION,
    r2              DOUBLE PRECISION,
    num_samples     INTEGER,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS public.trip_predictions (
    trip_id              VARCHAR(50) PRIMARY KEY,
    origin_lat           DOUBLE PRECISION,
    origin_lng           DOUBLE PRECISION,
    dest_lat             DOUBLE PRECISION,
    dest_lng             DOUBLE PRECISION,
    distance_km          DOUBLE PRECISION,
    vehicle_type         VARCHAR(20),
    surge_multiplier     DOUBLE PRECISION,
    weather              VARCHAR(20),
    hour_of_day          INTEGER,
    day_of_week          INTEGER,
    origin_zone          VARCHAR(30),
    predicted_fare       DOUBLE PRECISION,
    actual_fare          DOUBLE PRECISION,
    predicted_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.zone_daily_stats (
    stat_date          DATE,
    zone               VARCHAR(30),
    trip_count         INTEGER,
    avg_fare           DOUBLE PRECISION,
    avg_distance_km    DOUBLE PRECISION,
    avg_surge          DOUBLE PRECISION,
    rain_trip_count    INTEGER,
    updated_at         TIMESTAMP DEFAULT now(),
    PRIMARY KEY (stat_date, zone)
);

-- Seed sample data for testing
INSERT INTO public.raw_trips (
    trip_id, driver_id, passenger_id,
    origin_lat, origin_lng, dest_lat, dest_lng,
    request_time, distance_km, vehicle_type,
    surge_multiplier, weather, actual_fare
) VALUES
  ('T001','D001','P001',-6.2088,106.8456,-6.1751,106.8272,NOW()-INTERVAL '2 hours',4.2,'motor',1.0,'sunny',18000),
  ('T002','D002','P002',-6.1751,106.8272,-6.2297,106.8295,NOW()-INTERVAL '1 hour', 3.8,'car',  1.2,'rain', 45000),
  ('T003','D003','P003',-6.2297,106.8295,-6.1588,106.8461,NOW()-INTERVAL '30 mins',6.1,'motor',1.5,'rain', 28000),
  ('T004','D004','P004',-6.1944,106.8229,-6.2615,106.7871,NOW()-INTERVAL '15 mins',8.3,'premium',1.0,'sunny',120000),
  ('T005','D005','P005',-6.2615,106.7871,-6.2088,106.8456,NOW(),                   5.5,'car',  1.0,'sunny',52000)
ON CONFLICT (trip_id) DO NOTHING;

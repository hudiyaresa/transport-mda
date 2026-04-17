# 🗺️ GIS Transport MDA — Jakarta Ride-Hailing Data Platform

A production-grade Modern Data Architecture project simulating a real-time ride-hailing analytics platform for Jakarta, built entirely with open-source tools.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Objectives](#objectives)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Environment Variables](#environment-variables)
- [DAG Overview](#dag-overview)
- [Monitoring & Logging](#monitoring--logging)
- [API Reference](#api-reference)
- [Notebooks](#notebooks)

---

## Overview

Ride-hailing platforms like GoJek and Grab generate millions of trip events daily across Jakarta's five administrative zones. Each event carries GPS coordinates, distance, vehicle type, weather conditions, surge multiplier, and fare — making it a rich dataset for both real-time streaming pipelines and machine learning.

This project simulates that production system end-to-end: a Kafka producer generates synthetic trip events, a consumer persists them to PostgreSQL, Airflow orchestrates Spark jobs that enrich the data with GIS features (Haversine distance, zone labeling), store it in MinIO, and train a Gradient Boosted Tree model to predict fares. The trained model is served via FastAPI and results are visualised in Metabase and a Jupyter spatial analysis notebook.

---

## Architecture

```
[Trip Simulator] ──Kafka──▶ [Consumer → PostgreSQL raw_trips]
                                          │
                               [Airflow: gis_etl_pipeline]
                                          │
                          ┌───────────────┼────────────────┐
                   [Spark Enrich]   [MinIO raw/]    [PostgreSQL warehouse]
                  (Haversine+Zone)  (Parquet+GeoJSON)  (trips_enriched)
                                          │
                          ┌───────────────┼────────────────┐
                   [Airflow: fare_prediction_pipeline]
                          │
               ┌──────────┼──────────────┐
          [Spark Train] [MinIO model/] [PostgreSQL model_metrics]
                                │
                        [FastAPI /predict]
                                │
                   [Airflow: extract_predictions]
                                │
                    [PostgreSQL trip_predictions]
                                │
                   [Airflow: add_training_dataset]  ──▶ feedback loop
```

> _Add architecture diagram screenshot here_

---

## Objectives

- Stream real-time trip events through Kafka into a PostgreSQL source layer
- Build a GIS enrichment pipeline that computes Haversine distances and assigns Jakarta zone labels using Spark
- Store raw GeoJSON and Parquet files in MinIO as the data lake layer
- Train a Gradient Boosted Tree regressor to predict ride fares and version models in MinIO
- Serve fare predictions via a FastAPI endpoint with Prometheus metrics
- Implement a continuous-learning feedback loop that promotes high-confidence predictions back into the training set
- Visualise spatial demand heatmaps, KDE pickup density, and fare distributions in Jupyter

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Streaming | Apache Kafka + Zookeeper |
| Orchestration | Apache Airflow 2.10 (CeleryExecutor) |
| Processing | Apache Spark 3.5 (standalone cluster) |
| Data Lake | MinIO (S3-compatible) |
| Warehouse | PostgreSQL 13 |
| ML Serving | FastAPI + PySpark |
| Monitoring | Prometheus + Grafana + StatsD |
| Notebooks | JupyterLab + GeoPandas + Folium |
| Dashboards | Metabase |

---

## Project Structure

```
gis-transport-mda-v2/
├── README.md
├── setup.sh                          # Master startup script
├── requirements.txt                  # Local Python deps
├── script/
│   ├── generate_data.py              # Kafka trip producer
│   └── consumer.py                   # Kafka → PostgreSQL consumer
├── pipeline/
│   └── dags/
│       ├── helper/
│       │   ├── s3.py                 # Spark S3 config + JAR list
│       │   └── postgres.py           # JDBC helper
│       ├── gis_etl_pipeline/         # DAG 1
│       ├── fare_prediction_pipeline/ # DAG 2
│       ├── extract_predictions/      # DAG 3
│       └── add_training_dataset/     # DAG 4
├── ml_api/
│   ├── fastapi_serving.py
│   ├── Dockerfile
│   └── docker-compose.yaml
├── notebooks/
│   └── spatial_analysis.ipynb
└── setup/
    ├── airflow/                      # Airflow compose + Dockerfile + .env
    ├── spark/                        # Spark compose + custom Dockerfile
    ├── kafka/
    ├── data-lake/                    # MinIO
    ├── sources/                      # PostgreSQL
    └── airflow-monitoring/           # Prometheus + Grafana + Jupyter
```

---

## Setup Instructions

### Prerequisites

- Docker Desktop (≥ 4.37) with at least **8 GB RAM** allocated
- Python 3.10+
- Git Bash or WSL (on Windows)

### Quick Start

```bash
git clone <your-repo-url>
cd gis-transport-mda-v2
chmod +x setup.sh
./setup.sh
```

`setup.sh` will:
1. Build and start Airflow (creates the shared `airflow-networks` Docker network)
2. Register all connections via `airflow connections add`
3. Start Prometheus, Grafana, and Jupyter
4. Start MinIO and create the required buckets
5. Start PostgreSQL and run the schema init SQL
6. Build and start the Spark standalone cluster
7. Start Kafka + Zookeeper

### Start streaming data

```bash
# Terminal 1 — produce trip events
python script/generate_data.py

# Terminal 2 — consume into PostgreSQL
python script/consumer.py
```

### Tear down

```bash
./setup.sh down
```

### Re-register connections only (without restarting)

If you restart Airflow and lose connections:

```bash
docker exec airflow-webserver bash /init/init_connections.sh
```

---

## Environment Variables

All secrets live in `.env` files — never committed to git. Copy and edit before first run.

### `setup/airflow/.env`

| Variable | Description | Default |
|----------|-------------|---------|
| `AIRFLOW_UID` | Host user ID for file permissions | `50000` |
| `AIRFLOW_FERNET_KEY` | Encryption key for connection passwords | generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_DB_URI` | Metadata DB connection string | `postgresql+psycopg2://airflow:airflow@airflow-metadata/airflow` |
| `AIRFLOW_USER` | Webserver admin username | `admin` |
| `AIRFLOW_PASSWORD` | Webserver admin password | `admin` |

### `setup/data-lake/.env`

| Variable | Description | Default |
|----------|-------------|---------|
| `MINIO_ROOT_USER` | MinIO access key | `minio` |
| `MINIO_ROOT_PASSWORD` | MinIO secret key | `minio123` |
| `LOGS_BUCKET` | Bucket for Airflow remote logs | `airflow-logs` |
| `ML_BUCKET` | Bucket for Parquet, models, staging | `transport-bucket` |

### `setup/sources/.env`

| Variable | Description | Default |
|----------|-------------|---------|
| `SRC_POSTGRES_DB` | Database name | `transport_db` |
| `SRC_POSTGRES_USER` | PostgreSQL user | `postgres` |
| `SRC_POSTGRES_PASSWORD` | PostgreSQL password | `postgres` |
| `SRC_POSTGRES_PORT` | Host port mapping | `5433` |

---

## DAG Overview

> _Add Airflow DAG graph screenshots here_

### DAG 1 — `gis_etl_pipeline`
**Schedule:** Daily at 01:00 WIB

Reads raw trip events from PostgreSQL, computes Haversine distance, assigns Jakarta zone labels (Pusat / Utara / Selatan / Barat / Timur) using bounding box logic, saves GeoJSON + Parquet to MinIO, and loads enriched data + zone aggregates into the PostgreSQL warehouse.

```
extract_data → enrich_geo → load_raw_minio → load_warehouse
```

### DAG 2 — `fare_prediction_pipeline`
**Schedule:** Weekly (Sunday 02:00 WIB)

Loads 90 days of enriched trips, engineers features (log-transform fare, encode categoricals, StandardScaler), trains a GBT Regressor, saves the model to MinIO, and writes RMSE / MAE / R² metrics to PostgreSQL.

```
extract_data → preprocess → train → evaluate
```

### DAG 3 — `extract_predictions`
**Schedule:** Every 6 hours

Batch scores trips that haven't been predicted yet using the latest fitted pipeline and GBT model, writing results to `trip_predictions`.

```
score_unscored_trips
```

### DAG 4 — `add_training_dataset`
**Schedule:** Weekly (Sunday 01:00 WIB, runs before DAG 2)

Promotes predictions with ≤ 20% relative error back to `trips_enriched` as new labelled training rows, implementing a continuous-learning feedback loop.

```
extract_load_valid_predictions
```

---

## Monitoring & Logging

> _Add Grafana dashboard screenshot here_

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:9091 | — |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| Kafka UI | http://localhost:8085 | — |
| Grafana | http://localhost:3001 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Jupyter | http://localhost:8888 | token: transport123 |
| Fare API | http://localhost:8000/docs | — |
| PostgreSQL | localhost:5433 | postgres / postgres |

### Airflow → Prometheus pipeline

Airflow emits StatsD metrics → `statsd-exporter` bridges them to Prometheus → Grafana renders dashboards. The pre-built dashboard covers:

- DAG duration by pipeline and task
- Scheduler task queue (running / starving)
- Fare API request rate and P95 latency
- Fare API error rate

### Remote logging

Airflow task logs are shipped to MinIO (`s3://airflow-logs/`) via the `s3-conn` connection, keeping the Airflow container filesystem clean.

---

## API Reference

### `POST /predict`

Predicts fare for a single trip.

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "origin_lat": -6.2088, "origin_lng": 106.8456,
    "dest_lat": -6.1751,   "dest_lng": 106.8272,
    "distance_km": 4.2,    "vehicle_type": "motor",
    "surge_multiplier": 1.0, "weather": "sunny",
    "hour_of_day": 8,      "day_of_week": 1,
    "origin_zone": "Jakarta Pusat", "dest_zone": "Jakarta Utara"
  }'
```

**Response:**
```json
{
  "predicted_fare_idr": 18500,
  "model_version": "gbt_fare_v1",
  "distance_km": 4.2,
  "processing_ms": 45.2
}
```

### `GET /health`

Returns model status and version.

### `GET /metrics`

Prometheus metrics endpoint (request count, latency histogram).

---

## Notebooks

Open `notebooks/spatial_analysis.ipynb` at http://localhost:8888.

The notebook covers:

1. Load trip data from PostgreSQL into GeoPandas
2. Interactive Folium heatmap with Jakarta zone boundaries
3. KDE pickup density map
4. Fare distribution violin plots by zone and vehicle type
5. Hourly demand heatmap (zone × hour of day)
6. Predicted vs actual fare scatter plot (model audit)
7. Top-10 busiest origin → destination zone pairs

> _Add notebook map screenshot here_

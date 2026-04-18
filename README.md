x # GIS Transport MDA — Jakarta Ride-Hailing Data Platform

A production-grade Modern Data Architecture project simulating a real-time ride-hailing analytics platform for Jakarta, built entirely with open-source tools.

---

## Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Objectives](#objectives)
* [Tech Stack](#tech-stack)
* [Project Structure](#project-structure)
* [Prerequisites](#prerequisites)
* [Setup Instructions](#setup-instructions)
* [Environment Variables](#environment-variables)
* [DAG Overview](#dag-overview)
* [Execution Order](#execution-order)
* [Service URLs](#service-urls)
* [PostgreSQL Tables](#postgresql-tables)
* [API Reference](#api-reference)
* [Notebooks](#notebooks)
* [Dashboards & Monitoring](#dashboards--monitoring)
* [Airflow Connections](#airflow-connections)

---

## Overview

Ride-hailing platforms like GoJek and Grab generate millions of trip events daily across Jakarta. Each event includes GPS coordinates, distance, vehicle type, weather, surge multiplier, and fare.

This project simulates a full production pipeline:

* Real-time streaming (Kafka)
* Data warehousing (PostgreSQL)
* Data lake (MinIO)
* Batch processing (Spark)
* Orchestration (Airflow)
* Machine learning (GBT model)
* API serving (FastAPI)
* Visualization (Metabase, Jupyter, Grafana)

---

## Objectives

* Simulate a real-world **end-to-end data platform**
* Implement **streaming + batch architecture**
* Apply **GIS enrichment (Haversine + zoning)**
* Build a **production ML pipeline**
* Enable **continuous learning feedback loop**
* Expose predictions via **REST API**
* Provide **analytics dashboards**

---

## Architecture

```
[generate_data.py] ──Kafka──▶ [consumer.py] ──▶ [PostgreSQL: raw_trips]
                                                          │
                                              [Airflow: gis_etl_pipeline] (daily)
                                                          │
                                         ┌────────────────┼───────────────────┐
                                 [Spark enrich]    [MinIO: raw/parquet/]  [PostgreSQL: trips_enriched]
                                                          │
                                              [Airflow: fare_prediction_pipeline] (weekly)
                                                          │
                                         ┌────────────────┼───────────────────┐
                                   [Spark train]   [MinIO: ml/model/]   [PostgreSQL: model_metrics]
                                                          │
                                              [Airflow: extract_predictions] (6h)
                                                          │
                                                [PostgreSQL: trip_predictions]
                                                          │
                                              [Airflow: add_training_dataset]
                                                          │
                                                   feedback loop
                                                          │
                                                   [FastAPI /predict]
```

---

## Tech Stack

| Layer         | Tool                     |
| ------------- | ------------------------ |
| Streaming     | Apache Kafka + Zookeeper |
| Orchestration | Apache Airflow           |
| Processing    | Apache Spark             |
| Data Lake     | MinIO                    |
| Warehouse     | PostgreSQL               |
| ML Serving    | FastAPI                  |
| Monitoring    | Prometheus + Grafana     |
| Notebooks     | JupyterLab               |
| Dashboards    | Metabase                 |

---

## Project Structure

```
gis-transport-mda-v2/
├── script/
├── pipeline/dags/
├── ml_api/
├── notebooks/
├── setup/
└── setup.sh
```

---

## Prerequisites

* Docker Desktop ≥ 4.37 (**8GB RAM minimum**)
* Python 3.10+
* Bash environment (WSL / Linux / Mac)

---

## Setup Instructions

### 1. Start Infrastructure

```bash
chmod +x setup.sh
./setup.sh
```

---

### 2. Start Streaming

```bash
pip install -r requirements.txt
python script/generate_data.py
```

```bash
python script/consumer.py
```

---

### 3. Run Airflow Pipelines

Trigger DAGs in Airflow UI:

* `gis_etl_pipeline`
* `fare_prediction_pipeline`
* `extract_predictions`
* `add_training_dataset`

---

### 4. Start API

```bash
docker compose -f ml_api/docker-compose.yaml up --build -d
```

---

## Environment Variables

### Airflow

| Variable           | Description    |
| ------------------ | -------------- |
| AIRFLOW_UID        | User ID        |
| AIRFLOW_FERNET_KEY | Encryption key |
| AIRFLOW_DB_URI     | DB connection  |
| AIRFLOW_USER       | Admin user     |
| AIRFLOW_PASSWORD   | Admin password |

### MinIO

| Variable            | Description    |
| ------------------- | -------------- |
| MINIO_ROOT_USER     | Access key     |
| MINIO_ROOT_PASSWORD | Secret         |
| ML_BUCKET           | Storage bucket |

### PostgreSQL

| Variable              | Description |
| --------------------- | ----------- |
| SRC_POSTGRES_DB       | DB name     |
| SRC_POSTGRES_USER     | User        |
| SRC_POSTGRES_PASSWORD | Password    |

---

## DAG Overview

### 1. `gis_etl_pipeline` (Daily)

* Extract raw trips
* Compute Haversine distance
* Assign zones
* Store in MinIO + PostgreSQL

### 2. `fare_prediction_pipeline` (Weekly)

* Train GBT model
* Feature engineering
* Save model + metrics

### 3. `extract_predictions` (6-hour)

* Batch inference
* Store predictions

### 4. `add_training_dataset` (Weekly)

* Feedback loop
* Improve training dataset

---

## Execution Order

```
1. gis_etl_pipeline
2. fare_prediction_pipeline
3. extract_predictions
4. add_training_dataset
```

---

## Service URLs

| Service  | URL                                                      |
| -------- | -------------------------------------------------------- |
| Airflow  | [http://localhost:8080](http://localhost:8080)           |
| Spark    | [http://localhost:9091](http://localhost:9091)           |
| MinIO    | [http://localhost:9001](http://localhost:9001)           |
| Grafana  | [http://localhost:3001](http://localhost:3001)           |
| Metabase | [http://localhost:3002](http://localhost:3002)           |
| API      | [http://localhost:8000/docs](http://localhost:8000/docs) |

---

## PostgreSQL Tables

| Table            | Description       |
| ---------------- | ----------------- |
| raw_trips        | Raw Kafka data    |
| trips_enriched   | GIS enriched data |
| model_metrics    | Training results  |
| trip_predictions | Predictions       |
| zone_daily_stats | Aggregations      |

---

## API Reference

### POST `/predict`

Predict fare:

```json
{
  "distance_km": 4.2,
  "vehicle_type": "motor"
}
```

---

### GET `/health`

Check API status.

---

## Notebooks

* `spatial_analysis.ipynb`
* GeoPandas + Folium visualizations
* Heatmaps, KDE, demand patterns

---

## Dashboards & Monitoring

* **Grafana** → infrastructure metrics
* **Prometheus** → time-series metrics
* **Metabase** → business analytics

---

## Airflow Connections

Re-register if needed:

```bash
docker exec airflow-webserver bash /init/init_connections.sh
```

---
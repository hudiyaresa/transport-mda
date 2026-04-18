"""
FastAPI — Fare Prediction Model Server
Endpoints: POST /predict  |  GET /health  |  GET /metrics
"""
import math, os, time, logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from pyspark.ml import PipelineModel
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import SparkSession
from prometheus_client import Counter, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SPARK_MASTER  = os.getenv('SPARK_MASTER_URL',  'local[2]')
MODEL_PATH    = os.getenv('MODEL_PATH',         's3a://transport-bucket/ml/model/fare_model_new')
PIPE_PATH     = os.getenv('PIPE_PATH',          's3a://transport-bucket/ml/pipeline_model')
S3_ENDPOINT   = os.getenv('S3_ENDPOINT_URL',    'http://minio:9000')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY',      'minio')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY',      'minio123')
MODEL_VERSION = os.getenv('MODEL_VERSION',      'gbt_fare_latest')

REGISTRY = CollectorRegistry()
REQUEST_COUNT   = Counter(
    'fare_requests_total', 'Total prediction requests', ['status'], registry=REGISTRY,
)
REQUEST_LATENCY = Histogram(
    'fare_request_latency_seconds', 'Request latency in seconds',
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
    registry=REGISTRY,
)

spark = pipeline_model = fare_model = None


def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName('FareServing')
        .master(SPARK_MASTER)
        .config('spark.hadoop.fs.s3a.access.key',             S3_ACCESS_KEY)
        .config('spark.hadoop.fs.s3a.secret.key',             S3_SECRET_KEY)
        .config('spark.hadoop.fs.s3a.endpoint',               S3_ENDPOINT)
        .config('spark.hadoop.fs.s3a.impl',                   'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.path.style.access',      'true')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .getOrCreate()
    )


def _try_load_model() -> bool:
    """Attempt to load pipeline + model from MinIO. Returns True on success."""
    global spark, pipeline_model, fare_model
    try:
        if spark is None:
            spark = _build_spark()
            spark.sparkContext.setLogLevel('WARN')
        pipeline_model = PipelineModel.load(PIPE_PATH)
        fare_model     = GBTRegressionModel.load(MODEL_PATH)
        log.info('Model loaded successfully — version: %s', MODEL_VERSION)
        return True
    except Exception as e:
        log.warning('Model not loaded (will retry on next request): %s', e)
        pipeline_model = None
        fare_model     = None
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Spark and attempt model load at startup.
    # If the model isn't in MinIO yet, log a warning but don't crash —
    # the model will be loaded lazily on the first /predict request.
    global spark
    spark = _build_spark()
    spark.sparkContext.setLogLevel('WARN')
    _try_load_model()
    yield
    if spark:
        spark.stop()


app = FastAPI(title='Jakarta Fare Prediction API', version='1.0.0', lifespan=lifespan)


class TripInput(BaseModel):
    origin_lat:       float = Field(..., example=-6.2088)
    origin_lng:       float = Field(..., example=106.8456)
    dest_lat:         float = Field(..., example=-6.1751)
    dest_lng:         float = Field(..., example=106.8272)
    distance_km:      float = Field(..., example=4.2)
    vehicle_type:     str   = Field(..., example='motor')
    surge_multiplier: float = Field(1.0, example=1.2)
    weather:          str   = Field('sunny', example='rain')
    hour_of_day:      int   = Field(..., example=8)
    day_of_week:      int   = Field(..., example=1)
    origin_zone:      str   = Field('Jakarta Pusat', example='Jakarta Selatan')
    dest_zone:        str   = Field('Jakarta Pusat', example='Jakarta Utara')


@app.get('/health')
def health():
    loaded = fare_model is not None
    return {
        'status':        'ok' if loaded else 'model_not_loaded',
        'model_version': MODEL_VERSION if loaded else None,
        'hint':          None if loaded else 'Run fare_prediction_pipeline DAG first, then POST /reload',
    }


@app.post('/reload')
def reload_model():
    """Manually trigger a model reload from MinIO. Call this after retraining."""
    success = _try_load_model()
    if success:
        return {'status': 'reloaded', 'model_version': MODEL_VERSION}
    raise HTTPException(503, f'Model not found at {MODEL_PATH}. Run fare_prediction_pipeline first.')


@app.post('/predict')
def predict(trip: TripInput):
    # Lazy load: if model wasn't available at startup, try now
    if fare_model is None:
        if not _try_load_model():
            raise HTTPException(
                503,
                'Model not loaded. Run fare_prediction_pipeline DAG first, '
                'then POST /reload or restart the container.',
            )
    t0 = time.perf_counter()
    try:
        df          = spark.createDataFrame([trip.model_dump()])
        transformed = pipeline_model.transform(df)
        log_pred    = fare_model.transform(transformed).select('prediction').collect()[0][0]
        fare_idr    = round((math.expm1(log_pred)) / 500) * 500
        elapsed_ms  = (time.perf_counter() - t0) * 1000
        REQUEST_COUNT.labels(status='ok').inc()
        REQUEST_LATENCY.observe(elapsed_ms / 1000)
        return {
            'predicted_fare_idr': fare_idr,
            'model_version':      MODEL_VERSION,
            'distance_km':        trip.distance_km,
            'processing_ms':      round(elapsed_ms, 2),
        }
    except Exception as e:
        REQUEST_COUNT.labels(status='error').inc()
        raise HTTPException(500, str(e))


@app.get('/metrics', response_class=PlainTextResponse, include_in_schema=False)
def metrics():
    return PlainTextResponse(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('fastapi_serving:app', host='0.0.0.0', port=8000)
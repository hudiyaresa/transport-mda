from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from helper.s3 import JAR_LIST, SPARK_CONF


@dag(
    dag_id='gis_etl_pipeline',
    start_date=datetime(2024, 9, 1),
    schedule='0 18 * * *',
    catchup=False,
    tags=['transport', 'gis', 'etl'],
)
def gis_etl_pipeline():
    extract_data = SparkSubmitOperator(
        task_id='extract_data',
        application='/opt/airflow/dags/gis_etl_pipeline/tasks/extract_data.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    enrich_geo = SparkSubmitOperator(
        task_id='enrich_geo',
        application='/opt/airflow/dags/gis_etl_pipeline/tasks/enrich_geo.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    load_raw_minio = SparkSubmitOperator(
        task_id='load_raw_minio',
        application='/opt/airflow/dags/gis_etl_pipeline/tasks/load_raw.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    load_warehouse = SparkSubmitOperator(
        task_id='load_warehouse',
        application='/opt/airflow/dags/gis_etl_pipeline/tasks/load_warehouse.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    extract_data >> enrich_geo >> load_raw_minio >> load_warehouse

gis_etl_pipeline()

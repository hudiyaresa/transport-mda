from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from helper.s3 import JAR_LIST, SPARK_CONF


@dag(
    dag_id='extract_predictions',
    start_date=datetime(2024, 9, 1),
    schedule='0 */6 * * *',
    catchup=False,
    tags=['transport', 'ml', 'batch-inference'],
)
def extract_predictions():
    score_trips = SparkSubmitOperator(
        task_id='score_unscored_trips',
        application='/opt/airflow/dags/extract_predictions/tasks/extract_predictions.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    score_trips

extract_predictions()

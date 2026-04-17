from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from helper.s3 import JAR_LIST, SPARK_CONF


@dag(
    dag_id='add_training_dataset',
    start_date=datetime(2024, 9, 1),
    schedule='0 18 * * 0',
    catchup=False,
    tags=['transport', 'ml', 'feedback-loop'],
)
def add_training_dataset():
    extract_valid = SparkSubmitOperator(
        task_id='extract_load_valid_predictions',
        application='/opt/airflow/dags/add_training_dataset/tasks/extract_load_valid_predictions.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    extract_valid

add_training_dataset()

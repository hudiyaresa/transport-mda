from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from helper.s3 import JAR_LIST, SPARK_CONF


@dag(
    dag_id='fare_prediction_pipeline',
    start_date=datetime(2024, 9, 1),
    schedule='0 19 * * 0',
    catchup=False,
    tags=['transport', 'ml', 'fare-prediction'],
)
def fare_prediction_pipeline():
    extract_data = SparkSubmitOperator(
        task_id='extract_data',
        application='/opt/airflow/dags/fare_prediction_pipeline/tasks/extract_data.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    preprocess = SparkSubmitOperator(
        task_id='preprocess',
        application='/opt/airflow/dags/fare_prediction_pipeline/tasks/preprocess.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    train = SparkSubmitOperator(
        task_id='train',
        application='/opt/airflow/dags/fare_prediction_pipeline/tasks/train.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    evaluate = SparkSubmitOperator(
        task_id='evaluate',
        application='/opt/airflow/dags/fare_prediction_pipeline/tasks/evaluate.py',
        conn_id='spark-conn',
        conf=SPARK_CONF,
        packages="org.postgresql:postgresql:42.2.23",
        verbose=True,
    )

    extract_data >> preprocess >> train >> evaluate

fare_prediction_pipeline()

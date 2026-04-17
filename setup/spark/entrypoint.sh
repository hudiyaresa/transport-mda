#!/bin/bash
set -e

SPARK_HOME=/opt/spark

case "$1" in
  master)
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master
    ;;
  worker)
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $2
    ;;
  *)
    exec "$@"
    ;;
esac

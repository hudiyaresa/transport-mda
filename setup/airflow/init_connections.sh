#!/bin/bash
set -e

register() {
  local NAME=$1; shift
  airflow connections delete "$NAME" 2>/dev/null || true
  airflow connections add "$NAME" "$@"
  echo "registered: $NAME"
}

register spark-conn \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077

register s3-conn \
  --conn-type     aws \
  --conn-login    minio \
  --conn-password minio123 \
  --conn-extra    '{"endpoint_url": "http://minio:9000"}'

register transport-source-db-conn \
  --conn-type     postgres \
  --conn-host     sources \
  --conn-port     5432 \
  --conn-schema   transport_db \
  --conn-login    postgres \
  --conn-password postgres

register transport-warehouse-db-conn \
  --conn-type     postgres \
  --conn-host     sources \
  --conn-port     5432 \
  --conn-schema   transport_db \
  --conn-login    postgres \
  --conn-password postgres

airflow connections list

#!/bin/bash
set -e

ACTION=${1:-up}
C='[0;36m'; G='[0;32m'; Y='[1;33m'; N='[0m'
step() { echo -e "\n${C}══ $1 ══${N}"; }

if [ "$ACTION" = "down" ]; then
  step "Stopping all services"
  for f in airflow-monitoring spark kafka data-lake sources airflow; do
    docker compose -f setup/$f/docker-compose.yml down -v 2>/dev/null || true
  done
  echo -e "${G}Done${N}"
  exit 0
fi

step "1/6 — Airflow"
docker compose -f setup/airflow/docker-compose.yml down -v
docker compose -f setup/airflow/docker-compose.yml up --build --detach --force-recreate

echo -e "${Y}Waiting for webserver...${N}"
until curl -sf http://localhost:8080/health > /dev/null 2>&1; do echo -n "."; sleep 5; done
echo -e "\n${G}Airflow ready${N}"

step "2/6 — Registering connections"
docker exec airflow-webserver airflow connections delete spark-conn               2>/dev/null || true
docker exec airflow-webserver airflow connections add spark-conn \
  --conn-type spark --conn-host "spark://spark-master" --conn-port 7077

docker exec airflow-webserver airflow connections delete s3-conn                  2>/dev/null || true
docker exec airflow-webserver airflow connections add s3-conn \
  --conn-type aws --conn-login minio --conn-password minio123 \
  --conn-extra '{"endpoint_url": "http://minio:9000"}'

docker exec airflow-webserver airflow connections delete transport-source-db-conn 2>/dev/null || true
docker exec airflow-webserver airflow connections add transport-source-db-conn \
  --conn-type postgres --conn-host sources --conn-port 5432 \
  --conn-schema transport_db --conn-login postgres --conn-password postgres

docker exec airflow-webserver airflow connections delete transport-warehouse-db-conn 2>/dev/null || true
docker exec airflow-webserver airflow connections add transport-warehouse-db-conn \
  --conn-type postgres --conn-host sources --conn-port 5432 \
  --conn-schema transport_db --conn-login postgres --conn-password postgres

echo "Registered connections:"
docker exec airflow-webserver airflow connections list
echo -e "${G}Connections registered${N}"

step "2b — Verifying spark-submit is available in Airflow container"
docker exec airflow-webserver bash -c 'which spark-submit && spark-submit --version 2>&1 | head -2' \
  && echo -e "${G}spark-submit found${N}" \
  || echo -e "\033[0;31mWARN: spark-submit not found on PATH — check SPARK_HOME in Dockerfile\033[0m"

step "3/6 — Monitoring"
docker compose -f setup/airflow-monitoring/docker-compose.yml down -v
docker compose -f setup/airflow-monitoring/docker-compose.yml up --build --detach --force-recreate

step "4/6 — MinIO"
docker compose -f setup/data-lake/docker-compose.yml down -v
docker compose -f setup/data-lake/docker-compose.yml up --build --detach --force-recreate

step "5/6 — PostgreSQL"
docker compose -f setup/sources/docker-compose.yml down -v
docker compose -f setup/sources/docker-compose.yml up --build --detach --force-recreate

step "6/6 — Spark"
docker compose -f setup/spark/docker-compose.yml down -v
docker compose -f setup/spark/docker-compose.yml up --build --detach --force-recreate

step "Kafka (optional)"
docker compose -f setup/kafka/docker-compose.yml down -v
docker compose -f setup/kafka/docker-compose.yml up --build --detach --force-recreate

pip install -r requirements.txt --quiet

echo -e "\n${C}══════════════════════════════════════════════════${N}"
printf "  %-16s %-35s %s\n" "Service" "URL" "Credentials"
printf "  %-16s %-35s %s\n" "───────────────" "──────────────────────────────────" "────────────────────"
printf "  %-16s %-35s %s\n" "Airflow"    "http://localhost:8080"  "admin / admin"
printf "  %-16s %-35s %s\n" "Spark UI"   "http://localhost:9091"  ""
printf "  %-16s %-35s %s\n" "MinIO"      "http://localhost:9001"  "minio / minio123"
printf "  %-16s %-35s %s\n" "Kafka UI"   "http://localhost:8085"  ""
printf "  %-16s %-35s %s\n" "Grafana"    "http://localhost:3001"  "admin / admin"
printf "  %-16s %-35s %s\n" "Prometheus" "http://localhost:9090"  ""
printf "  %-16s %-35s %s\n" "Jupyter"    "http://localhost:8888"  "token: transport123"
printf "  %-16s %-35s %s\n" "PostgreSQL" "localhost:5433"         "postgres/postgres/transport_db"
echo ""

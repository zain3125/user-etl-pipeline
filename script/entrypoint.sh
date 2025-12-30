#!/bin/bash
set -e

echo "Upgrading Airflow DB..."
airflow db upgrade

echo "Creating admin user (if not exists)..."
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin || true

echo "Starting Airflow: $@"
exec airflow "$@"

#!/bin/bash
set -e

echo "Installing requirements..."
if [ -f /opt/airflow/requirements.txt ]; then
  pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

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

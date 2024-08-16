#!/bin/bash

# Display a message before starting the services
echo "Starting Airflow and MongoDB services..."

# Start Airflow services
docker compose -f docker-compose-airflow.yaml -p airflow-etl-ohitv up -d

# Start MongoDB services
docker compose -f docker-compose-mongodb.yaml -p mongodb up -d

# Display a message after services are started
echo "Airflow and MongoDB services have been started."


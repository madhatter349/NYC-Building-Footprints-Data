#!/bin/bash

# Start Postgres (PostGIS image default entrypoint)
docker-entrypoint.sh postgres &

echo "Waiting for Postgres to be ready..."
until pg_isready -h localhost -p 5432; do
  sleep 2
done

echo "Running DB initializer..."
python main.py

echo "Done. Keeping container alive..."
tail -f /dev/null

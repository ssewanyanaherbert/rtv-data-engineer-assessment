#!/bin/bash
# Runs once, on first start of an empty data volume.
# Creates the Airflow metadata database and a read-only role for the dashboard.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE airflow LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;

    CREATE ROLE bi_reader LOGIN PASSWORD '${BI_READER_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO bi_reader;
EOSQL

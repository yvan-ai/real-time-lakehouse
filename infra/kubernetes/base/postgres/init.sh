#!/usr/bin/env bash
# Wrapper around init.sql — sets the debezium role password from an env var
# so it never appears in plaintext SQL committed to the repository.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -f /docker-entrypoint-initdb.d/init.sql

# Set debezium role password from the Secret-backed env var.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c "ALTER ROLE debezium WITH PASSWORD '${DEBEZIUM_PASSWORD}';"

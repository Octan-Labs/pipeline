#!/bin/bash

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

npx clickhouse-migrations migrate --host "$CLICKHOUSE_HOST" --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --db "$CLICKHOUSE_DATA" --migrations-home ./migrations

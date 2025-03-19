#!/bin/bash

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found"
    exit 1
fi

# Load environment variables
source .env

# Check required environment variables
required_vars=("DB_HOST" "DB_PORT" "DB_USER" "DB_PASSWORD" "DB_NAME")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: $var is not set in .env file"
        exit 1
    fi
done

# Function to execute psql command with retries
execute_psql() {
    local max_retries=3
    local retry_delay=5
    local attempt=1
    
    while [ $attempt -le $max_retries ]; do
        if PGPASSWORD=$DB_PASSWORD psql "postgresql://$DB_USER@$DB_HOST:$DB_PORT/$1?sslmode=require" -c "$2"; then
            return 0
        fi
        
        if [ $attempt -lt $max_retries ]; then
            echo "Attempt $attempt failed. Waiting $retry_delay seconds before retry..."
            sleep $retry_delay
            retry_delay=$((retry_delay * 2))  # Exponential backoff
        fi
        
        attempt=$((attempt + 1))
    done
    
    echo "Failed after $max_retries attempts"
    return 1
}

# Create database
echo "Creating database..."
execute_psql "postgres" "CREATE DATABASE $DB_NAME;" || true

# Wait a bit before proceeding
sleep 2

# Create table
echo "Creating tables..."
execute_psql "$DB_NAME" "CREATE TABLE IF NOT EXISTS alert_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_fingerprint TEXT,
    alert_name TEXT,
    alert_description TEXT,
    alert_summary TEXT,
    alert_severity TEXT,
    kubernetes_metadata JSONB,
    rule_id TEXT,
    severity TEXT,
    alert_types TEXT[],
    composite_query JSONB,
    api_status_code INTEGER,
    api_response TEXT,
    processing_time_ms INTEGER
);"

# Wait a bit before creating indexes
sleep 2

# Create indexes
echo "Creating indexes..."
execute_psql "$DB_NAME" "CREATE INDEX IF NOT EXISTS idx_alert_fingerprint ON alert_metrics(alert_fingerprint);"
execute_psql "$DB_NAME" "CREATE INDEX IF NOT EXISTS idx_alert_name ON alert_metrics(alert_name);"
execute_psql "$DB_NAME" "CREATE INDEX IF NOT EXISTS idx_alert_types ON alert_metrics USING GIN(alert_types);"
execute_psql "$DB_NAME" "CREATE INDEX IF NOT EXISTS idx_kubernetes_namespace ON alert_metrics((kubernetes_metadata->>'namespace'));"
execute_psql "$DB_NAME" "CREATE INDEX IF NOT EXISTS idx_rule_id ON alert_metrics(rule_id);"
execute_psql "$DB_NAME" "CREATE INDEX IF NOT EXISTS idx_timestamp ON alert_metrics(timestamp);"

echo "Database setup completed successfully" 
#!/bin/bash

# Load environment variables
set -a
source .env
set +a

# Create provisioning directory if it doesn't exist
mkdir -p grafana/provisioning/datasources

# Generate the datasource configuration with actual values
cat > grafana/provisioning/datasources/postgresql.yml << EOF
apiVersion: 1

datasources:
  - name: PostgreSQL
    type: postgres
    access: proxy
    url: ${POSTGRES_HOST}:${POSTGRES_PORT}
    database: ${POSTGRES_DB}
    user: ${POSTGRES_USER}
    secureJsonData:
      password: "${POSTGRES_PASSWORD}"
    jsonData:
      sslmode: "${POSTGRES_SSL_MODE}"
      maxOpenConns: 100
      maxIdleConns: 100
      maxIdleConnsAuto: true
      connMaxLifetime: 14400
      postgresVersion: 1500
      timescaledb: false
    editable: true
EOF

echo "Grafana datasource configuration generated successfully!"
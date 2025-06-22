#!/bin/bash

# Set variables
SRC_PROJECT="hubspot-452402"
SRC_DATASET="Hubspot_prod"
TODAY=$(date +%d%m%y)  # Format: DDMMYY
DEST_DATASET="${SRC_DATASET}_${TODAY}"

echo "🔁 Backing up ${SRC_DATASET} to ${DEST_DATASET}..."

# Delete destination dataset if it already exists
if bq ls --format=prettyjson ${SRC_PROJECT}:${DEST_DATASET} > /dev/null 2>&1; then
  echo "⚠️ Dataset ${DEST_DATASET} exists. Deleting it..."
  bq rm -r -f -d ${SRC_PROJECT}:${DEST_DATASET}
fi

# Create destination dataset
bq mk --location=EU --dataset --description "Backup of ${SRC_DATASET} on ${TODAY}" ${SRC_PROJECT}:${DEST_DATASET}

# List all tables in the source dataset
TABLES=$(bq ls -n 1000 --format=prettyjson ${SRC_PROJECT}:${SRC_DATASET} | jq -r '.[].tableReference.tableId')

# Copy each table
for table in $TABLES; do
  echo "📄 Copying table: $table"
  bq cp ${SRC_PROJECT}:${SRC_DATASET}.${table} ${SRC_PROJECT}:${DEST_DATASET}.${table}
done

echo "✅ Backup completed: ${DEST_DATASET}"

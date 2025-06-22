#!/bin/bash

# === CONFIG ===
PROJECT="hubspot-452402"
SRC_DATASET="Hubspot_staging"
DEST_DATASET="Hubspot_prod"
TIMESTAMP=$(date +"%d%m%y_%H%M%S")
BACKUP_DATASET="${DEST_DATASET}_${TIMESTAMP}"
LOG_FILE="sync_log_${TIMESTAMP}.txt"

# === COLORS ===
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log() {
  echo -e "$1" | tee -a "$LOG_FILE"
}

log "📋 Logging to $LOG_FILE"
log "${RED}⚠️  THIS WILL BACK UP, DELETE TABLES + VIEWS, AND OVERWRITE '${DEST_DATASET}' FROM '${SRC_DATASET}'${NC}"
log "Backup will be created as: ${BACKUP_DATASET}"
log ""

# === Detect region of DEST_DATASET ===
REGION=$(bq show --format=prettyjson ${PROJECT}:${DEST_DATASET} | jq -r '.location')
if [[ -z "$REGION" ]]; then
  log "${RED}❌ Failed to determine region of ${DEST_DATASET}. Aborting.${NC}"
  exit 1
fi
log "📍 Detected region: $REGION"

# === Create backup ===
log "📦 Creating backup dataset: ${BACKUP_DATASET}"
bq mk --location=${REGION} --description "Auto-backup of ${DEST_DATASET} on ${TIMESTAMP}" ${PROJECT}:${BACKUP_DATASET} >> "$LOG_FILE" 2>&1

TABLES_AND_VIEWS=$(bq ls --format=prettyjson ${PROJECT}:${DEST_DATASET} | \
  jq -r '.[] | select(.type == "TABLE" or .type == "VIEW") | .tableReference.tableId')

for table in $TABLES_AND_VIEWS; do
  log "🔄 Backing up: $table"
  bq cp --location=${REGION} ${PROJECT}:${DEST_DATASET}.${table} ${PROJECT}:${BACKUP_DATASET}.${table} >> "$LOG_FILE" 2>&1
done

log "🔍 Validating table backups (row count + schema)..."
VALID_BACKUP=true

TABLES_TO_VALIDATE=$(bq ls --format=prettyjson ${PROJECT}:${DEST_DATASET} | \
  jq -r '.[] | select(.type == "TABLE") | .tableReference.tableId')

for table in $TABLES_TO_VALIDATE; do
  orig_count=$(bq query --nouse_cache --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`${PROJECT}.${DEST_DATASET}.${table}\`" | tail -n 1)
  backup_count=$(bq query --nouse_cache --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) FROM \`${PROJECT}.${BACKUP_DATASET}.${table}\`" | tail -n 1)

  orig_schema=$(bq show --format=prettyjson ${PROJECT}:${DEST_DATASET}.${table} | jq -S '.schema.fields')
  backup_schema=$(bq show --format=prettyjson ${PROJECT}:${BACKUP_DATASET}.${table} | jq -S '.schema.fields')

  if [[ "$orig_count" != "$backup_count" ]]; then
    log "${RED}❌ $table: Row count mismatch ($orig_count ≠ $backup_count)${NC}"
    VALID_BACKUP=false
  elif [[ "$orig_schema" != "$backup_schema" ]]; then
    log "${RED}❌ $table: Schema mismatch${NC}"
    VALID_BACKUP=false
  else
    log "✅ $table: Backup valid"
  fi
done

if ! $VALID_BACKUP; then
  log "${RED}❌ Backup validation failed. Aborting to protect prod data.${NC}"
  exit 1
fi

log "${GREEN}✅ Backup passed all checks: ${BACKUP_DATASET}${NC}"
log ""

read -p "❗ Type '${DEST_DATASET}' to confirm DELETING TABLES + VIEWS and syncing from '${SRC_DATASET}': " CONFIRM
if [[ "$CONFIRM" != "$DEST_DATASET" ]]; then
  log "${RED}❌ Confirmation failed. Aborting.${NC}"
  exit 1
fi

log "🧹 Deleting all TABLES + VIEWS in ${DEST_DATASET}..."

TO_DELETE=$(bq ls --format=prettyjson ${PROJECT}:${DEST_DATASET} | \
  jq -r '.[] | select(.type == "TABLE" or .type == "VIEW") | .tableReference.tableId')

for table in $TO_DELETE; do
  log "❌ Deleting: $table"
  bq rm -f -t ${PROJECT}:${DEST_DATASET}.${table} >> "$LOG_FILE" 2>&1
done

log "📥 Copying only TABLES from ${SRC_DATASET} to ${DEST_DATASET}..."

TABLES_TO_COPY=$(bq ls --format=prettyjson ${PROJECT}:${SRC_DATASET} | \
  jq -r '.[] | select(.type == "TABLE") | .tableReference.tableId')

for table in $TABLES_TO_COPY; do
  log "📄 Copying: $table"
  bq cp --location=${REGION} ${PROJECT}:${SRC_DATASET}.${table} ${PROJECT}:${DEST_DATASET}.${table} >> "$LOG_FILE" 2>&1
done

log "${GREEN}🎉 Sync complete. Production now mirrors staging (only tables).${NC}"
log "📦 Backup saved as: ${BACKUP_DATASET}"
log "📋 Log file saved to: $LOG_FILE"

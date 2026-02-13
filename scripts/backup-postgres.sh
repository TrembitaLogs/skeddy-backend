#!/bin/sh
# PostgreSQL backup script for Skeddy
# Runs via cron in the backup Docker container (daily at 02:00 UTC)
#
# Required env vars: PGPASSWORD
# Optional env vars: BACKUP_DIR, BACKUP_RETENTION_DAYS, POSTGRES_HOST,
#   POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, BACKUP_S3_BUCKET, BACKUP_S3_PREFIX

set -eu

# Configuration from environment variables (with defaults)
BACKUP_DIR="${BACKUP_DIR:-/backups}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"
POSTGRES_HOST="${POSTGRES_HOST:-db}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-skeddy}"
POSTGRES_USER="${POSTGRES_USER:-skeddy}"
BACKUP_S3_BUCKET="${BACKUP_S3_BUCKET:-}"
BACKUP_S3_PREFIX="${BACKUP_S3_PREFIX:-backups}"

TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/skeddy_backup_${TIMESTAMP}.dump"
STATUS_FILE="${BACKUP_DIR}/.last_backup_status"

log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] BACKUP: $1"
}

log "Starting PostgreSQL backup..."
START_TIME=$(date +%s)

mkdir -p "${BACKUP_DIR}"

# Run pg_dump with custom format (compressed, supports selective restore)
if pg_dump \
    --host="${POSTGRES_HOST}" \
    --port="${POSTGRES_PORT}" \
    --username="${POSTGRES_USER}" \
    --dbname="${POSTGRES_DB}" \
    --format=custom \
    --file="${BACKUP_FILE}"; then

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    BACKUP_SIZE=$(stat -c %s "${BACKUP_FILE}" 2>/dev/null || echo "0")

    log "Backup completed: file=$(basename "${BACKUP_FILE}") size=${BACKUP_SIZE} bytes duration=${DURATION}s"

    printf '{"status":"ok","timestamp":"%s","file":"%s","size":%s,"duration":%s}\n' \
        "${TIMESTAMP}" "$(basename "${BACKUP_FILE}")" "${BACKUP_SIZE}" "${DURATION}" > "${STATUS_FILE}"
else
    EXIT_CODE=$?
    # Clean up partial backup file
    rm -f "${BACKUP_FILE}"
    log "ERROR: pg_dump failed with exit code ${EXIT_CODE}"
    printf '{"status":"error","timestamp":"%s","error":"pg_dump failed with exit code %s"}\n' \
        "${TIMESTAMP}" "${EXIT_CODE}" > "${STATUS_FILE}"
    exit 1
fi

# Optional S3/Spaces upload
if [ -n "${BACKUP_S3_BUCKET}" ]; then
    if command -v aws >/dev/null 2>&1; then
        BACKUP_FILENAME=$(basename "${BACKUP_FILE}")
        S3_PATH="s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/${BACKUP_FILENAME}"
        log "Uploading to S3: ${S3_PATH}"
        if aws s3 cp "${BACKUP_FILE}" "${S3_PATH}"; then
            log "S3 upload completed"
        else
            log "WARNING: S3 upload failed (backup is still saved locally)"
        fi
    else
        log "WARNING: aws CLI not available, skipping S3 upload"
    fi
fi

# Apply retention policy — delete backups older than BACKUP_RETENTION_DAYS
log "Applying retention policy: removing backups older than ${BACKUP_RETENTION_DAYS} days"
DELETED_COUNT=0
for f in $(find "${BACKUP_DIR}" -name "skeddy_backup_*.dump" -type f -mtime +"${BACKUP_RETENTION_DAYS}"); do
    rm -f "$f"
    DELETED_COUNT=$((DELETED_COUNT + 1))
    log "Deleted old backup: $(basename "$f")"
done
REMAINING=$(find "${BACKUP_DIR}" -name "skeddy_backup_*.dump" -type f | wc -l)
log "Retention: deleted=${DELETED_COUNT} remaining=${REMAINING}"

log "Backup process completed"

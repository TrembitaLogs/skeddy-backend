#!/bin/sh
# PostgreSQL backup script for Skeddy
# Runs via cron every 30 minutes in the backup Docker container.
# The script checks whether enough time has passed since the last backup
# (configurable interval) or whether a manual trigger file exists.
#
# Required env vars: PGPASSWORD
# Optional env vars: BACKUP_DIR, BACKUP_RETENTION_DAYS, POSTGRES_HOST,
#   POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, BACKUP_S3_BUCKET,
#   BACKUP_S3_PREFIX, BACKUP_S3_ENDPOINT, BACKUP_INTERVAL_HOURS

set -eu

# Configuration from environment variables (with defaults)
BACKUP_DIR="${BACKUP_DIR:-/backups}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-7}"
POSTGRES_HOST="${POSTGRES_HOST:-db}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-skeddy}"
POSTGRES_USER="${POSTGRES_USER:-skeddy}"
BACKUP_S3_BUCKET="${BACKUP_S3_BUCKET:-}"
BACKUP_S3_PREFIX="${BACKUP_S3_PREFIX:-backups}"
BACKUP_S3_ENDPOINT="${BACKUP_S3_ENDPOINT:-}"

# Interval configuration: read from config file, fallback to env var, default 24h
CONFIG_FILE="${BACKUP_DIR}/.backup_config.json"
STATUS_FILE="${BACKUP_DIR}/.last_backup_status"
TRIGGER_FILE="${BACKUP_DIR}/.trigger_backup"
DEFAULT_INTERVAL_HOURS="${BACKUP_INTERVAL_HOURS:-24}"

log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] BACKUP: $1"
}

mkdir -p "${BACKUP_DIR}"

# Read interval from config file (JSON: {"interval_hours": N, "retention_days": N})
INTERVAL_HOURS="${DEFAULT_INTERVAL_HOURS}"
if [ -f "${CONFIG_FILE}" ]; then
    # Parse interval_hours from JSON config
    PARSED_INTERVAL=$(sed -n 's/.*"interval_hours"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' "${CONFIG_FILE}")
    if [ -n "${PARSED_INTERVAL}" ] && [ "${PARSED_INTERVAL}" -gt 0 ] 2>/dev/null; then
        INTERVAL_HOURS="${PARSED_INTERVAL}"
    fi
    # Parse retention_days from JSON config
    PARSED_RETENTION=$(sed -n 's/.*"retention_days"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' "${CONFIG_FILE}")
    if [ -n "${PARSED_RETENTION}" ] && [ "${PARSED_RETENTION}" -gt 0 ] 2>/dev/null; then
        BACKUP_RETENTION_DAYS="${PARSED_RETENTION}"
    fi
fi

INTERVAL_SECONDS=$((INTERVAL_HOURS * 3600))

# Check if manual trigger exists
TRIGGERED=false
if [ -f "${TRIGGER_FILE}" ]; then
    log "Manual trigger detected — starting backup"
    rm -f "${TRIGGER_FILE}"
    TRIGGERED=true
fi

# Check interval since last backup (skip check if manually triggered)
if [ "${TRIGGERED}" = "false" ]; then
    if [ -f "${STATUS_FILE}" ]; then
        LAST_TIMESTAMP=$(sed -n 's/.*"timestamp"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${STATUS_FILE}")
        if [ -n "${LAST_TIMESTAMP}" ]; then
            # Convert timestamp (YYYYMMDD_HHMMSS) to epoch
            LAST_YEAR=$(echo "${LAST_TIMESTAMP}" | cut -c1-4)
            LAST_MONTH=$(echo "${LAST_TIMESTAMP}" | cut -c5-6)
            LAST_DAY=$(echo "${LAST_TIMESTAMP}" | cut -c7-8)
            LAST_HOUR=$(echo "${LAST_TIMESTAMP}" | cut -c10-11)
            LAST_MIN=$(echo "${LAST_TIMESTAMP}" | cut -c12-13)
            LAST_SEC=$(echo "${LAST_TIMESTAMP}" | cut -c14-15)
            LAST_EPOCH=$(date -u -d "${LAST_YEAR}-${LAST_MONTH}-${LAST_DAY} ${LAST_HOUR}:${LAST_MIN}:${LAST_SEC}" +%s 2>/dev/null || echo "0")
            NOW_EPOCH=$(date +%s)
            ELAPSED=$((NOW_EPOCH - LAST_EPOCH))
            if [ "${ELAPSED}" -lt "${INTERVAL_SECONDS}" ]; then
                REMAINING=$(( (INTERVAL_SECONDS - ELAPSED) / 60 ))
                log "Skipping: last backup ${ELAPSED}s ago, next in ~${REMAINING}m (interval=${INTERVAL_HOURS}h)"
                exit 0
            fi
        fi
    fi
fi

# Proceed with backup
TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/skeddy_backup_${TIMESTAMP}.dump"

log "Starting PostgreSQL backup..."
START_TIME=$(date +%s)

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
S3_ENDPOINT_FLAG=""
if [ -n "${BACKUP_S3_ENDPOINT}" ]; then
    S3_ENDPOINT_FLAG="--endpoint-url ${BACKUP_S3_ENDPOINT}"
fi

if [ -n "${BACKUP_S3_BUCKET}" ]; then
    if command -v aws >/dev/null 2>&1; then
        BACKUP_FILENAME=$(basename "${BACKUP_FILE}")
        S3_PATH="s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/${BACKUP_FILENAME}"
        log "Uploading to S3: ${S3_PATH}"
        # shellcheck disable=SC2086
        if aws s3 cp "${BACKUP_FILE}" "${S3_PATH}" ${S3_ENDPOINT_FLAG}; then
            log "S3 upload completed"
        else
            log "WARNING: S3 upload failed (backup is still saved locally)"
        fi

        # Remote retention: remove old backups from S3
        log "Applying remote retention: removing backups older than ${BACKUP_RETENTION_DAYS} days from S3"
        CUTOFF_DATE=$(date -u -d "-${BACKUP_RETENTION_DAYS} days" +"%Y%m%d" 2>/dev/null || date -u -v-${BACKUP_RETENTION_DAYS}d +"%Y%m%d")
        # shellcheck disable=SC2086
        aws s3 ls "s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/" ${S3_ENDPOINT_FLAG} 2>/dev/null | while read -r line; do
            REMOTE_FILE=$(echo "${line}" | awk '{print $NF}')
            # Extract date part from filename: skeddy_backup_YYYYMMDD_HHMMSS.dump
            FILE_DATE=$(echo "${REMOTE_FILE}" | sed -n 's/skeddy_backup_\([0-9]\{8\}\)_.*/\1/p')
            if [ -n "${FILE_DATE}" ] && [ "${FILE_DATE}" -lt "${CUTOFF_DATE}" ] 2>/dev/null; then
                log "Deleting old remote backup: ${REMOTE_FILE}"
                # shellcheck disable=SC2086
                aws s3 rm "s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/${REMOTE_FILE}" ${S3_ENDPOINT_FLAG} || true
            fi
        done
    else
        log "WARNING: aws CLI not available, skipping S3 upload"
    fi
fi

# Apply local retention policy — delete backups older than BACKUP_RETENTION_DAYS
log "Applying local retention policy: removing backups older than ${BACKUP_RETENTION_DAYS} days"
DELETED_COUNT=0
for f in $(find "${BACKUP_DIR}" -name "skeddy_backup_*.dump" -type f -mtime +"${BACKUP_RETENTION_DAYS}"); do
    rm -f "$f"
    DELETED_COUNT=$((DELETED_COUNT + 1))
    log "Deleted old backup: $(basename "$f")"
done
REMAINING=$(find "${BACKUP_DIR}" -name "skeddy_backup_*.dump" -type f | wc -l)
log "Retention: deleted=${DELETED_COUNT} remaining=${REMAINING}"

log "Backup process completed"

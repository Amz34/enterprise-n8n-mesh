#!/usr/bin/env bash
set -euo pipefail

# n8n Automated Nightly Snapshot Backup (Enterprise PostgreSQL + SQLite Fallback)
BACKUP_DIR="/home/ubuntu/projects/n8n/backups"
DATA_DIR="/home/ubuntu/projects/n8n/data"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
TARGET_SQLITE="${BACKUP_DIR}/n8n_backup_${TIMESTAMP}.sqlite"
TARGET_PG="${BACKUP_DIR}/n8n_backup_${TIMESTAMP}.dump"

mkdir -p "${BACKUP_DIR}"

# 1. Atomic dump of enterprise PostgreSQL n8n database
if docker exec centralised_agent_pgvector pg_isready -U agent_admin >/dev/null 2>&1; then
    docker exec centralised_agent_pgvector pg_dump -U agent_admin -d n8n_enterprise -Fc > "${TARGET_PG}"
    chmod 600 "${TARGET_PG}"
fi

# 2. SQLite backup fallback if file exists
if [ -f "${DATA_DIR}/database.sqlite" ]; then
    sqlite3 "${DATA_DIR}/database.sqlite" "VACUUM INTO '${TARGET_SQLITE}';"
    chmod 600 "${TARGET_SQLITE}"
fi

# 3. Retain only the last 7 daily backup files
find "${BACKUP_DIR}" -maxdepth 1 \( -name "n8n_backup_*.sqlite" -o -name "n8n_backup_*.dump" \) -type f -mtime +7 -delete

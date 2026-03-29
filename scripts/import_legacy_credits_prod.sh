#!/usr/bin/env bash
# Re-import legacy credits from old MySQL into production PostgreSQL.
# Run on the production server: bash scripts/import_legacy_credits_prod.sh
#
# Safe to re-run: uses ON CONFLICT and skips already claimed records.
set -euo pipefail

MYSQL_HOST="${LEGACY_MYSQL_HOST:-10.136.12.30}"
MYSQL_PORT="${LEGACY_MYSQL_PORT:-3306}"
MYSQL_USER="${LEGACY_MYSQL_USER:?Set LEGACY_MYSQL_USER}"
MYSQL_PASS="${LEGACY_MYSQL_PASS:?Set LEGACY_MYSQL_PASS}"
MYSQL_DB="${LEGACY_MYSQL_DB:-beta_skeddy}"

TSV_FILE="/tmp/legacy_credits_export.tsv"

echo "==> Exporting from MySQL ${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}..."
mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" \
  --batch --skip-column-names -e "
    SELECT u.id, u.phone_number, u.license_number,
           IFNULL(NULLIF(TRIM(p.name),''), '\\\\N'),
           IFNULL(NULLIF(TRIM(p.public_email),''), '\\\\N'),
           IFNULL(ub.balance, 0)
    FROM user u
    LEFT JOIN profile p ON u.id = p.user_id
    LEFT JOIN user_balance ub ON u.id = ub.user_id
    WHERE u.phone_number IS NOT NULL AND u.phone_number != ''
      AND u.license_number IS NOT NULL AND u.license_number != ''
  " 2>/dev/null > "$TSV_FILE"

ROWS=$(wc -l < "$TSV_FILE")
echo "==> Exported ${ROWS} rows"

echo "==> Copying into PostgreSQL container..."
docker cp "$TSV_FILE" skeddy-db:/tmp/legacy.tsv

echo "==> Importing into legacy_credits..."
docker exec skeddy-db psql -U skeddy -d skeddy -c "
CREATE TEMP TABLE _staging (
  old_user_id INTEGER,
  phone_number VARCHAR(20),
  license_number VARCHAR(50),
  name VARCHAR(255),
  email VARCHAR(255),
  balance INTEGER
);
COPY _staging FROM '/tmp/legacy.tsv' WITH (FORMAT text, NULL '\N');
INSERT INTO legacy_credits (id, old_user_id, phone_number, license_number, name, email, balance)
SELECT gen_random_uuid(), old_user_id, phone_number, license_number,
       NULLIF(name, '\N'), NULLIF(email, '\N'), balance
FROM _staging
ON CONFLICT (phone_number, license_number)
DO UPDATE SET
  balance = EXCLUDED.balance,
  name = EXCLUDED.name,
  email = EXCLUDED.email
WHERE legacy_credits.claimed_at IS NULL;
SELECT count(*) AS total,
       count(*) FILTER (WHERE claimed_at IS NOT NULL) AS claimed,
       count(*) FILTER (WHERE claimed_at IS NULL AND balance > 0) AS available
FROM legacy_credits;
"

rm -f "$TSV_FILE"
echo "==> Done"

#!/usr/bin/env python3
"""Import legacy credit balances from the old MySQL database into legacy_credits table.

Re-runnable: uses ON CONFLICT to update existing records (matched by phone+license).
Skips rows with empty phone_number or license_number.

Usage:
    uv run python scripts/import_legacy_credits.py [--dry-run]

Requires: pymysql (install with: uv add --dev pymysql)

Environment variables (or .env):
    DATABASE_URL          — new Skeddy PostgreSQL (already in .env)
    LEGACY_MYSQL_HOST     — old MySQL host     (default: 198.199.89.199)
    LEGACY_MYSQL_PORT     — old MySQL port     (default: 3306)
    LEGACY_MYSQL_USER     — old MySQL user     (default: skeddy)
    LEGACY_MYSQL_PASSWORD — old MySQL password
    LEGACY_MYSQL_DB       — old MySQL database (default: beta_skeddy)
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

# Ensure project root is on sys.path so `app` package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pymysql
from sqlalchemy import create_engine, text

from app.config import settings

# ---------------------------------------------------------------------------
# Old MySQL connection settings
# ---------------------------------------------------------------------------
MYSQL_HOST = os.getenv("LEGACY_MYSQL_HOST", "198.199.89.199")
MYSQL_PORT = int(os.getenv("LEGACY_MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("LEGACY_MYSQL_USER", "skeddy")
MYSQL_PASSWORD = os.getenv("LEGACY_MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("LEGACY_MYSQL_DB", "beta_skeddy")

MYSQL_QUERY = """
    SELECT u.id, u.phone_number, u.license_number, p.name, p.public_email, ub.balance
    FROM user u
    LEFT JOIN profile p ON u.id = p.user_id
    LEFT JOIN user_balance ub ON u.id = ub.user_id
"""

# ---------------------------------------------------------------------------
# PostgreSQL upsert
# ---------------------------------------------------------------------------
PG_UPSERT = text("""
    INSERT INTO legacy_credits (id, old_user_id, phone_number, license_number, name, email, balance)
    VALUES (:id, :old_user_id, :phone_number, :license_number, :name, :email, :balance)
    ON CONFLICT (phone_number, license_number)
    DO UPDATE SET
        balance = EXCLUDED.balance,
        name    = EXCLUDED.name,
        email   = EXCLUDED.email
    WHERE legacy_credits.claimed_at IS NULL
""")

BATCH_SIZE = 1000


def fetch_from_mysql() -> list[dict]:
    """Fetch all user credit data from the old MySQL database."""
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(MYSQL_QUERY)
            rows = cur.fetchall()
    finally:
        conn.close()

    print(f"Fetched {len(rows)} rows from MySQL")
    return rows


def sync_pg_url() -> str:
    """Convert async DATABASE_URL to synchronous for this one-off script."""
    url = settings.DATABASE_URL
    return url.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)


def import_to_postgres(rows: list[dict], *, dry_run: bool = False) -> None:
    """Upsert legacy credit rows into PostgreSQL."""
    engine = create_engine(sync_pg_url())

    skipped = 0
    prepared = []

    for row in rows:
        phone = (row.get("phone_number") or "").strip()
        license_num = (row.get("license_number") or "").strip()

        if not phone or not license_num:
            skipped += 1
            continue

        prepared.append(
            {
                "id": str(uuid.uuid4()),
                "old_user_id": row["id"],
                "phone_number": phone,
                "license_number": license_num,
                "name": (row.get("name") or "").strip() or None,
                "email": (row.get("public_email") or "").strip() or None,
                "balance": row.get("balance") or 0,
            }
        )

    print(f"Prepared {len(prepared)} rows, skipped {skipped} (missing phone/license)")

    if dry_run:
        print("[DRY RUN] No changes written to PostgreSQL")
        for r in prepared[:5]:
            print(
                f"  sample: {r['phone_number']} / {r['license_number']} — balance={r['balance']}"
            )
        return

    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(prepared), BATCH_SIZE):
            batch = prepared[i : i + BATCH_SIZE]
            conn.execute(PG_UPSERT, batch)
            inserted += len(batch)
            print(f"  upserted {inserted}/{len(prepared)}...")

    print(f"Done. {inserted} rows upserted into legacy_credits")


def main() -> None:
    parser = argparse.ArgumentParser(description="Import legacy credits from old MySQL DB")
    parser.add_argument(
        "--dry-run", action="store_true", help="Fetch and validate without writing"
    )
    args = parser.parse_args()

    if not MYSQL_PASSWORD:
        print("ERROR: LEGACY_MYSQL_PASSWORD is not set", file=sys.stderr)
        sys.exit(1)

    rows = fetch_from_mysql()
    import_to_postgres(rows, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

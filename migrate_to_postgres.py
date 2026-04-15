#!/usr/bin/env python3
"""
Migrate data from local SQLite (edge_signals.db) to PostgreSQL (Railway).

Usage:
    DATABASE_URL="postgresql://..." python migrate_to_postgres.py
"""

import os
import sys
import sqlite3
import psycopg2
import psycopg2.extras
from datetime import datetime

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "data", "edge_signals.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable")
    print('  export DATABASE_URL="postgresql://user:pass@host:port/dbname"')
    sys.exit(1)

if not os.path.exists(SQLITE_PATH):
    print(f"ERROR: SQLite database not found at {SQLITE_PATH}")
    sys.exit(1)


def migrate():
    print("=" * 60)
    print("  SQLite → PostgreSQL Migration")
    print("=" * 60)

    # Connect to both databases
    sqlite_db = sqlite3.connect(SQLITE_PATH)
    sqlite_db.row_factory = sqlite3.Row

    pg = psycopg2.connect(DATABASE_URL)
    pg.autocommit = False

    # Create tables first (import from edge_db)
    print("\n[1/7] Creating PostgreSQL tables...")
    os.environ["DATABASE_URL"] = DATABASE_URL
    from edge_db import _create_tables
    _create_tables(pg)
    print("  ✓ Tables created")

    # Migrate each table
    tables = [
        ("stocks", "orderbook_id"),
        ("insider_transactions", None),
        ("owner_snapshots", None),
        ("owner_history", None),
        ("meta", "key"),
        ("simulation_holdings", None),
        ("simulation_trades", None),
        ("ai_scores", None),
    ]

    for table, conflict_col in tables:
        _migrate_table(sqlite_db, pg, table, conflict_col)

    sqlite_db.close()
    pg.close()

    print("\n" + "=" * 60)
    print("  ✓ Migration complete!")
    print("=" * 60)


def _migrate_table(sqlite_db, pg, table, conflict_col=None):
    """Migrate a single table from SQLite to PostgreSQL."""
    # Get row count
    count = sqlite_db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if count == 0:
        print(f"\n[{table}] Empty, skipping")
        return

    print(f"\n[{table}] Migrating {count:,} rows...")

    # Get column names from SQLite
    cursor = sqlite_db.execute(f"SELECT * FROM {table} LIMIT 1")
    columns = [desc[0] for desc in cursor.description]
    # Filter out 'id' column for tables with SERIAL primary key
    if table not in ("stocks", "meta"):
        columns = [c for c in columns if c != "id"]

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    # Build upsert or insert SQL
    if conflict_col:
        update_cols = [c for c in columns if c != conflict_col]
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({conflict_col}) DO UPDATE SET {update_set}"
    else:
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    # Fetch all rows
    if table not in ("stocks", "meta"):
        select_cols = ", ".join(columns)
    else:
        select_cols = col_list

    rows = sqlite_db.execute(f"SELECT {select_cols} FROM {table}").fetchall()

    # Batch insert
    batch_size = 1000
    cur = pg.cursor()
    inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        for row in batch:
            try:
                cur.execute(sql, tuple(row))
                inserted += 1
            except Exception as e:
                pg.rollback()
                print(f"  ⚠ Error on row: {e}")
                # Try to continue
                pg = psycopg2.connect(DATABASE_URL)
                pg.autocommit = False
                cur = pg.cursor()
                continue

        pg.commit()
        if (i + batch_size) % 10000 == 0 or i + batch_size >= len(rows):
            print(f"  {min(i + batch_size, len(rows)):,}/{count:,} rows...")

    cur.close()
    print(f"  ✓ {inserted:,} rows migrated")


if __name__ == "__main__":
    migrate()

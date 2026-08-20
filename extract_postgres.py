#!/usr/bin/env python3
"""
Extract-Load: Postgres app schema → DuckDB raw schema.

Incremental on updated_at / created_at / changed_at watermarks.
Hard-delete detection via users_cdc op='D' events applied to raw.users.
"""

import os
import sys
from datetime import datetime, timezone

import duckdb
import psycopg2

PG_DSN = "host=localhost port=5432 dbname=endeavoriq user=endeavoriq password=endeavoriq"

_HERE = os.path.dirname(os.path.abspath(__file__))
DUCK_PATH = os.path.join(os.path.dirname(_HERE), "pipeline.duckdb")

PG_SCHEMA = "app"
DUCK_SCHEMA = "raw"
EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

PG_TO_DUCK = {
    "integer":                  "INTEGER",
    "bigint":                   "BIGINT",
    "text":                     "VARCHAR",
    "boolean":                  "BOOLEAN",
    "timestamp with time zone": "TIMESTAMPTZ",
    "date":                     "DATE",
    "character":                "VARCHAR",
    "character varying":        "VARCHAR",
}

# subscription_changes is append-only (no updated_at); users_cdc uses changed_at.
TABLES = [
    {"name": "accounts",             "pk": "id",  "watermark": "updated_at"},
    {"name": "users",                "pk": "id",  "watermark": "updated_at"},
    {"name": "plans",                "pk": "id",  "watermark": "updated_at"},
    {"name": "subscriptions",        "pk": "id",  "watermark": "updated_at"},
    {"name": "subscription_changes", "pk": "id",  "watermark": "created_at"},
    {"name": "users_cdc",            "pk": "seq", "watermark": "changed_at"},
]


def pg_columns(pg_cur, table_name):
    pg_cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (PG_SCHEMA, table_name),
    )
    return [(row[0], PG_TO_DUCK.get(row[1], "VARCHAR")) for row in pg_cur.fetchall()]


def ensure_table(duck, table_name, columns, pk, extra=None):
    col_defs = [f'"{c}" {t}' for c, t in columns]
    if extra:
        col_defs += [f'"{c}" {t}' for c, t in extra]
    duck.execute(
        f'CREATE TABLE IF NOT EXISTS {DUCK_SCHEMA}."{table_name}" '
        f'({", ".join(col_defs)}, PRIMARY KEY ("{pk}"))'
    )


def get_watermark(duck, key):
    row = duck.execute(
        "SELECT last_value FROM raw.el_watermarks WHERE table_name = ?", (key,)
    ).fetchone()
    return row[0] if row else EPOCH


def set_watermark(duck, key, value):
    duck.execute(
        "INSERT OR REPLACE INTO raw.el_watermarks (table_name, last_value) VALUES (?, ?)",
        (key, value),
    )


def load_table(pg_cur, duck, cfg):
    name = cfg["name"]
    pk = cfg["pk"]
    wm_col = cfg["watermark"]

    cols = pg_columns(pg_cur, name)
    # users gets a synthetic deleted_at to track hard deletes (absent in Postgres source)
    extra = [("deleted_at", "TIMESTAMPTZ")] if name == "users" else None
    ensure_table(duck, name, cols, pk, extra=extra)

    wm = get_watermark(duck, name)
    pg_cur.execute(
        f'SELECT * FROM {PG_SCHEMA}."{name}" WHERE "{wm_col}" > %s ORDER BY "{wm_col}"',
        (wm,),
    )
    rows = pg_cur.fetchall()

    if rows:
        col_names = ", ".join(f'"{c}"' for c, _ in cols)
        placeholders = ", ".join(["?"] * len(cols))
        duck.executemany(
            f'INSERT OR REPLACE INTO {DUCK_SCHEMA}."{name}" ({col_names}) VALUES ({placeholders})',
            rows,
        )
        wm_idx = [c for c, _ in cols].index(wm_col)
        set_watermark(duck, name, max(r[wm_idx] for r in rows))

    print(f"  {name:<25} {len(rows):>6} rows   (watermark: {str(wm)[:19]})")
    return len(rows)


def apply_hard_deletes(duck):
    """
    Scan raw.users_cdc for D events not yet applied, then:
    - UPDATE deleted_at on existing raw.users rows, or
    - INSERT a tombstone if the user was deleted before/during the first load.
    Uses its own watermark ('hard_deletes_applied') independent of the cdc data load.
    """
    wm = get_watermark(duck, "hard_deletes_applied")

    deletes = duck.execute(
        """
        SELECT user_id, account_id, email, full_name, role, status, changed_at
        FROM raw.users_cdc
        WHERE op = 'D' AND changed_at > ?
        ORDER BY changed_at
        """,
        (wm,),
    ).fetchall()

    if not deletes:
        print(f"  {'users (hard deletes)':<25}      0 applied")
        return

    for user_id, account_id, email, full_name, role, status, changed_at in deletes:
        exists = duck.execute(
            'SELECT id FROM raw.users WHERE id = ?', (user_id,)
        ).fetchone()
        if exists:
            duck.execute(
                'UPDATE raw.users SET deleted_at = ? WHERE id = ?',
                (changed_at, user_id),
            )
        else:
            # User deleted before first load — insert tombstone from CDC data
            duck.execute(
                """
                INSERT INTO raw.users
                    (id, account_id, email, full_name, role, status,
                     created_at, updated_at, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, account_id, email, full_name, role, status,
                 changed_at, changed_at, changed_at),
            )

    set_watermark(duck, "hard_deletes_applied", max(r[6] for r in deletes))
    print(f"  {'users (hard deletes)':<25} {len(deletes):>6} applied")


def main():
    print(f"Connecting to Postgres …")
    try:
        pg = psycopg2.connect(PG_DSN)
    except psycopg2.OperationalError as e:
        sys.exit(f"Postgres connection failed: {e}")
    pg.set_session(readonly=True, autocommit=True)
    pg_cur = pg.cursor()

    print(f"Opening DuckDB: {DUCK_PATH}\n")
    duck = duckdb.connect(DUCK_PATH)
    duck.execute(f"CREATE SCHEMA IF NOT EXISTS {DUCK_SCHEMA}")
    duck.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.el_watermarks (
            table_name VARCHAR PRIMARY KEY,
            last_value TIMESTAMPTZ NOT NULL
        )
        """
    )

    print("Loading tables:")
    total = sum(load_table(pg_cur, duck, cfg) for cfg in TABLES)

    # Must run after users_cdc is loaded so we can read from raw.users_cdc
    print("\nApplying hard deletes to raw.users:")
    apply_hard_deletes(duck)

    duck.close()
    pg.close()
    print(f"\nDone. {total} rows upserted across {len(TABLES)} tables.")


if __name__ == "__main__":
    main()

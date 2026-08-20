"""
Snapshot raw.users before the EL run so the DIY hard-delete model
(stg_app__users_deleted_diy) can detect deletions without users_cdc.

Run BEFORE extract_postgres.py on each pipeline cycle:
    python3 el/snapshot_users.py
    python3 el/extract_postgres.py
    ...

On the first ever run, raw.users may not yet exist; the snapshot will
be an empty table in that case and no deletions will be reported until
the second cycle.
"""
import duckdb

DB_PATH = "pipeline.duckdb"


def snapshot_users() -> None:
    con = duckdb.connect(DB_PATH)
    try:
        users_exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='raw' AND table_name='users'"
        ).fetchone()[0]

        if users_exists:
            con.execute(
                "CREATE OR REPLACE TABLE raw.users_snapshot AS SELECT * FROM raw.users"
            )
            n = con.execute("SELECT COUNT(*) FROM raw.users_snapshot").fetchone()[0]
            print(f"Snapshotted {n} rows from raw.users → raw.users_snapshot")
        else:
            # First-ever run: create empty snapshot so dbt source doesn't fail
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.users_snapshot (
                    id         INTEGER PRIMARY KEY,
                    account_id INTEGER,
                    email      VARCHAR,
                    full_name  VARCHAR,
                    role       VARCHAR,
                    status     VARCHAR,
                    created_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ,
                    deleted_at TIMESTAMPTZ
                )
            """)
            print("raw.users not found yet — created empty raw.users_snapshot")
    finally:
        con.close()


if __name__ == "__main__":
    snapshot_users()

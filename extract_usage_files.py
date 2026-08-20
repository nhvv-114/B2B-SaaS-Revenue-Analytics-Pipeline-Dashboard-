#!/usr/bin/env python3
"""
Extract-Load: daily JSONL usage files → DuckDB raw.usage_events.

Incremental strategy: tracks each file's mtime in raw.usage_file_state.
- New file (not in state table) → load.
- File mtime changed → reload (catches late-arriving events added to a prior day's file).
- File mtime unchanged → skip.

Handles:
  - Missing files (gaps in date sequence) — silently skipped, not an error.
  - Out-of-order lines within a file — irrelevant; we don't sort.
  - Duplicate event_ids within a file — deduped via dict before insert.
  - Schema drift: 'region' column absent in files before 2025-11-01 — handled with .get().
  - INSERT OR REPLACE on event_id ensures idempotency across runs.
"""

import glob
import json
import os
from datetime import datetime, timezone

import duckdb

_HERE = os.path.dirname(os.path.abspath(__file__))
USAGE_DIR = os.path.join(os.path.dirname(_HERE), "drop", "usage")
DUCK_PATH = os.path.join(os.path.dirname(_HERE), "pipeline.duckdb")
DUCK_SCHEMA = "raw"


DDL_EVENTS = """
    CREATE TABLE IF NOT EXISTS raw.usage_events (
        event_id   VARCHAR PRIMARY KEY,
        account_id INTEGER,
        usage_date DATE,
        metric     VARCHAR,
        quantity   DOUBLE,
        region     VARCHAR    -- NULL for files before schema drift (2025-11-01)
    )
"""

DDL_FILE_STATE = """
    CREATE TABLE IF NOT EXISTS raw.usage_file_state (
        filename  VARCHAR PRIMARY KEY,
        mtime     DOUBLE,      -- os.path.getmtime() — float seconds since epoch
        loaded_at TIMESTAMPTZ,
        row_count INTEGER
    )
"""


def load_file(duck, path, filename):
    """
    Read one JSONL file, deduplicate by event_id, upsert into raw.usage_events.
    Returns the number of unique events written.
    """
    events = {}  # event_id → row tuple; dict dedupes within the file

    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError:
                continue  # skip malformed lines

            eid = obj.get("event_id")
            if not eid:
                continue

            events[eid] = (
                eid,
                obj.get("account_id"),
                obj.get("usage_date"),
                obj.get("metric"),
                obj.get("quantity"),
                obj.get("region"),   # None for pre-drift files — stored as NULL
            )

    if events:
        duck.executemany(
            """
            INSERT OR REPLACE INTO raw.usage_events
                (event_id, account_id, usage_date, metric, quantity, region)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            list(events.values()),
        )

    return len(events)


def get_file_state(duck):
    """Return {filename: mtime} from the metadata table."""
    rows = duck.execute(
        "SELECT filename, mtime FROM raw.usage_file_state"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def set_file_state(duck, filename, mtime, row_count):
    duck.execute(
        """
        INSERT OR REPLACE INTO raw.usage_file_state
            (filename, mtime, loaded_at, row_count)
        VALUES (?, ?, ?, ?)
        """,
        (filename, mtime, datetime.now(tz=timezone.utc), row_count),
    )


def main():
    print(f"Opening DuckDB: {DUCK_PATH}\n")
    duck = duckdb.connect(DUCK_PATH)
    duck.execute(f"CREATE SCHEMA IF NOT EXISTS {DUCK_SCHEMA}")
    duck.execute(DDL_EVENTS)
    duck.execute(DDL_FILE_STATE)

    state = get_file_state(duck)

    all_files = sorted(glob.glob(os.path.join(USAGE_DIR, "usage_*.jsonl")))
    if not all_files:
        print(f"No usage files found in {USAGE_DIR}")
        duck.close()
        return

    loaded = skipped = total_rows = 0

    print(f"Scanning {len(all_files)} files in {USAGE_DIR}:")
    for path in all_files:
        filename = os.path.basename(path)
        mtime = os.path.getmtime(path)
        known_mtime = state.get(filename)

        if known_mtime is not None and abs(known_mtime - mtime) < 0.001:
            skipped += 1
            continue  # unchanged — skip

        action = "reload" if known_mtime is not None else "load"
        row_count = load_file(duck, path, filename)
        set_file_state(duck, filename, mtime, row_count)
        total_rows += row_count
        loaded += 1
        print(f"  [{action:>6}] {filename}  →  {row_count} events")

    duck.close()
    print(
        f"\nDone. {loaded} files processed, {skipped} skipped (unchanged)."
        f"  {total_rows} events upserted."
    )


if __name__ == "__main__":
    main()

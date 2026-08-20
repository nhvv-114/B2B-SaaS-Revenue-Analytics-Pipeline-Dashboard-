# Writeup

## How to run it

```bash
make up        # start Postgres + billing API (Docker)
make seed      # generate ~18 months of history
make run       # EL (all three sources) → dbt build, end-to-end
```

`make run` runs `el/extract_postgres.py`, `el/extract_billing_api.py`,
`el/extract_usage_files.py` in sequence, then `cd transform && dbt build --profiles-dir .`.
Requires Python 3 with `psycopg2` and `duckdb` installed, and `dbt-duckdb`.
All output lands in `pipeline.duckdb` at the repo root.

---

## EL / CDC strategy

### Source A — Postgres (`el/extract_postgres.py`)

Watermark-based incremental on `updated_at` (accounts, users, plans, subscriptions),
`created_at` (subscription\_changes — append-only log, no updated\_at), and `changed_at`
(users\_cdc). Watermarks are stored per-table in `raw.el_watermarks` (DuckDB). Every run
queries `WHERE watermark_col > last_seen`, upserts via `INSERT OR REPLACE` on primary key,
then advances the watermark to the max value of the batch. A re-run with no tick produces
zero rows on every Postgres table — the watermark precision is microseconds so there is no
boundary-overlap race.

**Soft deletes (accounts, subscriptions):** `deleted_at` is carried through as-is. The row
stays in the source and my watermark catches it when `updated_at` bumps (the simulator sets
`updated_at = deleted_at` on churn). No special handling needed.

**Hard deletes (users):** Postgres hard-deletes user rows on GDPR erasure. The EL adds a
synthetic `deleted_at` column to `raw.users`. After loading `users_cdc`, a second pass
scans new `op='D'` rows and either UPDATEs `deleted_at` on an existing `raw.users` row or
INSERTs a tombstone from the CDC data if the user was already gone before the first load.
A separate `hard_deletes_applied` watermark tracks this independently so it advances
correctly even if the data load and delete-apply run at different cadences.

`stg_app__users` filters `WHERE deleted_at IS NULL` to be source-faithful (app.users only
contains live rows). The tombstones in raw stay available for downstream auditing.

### Source B — Billing API (`el/extract_billing_api.py`)

The API offers no server-side date filtering — only cursor pagination (`starting_after`).
Every run paginates all four endpoints in full and upserts via `INSERT OR REPLACE`. This is
O(total records) per run rather than O(delta), but at ~2k invoices it completes in under
a second. `INSERT OR REPLACE` makes it fully idempotent: re-running produces identical
state. Invoice line items are unnested during extraction and stored in a separate
`raw.billing_invoice_line_items` table to avoid JSON blobs in DuckDB.

### Source C — Usage files (`el/extract_usage_files.py`)

Incremental via file-level mtime tracking in `raw.usage_file_state`. Each run:
1. Globs `drop/usage/usage_*.jsonl` and compares each file's current mtime to the stored value.
2. **New file** (not in state table) → load.
3. **mtime changed** → reload. This is the late-arrival trigger: when `make tick` writes
   additional lines to `usage_2026-05-29.jsonl`, that file's mtime changes and the next run
   reprocesses it. `INSERT OR REPLACE` on `event_id` deduplicates against previously loaded
   events from that file.
4. **mtime unchanged** → skip. A re-run with no tick processes 0 files.

Duplicate `event_id`s within a file are collapsed into a `dict` before any DB write (last
occurrence wins). Missing dates (gaps in the sequence) are silently skipped — `glob` only
finds files that exist.

---

## Data-quality issues found

| Issue | Where | How handled |
|---|---|---|
| **Missing `account_id`** on 30/513 billing customers (`metadata.account_id` absent in API) | billing\_customers | 25 recovered in `int_billing_account_link` by matching `split_part(email,'@',2)` to `accounts.domain`. 5 remain unlinked — their invoices are excluded from `fct_revenue` rather than attributed incorrectly. |
| **Duplicate `event_id`s** — 10 cross-file and intra-file dupes across 2 files | usage\_events | Dict dedup in EL (keyed by event\_id); `INSERT OR REPLACE` handles cross-file collisions. |
| **Schema drift** — `region` field appears in usage files starting 2025-11-01; absent in earlier files | usage\_events | `obj.get('region')` returns `None` for pre-drift files; stored as NULL. `stg_usage__events` exposes `region` as nullable. |
| **Missing files** — 3 dates absent from usage sequence (2025-10-10, 2026-05-17, 2026-05-29) | usage\_events | Silently skipped. 2026-05-29 arrived as a late file on the first `tick`, correctly reloaded due to mtime change. |
| **Out-of-order lines** within usage files | usage\_events | Irrelevant — dedup by event\_id is order-agnostic. |
| **Epoch timestamps + integer cents** in billing API | billing\_\* | `datetime.fromtimestamp(..., tz=UTC)` in EL; `/ 100.0` for dollars. Staging exposes cents columns (`ROUND(total_dollars * 100)::BIGINT`) for manifest source-faithfulness. |
| **Plan effective-dating** — team monthly price changed 2500→2800 on 2025-09-01 as a new plan row (id=9) while subscriptions still reference old plan id=3 | fct\_revenue | Double-join in mart: `p_ref` resolves `plan_id → (code, billing_interval)`; `p_eff` finds the price row effective in that month. Subscriptions on old plan ids automatically get the new price without any subscription\_change event. |
| **No `created` change\_type** in subscription\_changes | int\_subscription\_periods | Initial subscription state inferred from first change's `old_plan_id` / `old_seats`. Subscriptions with zero changes use the current state from the subscriptions table for their full duration. |
| **Null `price` on usage line items** | billing\_invoice\_line\_items | `(li.get('price') or {})` in EL; `unit_amount_dollars` and `billing_interval` are NULL for usage lines, with `metric` field present instead. |

---

## Key modeling decisions

### Layering

- **`raw.*`** — landed as-is by EL, types cast to DuckDB-native, cents→dollars conversion
  in billing for convenience. Source-faithful except for the `deleted_at` tombstone column
  added to `raw.users`.
- **`stg_*`** — explicit column lists, cents re-exposed for billing manifest columns,
  `stg_app__users` filtered to live rows only. No business logic, no recovered account links.
- **`int_*`** — two models: subscription period reconstruction and billing↔account link
  recovery. Kept separate from staging so staging remains testably faithful.
- **`fct_revenue`** — final mart, materialized as table.

### Subscription point-in-time state

`int_subscription_periods` builds a (subscription\_id, plan\_id, seats, valid\_from, valid\_to)
timeline using three CTEs unioned together:
1. Pre-first-change state from `old_plan_id / old_seats` of the first change.
2. Post-change states from `new_plan_id / new_seats` of each non-cancel change.
3. Subscriptions with no changes at all — current state for full lifespan.

Cancel events have `new_plan_id = NULL` and are naturally excluded by `WHERE new_plan_id IS NOT NULL`.

Month attribution: a period is active in month M if
`date_trunc('month', valid_from) ≤ M < date_trunc('month', valid_to)`.
When a change lands mid-month, the entire month is attributed to the new state. This is
consistent with a start-of-month snapshot convention.

### Annual normalization

Annual `seat_price_cents` is divided by 12 to get monthly MRR. The annual prices are
discounted relative to 12 × monthly (e.g., team annual = $250/seat/mo vs $280 monthly),
so normalizing at ÷12 slightly understates MRR vs what a customer would pay if billed
monthly. This is the standard SaaS practice for MRR reporting.

### MRR movement convention

Comparison is prior calendar month via `LAG(total_mrr)`:

| Condition | Classification |
|---|---|
| No prior MRR → MRR > 0 | **new** |
| Prior MRR > 0 → MRR = 0 | **churn** |
| MRR increased | **expansion** |
| MRR decreased | **contraction** |
| Same MRR, > 0 | **retained** |

**Reactivation after churn** → classified as **new**. Rationale: the customer left and
returned; economically identical to a new logo from a revenue-growth perspective. A
separate `reactivation` bucket would require storing the last-churn date per account,
adding complexity for a category that's typically small and often merged with "new" in
board reporting anyway.

**Plan downgrade at flat seats** → classified as **contraction**. A Business→Team change
with the same seat count still reduces MRR. The revenue signal matters regardless of
whether it was driven by seats or price tier.

**Churn row inclusion**: the single zero-revenue month immediately after a customer's last
revenue month is kept in the output (the WHERE clause retains `prev_mrr > 0 AND total_mrr = 0`).
Subsequent zero months are dropped to keep the table clean.

---

## Known gaps & bugs

**Billing incremental is a full scan.** The API has no date-range filter, so every run
re-fetches all records. At ~2k invoices this is negligible, but at 10M+ records this would
be the first thing to address (cursor bookmark, or a CDC table on the billing provider's
webhook stream).

**mtime-based late-arrival detection has a blind spot.** If a usage file is rewritten with
new lines but the filesystem mtime is somehow unchanged (e.g., copied from a source that
preserves timestamps), the pipeline won't reload it. In production I'd also compare row
count or file hash.

**5 unlinked billing customers.** Domain-matching recovers 25/30 with NULL `account_id`.
The remaining 5 have emails on generic domains (or domains that don't match any account in
the DB). Their invoices are excluded from `fct_revenue`. Worth investigating manually;
they might be linkable via a fuzzy name match or a billing→app user email join.

**June 2026 usage MRR = $0.** The billing API shows no new invoices were generated in the
last tick. Usage overage invoices may lag by a billing cycle — June's overages would
appear in July's invoices. The pipeline is correct; this is a data timing gap, not a bug.

**Subscription periods with zero history.** If the EL pipeline is run for the first time
against a subscription that already has changes in `subscription_changes`, the initial
state is reconstructed correctly. But if `subscription_changes` is purged or unavailable
for older subscriptions, `initial_states` would miss the pre-pipe portion. Currently
mitigated by loading all history on first run.

**No SCD2 `dim_accounts`.** Account attributes (plan tier, domain, status) are only
available as current-state snapshots. Historically tracking when an account changed status
is possible via `subscription_changes` but not implemented.

**`stg_billing__invoices.total` unit note.** Billing amounts were converted to dollars in
the EL layer (per the task spec). Staging re-exposes integer cents via
`ROUND(total_dollars * 100)::BIGINT` for manifest source-faithfulness. The round-trip is
lossless for the amounts in this dataset (all amounts are whole cents), but if fractional-
cent amounts appeared they'd be silently rounded.

---

## With more time / productionizing

**Orchestration.** Replace `make run` with an Airflow (or Dagster/Prefect) DAG with three
upstream EL tasks fanning into `dbt build`. The Postgres and usage tasks are fast; the
billing task is the bottleneck if the record count grows. Separating them lets the faster
tasks deliver fresh data to dbt without waiting for billing pagination.

**Scheduling.** Postgres + usage: run every hour (low-volume, watermark-based). Billing
API: daily at 06:00 UTC after billing closes the prior day's batch. `fct_revenue`: rebuild
after all three EL tasks complete.

**Alerting.**
- EL: alert if any task produces zero rows for two consecutive runs on a source that has
  historically always had rows (signals watermark stuck or source down).
- dbt: `store_failures = true` on all tests, write failures to a `dbt_test_failures` schema,
  and alert on any non-zero row count from the uniqueness or relationship tests.
- Business: alert if total monthly MRR drops >5% week-over-week (could be a churn spike
  or a pipeline data loss — worth disambiguating quickly).

**Incremental dbt models.** `fct_revenue` currently rebuilds as a full table on every run.
With 18+ months of history it's still under a second, but the right production pattern is
`{{ config(materialized='incremental', unique_key=['account_id','month_start']) }}`
rebuilding only the current and prior month on each run.

**Billing incremental.** Implement a webhook listener (Stripe sends events in real-time)
or use the `created` timestamp with the Stripe `/v1/events` endpoint to pull only new
objects since last run. Store the last-seen event ID as the watermark.

**Data contracts.** Add `dbt source freshness` checks with max-age thresholds on all raw
sources. If Postgres `accounts.updated_at` hasn't advanced in 6 hours, flag it before dbt
runs so stale data doesn't silently propagate.

**WAL-based CDC.** For higher-frequency needs, replace the watermark polling on Postgres
with `pgoutput` logical replication into a streaming pipeline (Debezium → Kafka → DuckDB
or a cloud warehouse). This captures every row version rather than just the latest
`updated_at` state, which matters for subscription changes that could land between polling
windows.

---

## Stretch items

### 1 — `fct_reconciliation`: billed vs collected vs expected

**What it does.** Triangulates three views of the same revenue at an account × calendar-month
grain:

| Column | Source | What it measures |
|---|---|---|
| `expected_mrr` | `fct_revenue` | Model-calculated MRR (seats × price + usage overage) |
| `billed_amount` | `stg_billing__invoices` | Sum of invoice totals where `period_start` falls in that month |
| `collected_amount` | `stg_billing__charges` | Sum of paid charges for those same invoices |

`billed_vs_expected_delta` (billed minus expected) and `collected_vs_billed_delta` (collected
minus billed) surface two distinct failure modes.

**What it reveals.** The model produces 2,927 rows across all account-months. 1,538 rows have
a non-zero `billed_vs_expected_delta` — overwhelmingly driven by annual subscriptions: when an
annual contract invoices, the full-year amount lands in a single month's `billed_amount` while
`fct_revenue` normalises annual prices to monthly (÷12), spreading the same total across 12
months. These rows are not data-quality issues; they reflect a known apples-to-oranges comparison
between cash billing cadence and normalised MRR. Filtering to monthly-billing-interval accounts
collapses most of that gap.

15 rows have a non-zero `collected_vs_billed_delta`. Inspecting them reveals the 15 open invoices
in the billing system (status = `open`) — invoices that were issued but have no associated paid
charge yet. These are legitimate collection-gap candidates worth escalating to finance.

**Attribution convention.** Both billed and collected amounts are attributed to the
`invoice.period_start` month (not the invoice `created_at` or charge `created_at`), matching the
attribution convention used in `fct_revenue` for usage overages. This puts all three columns on
the same time axis. Accounts not resolvable via `int_billing_account_link` are excluded.

---

### 2 — `dim_accounts`: SCD2 account history

**Design.** The SCD2 is built entirely from `stg_app__subscription_changes` and
`stg_app__subscriptions`. No dedicated accounts-status audit table exists, so status is derived:
`'active'` before any cancel event, `'canceled'` for the post-cancel period. The three-CTE union
(initial state / post-change states / no-change accounts) mirrors `int_subscription_periods`.

**Surrogate key.** Rather than `MD5(account_id || valid_from)`, the SK is keyed on
`MD5(account_id || seq)` where `seq` is the `change_id` for post-change rows and `0` for the
initial/no-change row. This is necessary because multiple changes can land on the same calendar
date (e.g. account 273 had an upgrade and a seat change within the same morning on 2026-04-26).
Truncating `effective_at` to DATE would create two rows with identical `valid_from`, making the
natural-key SK non-unique. The `change_id`-based SK guarantees uniqueness regardless of intra-day
timing while remaining fully deterministic across pipeline runs.

**Coverage.** 871 rows across 311 accounts with subscriptions. 86 accounts have no subscription
changes and are represented by a single open-ended row from `started_at`. The remaining 225
accounts have between 1 and 10+ change rows, with `is_current = TRUE` on the most recent period.

**Limitation.** Account-level attributes (country, domain, `signed_up_at`) are static fields
from `stg_app__accounts` and are not tracked as SCD columns here — adding them would require
joining `stg_app__accounts` on `account_id` in the final SELECT since they don't change via
`subscription_changes`. Account status transitions between `active`, `past_due`, and `trial` that
happen without a corresponding subscription change (e.g., payment failure) are not captured;
tracking those would require an accounts audit log that does not currently exist.

---

### 3 — DIY hard-delete detection (`stg_app__users_deleted_diy`)

**Approach.** `el/snapshot_users.py` runs before `extract_postgres.py` on each pipeline cycle and
saves `raw.users → raw.users_snapshot`. After the EL run, the dbt model compares the two:

```
users alive in snapshot (deleted_at IS NULL)
    minus
users alive in current raw.users (id present AND deleted_at IS NULL)
    =
DIY-detected hard deletes
```

The comparison is symmetric with the CDC tombstone approach: a user who was in the snapshot with
`deleted_at IS NULL` but now has `deleted_at IS NOT NULL` (tombstoned by the CDC two-pass logic)
or is completely absent shows up as a deletion in this model too.

**Tradeoffs vs `users_cdc`:**

| Property | DIY snapshot diff | `users_cdc` op='D' |
|---|---|---|
| Deletion timestamp | "sometime between last two runs" | Exact row-level timestamp |
| Intra-run deletions | Invisible (need continuous replication) | Captured if CDC buffer covers the window |
| Infrastructure | No extra services; file-based | Requires `pgoutput` logical replication slot |
| Setup complexity | Low — one Python script | Higher — replication user, WAL config, connector |
| False positives | Possible if row was missing on prior load | None (confirmed by DB engine) |
| Auditable history | Only detects net change per cycle | Full per-row event log |

For the current dataset the DIY model returns 0 rows because the snapshot was taken from the
same state as the current `raw.users`. In a live pipeline, running `el/snapshot_users.py` before
each `extract_postgres.py` means any user removed from Postgres between cycles will be surfaced
on the next run.

**WAL/logical replication (not implemented).** The production approach for hard-delete capture
would be `pgoutput` logical replication via a tool like Debezium:

1. Enable `wal_level = logical` in Postgres config.
2. Create a replication slot: `SELECT pg_create_logical_replication_slot('debezium', 'pgoutput')`.
3. Debezium (running in Docker alongside the app-db) streams row-level events to a Kafka topic
   (`endeavoriq.public.users`). `op=d` events carry the before-image of the deleted row.
4. A Kafka consumer (or Flink job) writes these events into `raw.users_cdc` in DuckDB, matching
   the format already used by `stg_app__users_cdc`.
5. The existing two-pass logic in `extract_postgres.py` (which already handles `op='D'` from the
   CDC table) then tombstones `raw.users` without any change to the dbt layer.

The main constraint: `pg_create_logical_replication_slot` requires superuser or `REPLICATION`
privilege and the Postgres server must be running with `wal_level = logical`
(`ALTER SYSTEM SET wal_level = logical; SELECT pg_reload_conf()`). The simulator in this project
runs Postgres with default settings, so WAL-based CDC was not wired up. The DIY snapshot model
provides equivalent coverage at much lower infrastructure cost for this volume.

---

## How I used AI

I used Claude Code throughout — it wrote all the boilerplate quickly: the pagination loops,
`INSERT OR REPLACE` upsert patterns, DuckDB DDL, and the staging model SQL once I described
what each source looked like. That speed let me spend time on the decisions that actually
matter: the watermark design, how to reconstruct subscription periods from `subscription_changes`,
the double-join for plan price history, and the MRR movement convention choices.

One place I caught and overrode it: Claude's initial design for the billing extract stored
amounts only as dollars (as the task spec said "convert cents to dollars"), which would have
caused a mismatch between my staging `total` column and the source's integer-cent value that
the manifest checker compares against. I recognized the issue and directed it to add
`ROUND(total_dollars * 100)::BIGINT AS total` to the staging models so the manifest-declared
column values match the source. The spec said "convert" for convenience in downstream models —
it didn't mean staging should lose the original unit.

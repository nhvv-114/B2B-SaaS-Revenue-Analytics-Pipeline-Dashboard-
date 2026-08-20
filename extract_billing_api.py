#!/usr/bin/env python3
"""
Extract-Load: Billing REST API → DuckDB raw schema.

Paginates all 4 endpoints using cursor pagination (starting_after).
Converts epoch seconds → TIMESTAMPTZ and integer cents → DOUBLE dollars.
Invoice line items are unnested into raw.billing_invoice_line_items.

The API has no server-side date filtering, so every run paginates all records.
INSERT OR REPLACE on primary key makes this idempotent.
"""

import os
import sys
from datetime import datetime, timezone

import duckdb
import urllib.request
import urllib.parse
import json

BASE_URL = "http://localhost:8080"
PAGE_SIZE = 100

_HERE = os.path.dirname(os.path.abspath(__file__))
DUCK_PATH = os.path.join(os.path.dirname(_HERE), "pipeline.duckdb")
DUCK_SCHEMA = "raw"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cents_to_dollars(cents):
    """Integer cents → float dollars. None-safe."""
    return cents / 100.0 if cents is not None else None


def epoch_to_dt(epoch):
    """Unix epoch seconds → timezone-aware datetime. None-safe."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc) if epoch is not None else None


def api_get(path, params=None):
    url = BASE_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


def paginate(path):
    """Yield all records from a paginated list endpoint."""
    params = {"limit": PAGE_SIZE}
    while True:
        page = api_get(path, params)
        records = page["data"]
        yield from records
        if not page["has_more"]:
            break
        params["starting_after"] = records[-1]["id"]


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

DDL = {
    "billing_customers": """
        CREATE TABLE IF NOT EXISTS raw.billing_customers (
            id                VARCHAR PRIMARY KEY,
            name              VARCHAR,
            email             VARCHAR,
            account_id        VARCHAR,   -- metadata.account_id; NULL when absent
            created_at        TIMESTAMPTZ,
            livemode          BOOLEAN
        )
    """,
    "billing_invoices": """
        CREATE TABLE IF NOT EXISTS raw.billing_invoices (
            id                  VARCHAR PRIMARY KEY,
            customer_id         VARCHAR,
            subscription_id     VARCHAR,
            status              VARCHAR,
            currency            VARCHAR,
            period_start        TIMESTAMPTZ,
            period_end          TIMESTAMPTZ,
            created_at          TIMESTAMPTZ,
            total_dollars       DOUBLE,
            amount_due_dollars  DOUBLE,
            amount_paid_dollars DOUBLE
        )
    """,
    # Unnested from invoices.lines.data. id is the line-item id.
    "billing_invoice_line_items": """
        CREATE TABLE IF NOT EXISTS raw.billing_invoice_line_items (
            id               VARCHAR PRIMARY KEY,
            invoice_id       VARCHAR,
            amount_dollars   DOUBLE,
            currency         VARCHAR,
            description      VARCHAR,
            quantity         INTEGER,
            unit_amount_dollars DOUBLE,   -- price.unit_amount; NULL for usage lines
            billing_interval VARCHAR,     -- price.recurring.interval; NULL for usage lines
            kind             VARCHAR,     -- metadata.kind: 'subscription' | 'usage'
            plan_code        VARCHAR,     -- metadata.plan_code; NULL for usage lines
            metric           VARCHAR      -- metadata.metric; NULL for subscription lines
        )
    """,
    "billing_charges": """
        CREATE TABLE IF NOT EXISTS raw.billing_charges (
            id             VARCHAR PRIMARY KEY,
            invoice_id     VARCHAR,
            customer_id    VARCHAR,
            amount_dollars DOUBLE,
            currency       VARCHAR,
            status         VARCHAR,
            paid           BOOLEAN,
            failure_code   VARCHAR,
            created_at     TIMESTAMPTZ
        )
    """,
    "billing_refunds": """
        CREATE TABLE IF NOT EXISTS raw.billing_refunds (
            id             VARCHAR PRIMARY KEY,
            charge_id      VARCHAR,
            amount_dollars DOUBLE,
            reason         VARCHAR,
            created_at     TIMESTAMPTZ
        )
    """,
}


def ensure_tables(duck):
    for ddl in DDL.values():
        duck.execute(ddl)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_customers(duck):
    rows = []
    for c in paginate("/v1/customers"):
        rows.append((
            c["id"],
            c.get("name"),
            c.get("email"),
            c.get("metadata", {}).get("account_id"),   # sometimes absent
            epoch_to_dt(c.get("created")),
            c.get("livemode"),
        ))

    duck.executemany(
        """
        INSERT OR REPLACE INTO raw.billing_customers
            (id, name, email, account_id, created_at, livemode)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    print(f"  billing_customers:           {len(rows):>6} rows")
    return len(rows)


def load_invoices(duck):
    inv_rows = []
    li_rows = []

    for inv in paginate("/v1/invoices"):
        inv_rows.append((
            inv["id"],
            inv.get("customer"),
            inv.get("subscription"),
            inv.get("status"),
            inv.get("currency"),
            epoch_to_dt(inv.get("period_start")),
            epoch_to_dt(inv.get("period_end")),
            epoch_to_dt(inv.get("created")),
            cents_to_dollars(inv.get("total")),
            cents_to_dollars(inv.get("amount_due")),
            cents_to_dollars(inv.get("amount_paid")),
        ))

        for li in inv.get("lines", {}).get("data", []):
            price = li.get("price") or {}
            recurring = price.get("recurring") or {}
            meta = li.get("metadata") or {}
            li_rows.append((
                li["id"],
                inv["id"],
                cents_to_dollars(li.get("amount")),
                li.get("currency"),
                li.get("description"),
                li.get("quantity"),
                cents_to_dollars(price.get("unit_amount")),
                recurring.get("interval"),
                meta.get("kind"),
                meta.get("plan_code"),
                meta.get("metric"),
            ))

    duck.executemany(
        """
        INSERT OR REPLACE INTO raw.billing_invoices
            (id, customer_id, subscription_id, status, currency,
             period_start, period_end, created_at,
             total_dollars, amount_due_dollars, amount_paid_dollars)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        inv_rows,
    )
    duck.executemany(
        """
        INSERT OR REPLACE INTO raw.billing_invoice_line_items
            (id, invoice_id, amount_dollars, currency, description, quantity,
             unit_amount_dollars, billing_interval, kind, plan_code, metric)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        li_rows,
    )
    print(f"  billing_invoices:            {len(inv_rows):>6} rows")
    print(f"  billing_invoice_line_items:  {len(li_rows):>6} rows")
    return len(inv_rows) + len(li_rows)


def load_charges(duck):
    rows = []
    for c in paginate("/v1/charges"):
        rows.append((
            c["id"],
            c.get("invoice"),
            c.get("customer"),
            cents_to_dollars(c.get("amount")),
            c.get("currency"),
            c.get("status"),
            c.get("paid"),
            c.get("failure_code"),
            epoch_to_dt(c.get("created")),
        ))

    duck.executemany(
        """
        INSERT OR REPLACE INTO raw.billing_charges
            (id, invoice_id, customer_id, amount_dollars, currency,
             status, paid, failure_code, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    print(f"  billing_charges:             {len(rows):>6} rows")
    return len(rows)


def load_refunds(duck):
    rows = []
    for r in paginate("/v1/refunds"):
        rows.append((
            r["id"],
            r.get("charge"),
            cents_to_dollars(r.get("amount")),
            r.get("reason"),
            epoch_to_dt(r.get("created")),
        ))

    duck.executemany(
        """
        INSERT OR REPLACE INTO raw.billing_refunds
            (id, charge_id, amount_dollars, reason, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    print(f"  billing_refunds:             {len(rows):>6} rows")
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Opening DuckDB: {DUCK_PATH}\n")
    duck = duckdb.connect(DUCK_PATH)
    duck.execute(f"CREATE SCHEMA IF NOT EXISTS {DUCK_SCHEMA}")
    ensure_tables(duck)

    print("Loading billing API:")
    total = 0
    total += load_customers(duck)
    total += load_invoices(duck)
    total += load_charges(duck)
    total += load_refunds(duck)

    duck.close()
    print(f"\nDone. {total} rows upserted across 5 tables.")


if __name__ == "__main__":
    main()

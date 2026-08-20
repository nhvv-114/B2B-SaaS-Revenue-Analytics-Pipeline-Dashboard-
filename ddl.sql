-- EndeavorIQ — App OLTP (System A) schema.
-- The product's operational database; the subscription source of truth.
-- See docs/SCHEMA.md §3. created_at/updated_at everywhere; deleted_at for soft deletes.
-- `users` is the exception: hard-deleted, with `users_cdc` as its change log.

CREATE SCHEMA IF NOT EXISTS app;

DROP TABLE IF EXISTS app.users_cdc          CASCADE;
DROP TABLE IF EXISTS app.users              CASCADE;
DROP TABLE IF EXISTS app.subscription_changes CASCADE;
DROP TABLE IF EXISTS app.subscriptions      CASCADE;
DROP TABLE IF EXISTS app.accounts           CASCADE;
DROP TABLE IF EXISTS app.plans              CASCADE;

-- Effective-dated plan catalog. A price change creates a NEW row (same code+interval,
-- new effective_from); is_current flags the live version. One planted price change. SCD source.
CREATE TABLE app.plans (
    id                          integer PRIMARY KEY,
    code                        text NOT NULL,          -- starter | team | business | enterprise
    name                        text NOT NULL,
    billing_interval            text NOT NULL,          -- monthly | annual
    seat_price_cents            integer NOT NULL,       -- per seat, per billing period
    included_api_calls          integer NOT NULL,       -- monthly pool
    overage_api_per_1k_cents    integer NOT NULL,
    included_automations        integer NOT NULL,
    included_storage_gb         integer NOT NULL,
    effective_from              date NOT NULL,
    effective_to                date,                   -- NULL = current version
    is_current                  boolean NOT NULL,
    created_at                  timestamptz NOT NULL,
    updated_at                  timestamptz NOT NULL
);

CREATE TABLE app.accounts (
    id              integer PRIMARY KEY,
    name            text NOT NULL,
    domain          text NOT NULL,
    country         text NOT NULL,                      -- ISO-3166 alpha-2
    status          text NOT NULL,                      -- trial | active | past_due | churned
    signed_up_at    timestamptz NOT NULL,
    trial_ends_at   timestamptz,
    churned_at      timestamptz,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    deleted_at      timestamptz                          -- soft delete (set on churn)
);

CREATE TABLE app.subscriptions (
    id                      integer PRIMARY KEY,
    account_id              integer NOT NULL REFERENCES app.accounts(id),
    plan_id                 integer NOT NULL REFERENCES app.plans(id),
    status                  text NOT NULL,              -- trialing | active | past_due | canceled
    seats                   integer NOT NULL,
    current_period_start    date NOT NULL,
    current_period_end      date NOT NULL,
    started_at              timestamptz NOT NULL,
    canceled_at             timestamptz,
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,        -- moves on seat/plan/status change
    deleted_at              timestamptz
);

-- Explicit change log (candidate may use this OR derive SCD2 from subscriptions history).
CREATE TABLE app.subscription_changes (
    id                  integer PRIMARY KEY,
    subscription_id     integer NOT NULL REFERENCES app.subscriptions(id),
    change_type         text NOT NULL,                  -- upgrade | downgrade | seat_change | cancel | reactivate
    old_plan_id         integer,
    new_plan_id         integer,
    old_seats           integer,
    new_seats           integer,
    effective_at        timestamptz NOT NULL,
    created_at          timestamptz NOT NULL
);

-- Seats within an account. NOTE: NO deleted_at — rows are HARD-deleted (GDPR erasure).
CREATE TABLE app.users (
    id              integer PRIMARY KEY,
    account_id      integer NOT NULL REFERENCES app.accounts(id),
    email           text NOT NULL,
    full_name       text NOT NULL,
    role            text NOT NULL,                      -- admin | member | viewer
    status          text NOT NULL,                      -- active | invited | deactivated
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

-- Pre-built CDC/audit log for users. Captures I/U/D so hard-deletes are recoverable.
CREATE TABLE app.users_cdc (
    seq         bigint PRIMARY KEY,                     -- monotonic; ordering guaranteed
    op          char(1) NOT NULL,                       -- I | U | D
    user_id     integer NOT NULL,
    account_id  integer,
    email       text,
    full_name   text,
    role        text,
    status      text,
    changed_at  timestamptz NOT NULL
);

CREATE INDEX ix_accounts_updated_at      ON app.accounts(updated_at);
CREATE INDEX ix_subscriptions_account    ON app.subscriptions(account_id);
CREATE INDEX ix_subscriptions_updated_at ON app.subscriptions(updated_at);
CREATE INDEX ix_users_account            ON app.users(account_id);
CREATE INDEX ix_users_cdc_changed_at     ON app.users_cdc(changed_at);
CREATE INDEX ix_subchanges_subscription  ON app.subscription_changes(subscription_id);

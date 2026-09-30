-- Tally Postgres Schema (16+)
-- Append-only aggregates, durable dedup, transactional outbox, and atomic checkpoints.

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id     TEXT PRIMARY KEY,
    plan          TEXT NOT NULL,
    quotas        JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);

-- Append-only: a correction for late arrivals is a NEW row with revision+1, NEVER an UPDATE.
CREATE TABLE IF NOT EXISTS aggregates (
    tenant_id     TEXT NOT NULL,
    metric        TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    quantity      NUMERIC NOT NULL,
    event_count   BIGINT NOT NULL,
    revision      INT NOT NULL DEFAULT 0,
    sealed_at     TIMESTAMPTZ,
    computed_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, metric, window_start, revision)
);

CREATE INDEX IF NOT EXISTS idx_aggregates_lookup 
ON aggregates (tenant_id, metric, window_start, revision DESC);

-- Durable dedup tier (Tier 3). The unique primary key constraint IS the guarantee.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL
);

-- Transactional outbox: written in the SAME transaction as the aggregate.
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_key TEXT NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    published_at  TIMESTAMPTZ,
    attempts      INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished 
ON outbox (published_at) WHERE published_at IS NULL;

-- Consumer state. Offsets and state commit ATOMICALLY in the same transaction as aggregates.
CREATE TABLE IF NOT EXISTS checkpoints (
    consumer      TEXT NOT NULL,
    partition     INT NOT NULL,
    offset_       BIGINT NOT NULL,
    state         JSONB NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer, partition)
);

-- Events that arrived after their window sealed. Audited for reconciliation.
CREATE TABLE IF NOT EXISTS late_arrivals (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    metric        TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    quantity      NUMERIC NOT NULL,
    lateness_ms   BIGINT NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_late_arrivals_window 
ON late_arrivals (tenant_id, metric, window_start);

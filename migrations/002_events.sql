-- Event-sourced foundation: append-only log + transactional outbox.
-- The `claims` table (001) is now a projection folded from these events.

CREATE TABLE IF NOT EXISTS claim_events (
    event_id    TEXT PRIMARY KEY,
    claim_id    TEXT NOT NULL,
    seq         INT  NOT NULL,
    type        TEXT NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (claim_id, seq)
);
CREATE INDEX IF NOT EXISTS claim_events_claim_idx ON claim_events (claim_id, seq);

CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL PRIMARY KEY,
    event_id   TEXT NOT NULL UNIQUE,
    topic      TEXT NOT NULL,
    claim_id   TEXT NOT NULL,
    payload    TEXT NOT NULL,
    published  BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE NOT published;

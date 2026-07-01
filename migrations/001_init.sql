-- Application DB schema (separate from Temporal's own persistence store).

CREATE TABLE IF NOT EXISTS claims (
    claim_id        TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    callback_url    TEXT NOT NULL,
    metadata        JSONB NOT NULL,
    pdf_ref         TEXT,
    ocr_ref         TEXT,
    idempotency_key TEXT UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS model_outputs (
    claim_id      TEXT NOT NULL,
    model_name    TEXT NOT NULL,
    model_version TEXT NOT NULL,
    output        JSONB NOT NULL,
    confidence    DOUBLE PRECISION NOT NULL,
    latency_ms    INT NOT NULL DEFAULT 0,
    tokens_cost   INT NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (claim_id, model_name, model_version)
);

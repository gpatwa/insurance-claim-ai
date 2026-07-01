-- M10/M11: adjudication decision + review work queue on the claims projection.

ALTER TABLE claims ADD COLUMN IF NOT EXISTS decision TEXT;
ALTER TABLE claims ADD COLUMN IF NOT EXISTS reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb;

-- review work queue: adjudicated claims awaiting a human
CREATE INDEX IF NOT EXISTS claims_pending_review_idx
    ON claims (updated_at) WHERE decision = 'PEND' AND status = 'ADJUDICATED';

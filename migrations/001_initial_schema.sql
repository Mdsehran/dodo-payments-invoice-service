-- ============================================================
-- Dodo Payments — Initial Schema
-- All monetary values stored as BIGINT cents (never floats)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Businesses (tenants)
CREATE TABLE businesses (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- API Keys (hashed, scoped to a business)
CREATE TABLE api_keys (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id  UUID NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    key_hash     TEXT NOT NULL UNIQUE,     -- SHA-256 hex of raw key
    key_prefix   TEXT NOT NULL,            -- first 8 chars for identification
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at   TIMESTAMPTZ
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL;

-- Customers (scoped to a business)
CREATE TABLE customers (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id  UUID NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    email        TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(business_id, email)
);

CREATE INDEX idx_customers_business ON customers(business_id);

-- Invoices
CREATE TABLE invoices (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id     UUID NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    customer_id     UUID NOT NULL REFERENCES customers(id),
    state           TEXT NOT NULL DEFAULT 'draft'
                        CHECK (state IN ('draft','open','processing','paid','void','uncollectible')),
    total_cents     BIGINT NOT NULL DEFAULT 0 CHECK (total_cents >= 0),
    due_date        DATE NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_invoices_business_state ON invoices(business_id, state);
CREATE INDEX idx_invoices_customer       ON invoices(customer_id);

-- Invoice line items (server computes total from these — never trust client total)
CREATE TABLE invoice_line_items (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_id        UUID NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    description       TEXT NOT NULL,
    quantity          INT NOT NULL CHECK (quantity > 0),
    unit_amount_cents BIGINT NOT NULL CHECK (unit_amount_cents > 0)
);

-- Payment attempts
CREATE TABLE payment_attempts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_id       UUID NOT NULL REFERENCES invoices(id),
    idempotency_key  TEXT NOT NULL,
    card_token       TEXT NOT NULL,
    state            TEXT NOT NULL DEFAULT 'pending'
                         CHECK (state IN ('pending','succeeded','failed')),
    psp_ref          TEXT,                  -- PSP's reference ID on success
    failure_code     TEXT,                  -- e.g. "insufficient_funds"
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payment_attempts_invoice ON payment_attempts(invoice_id);
CREATE UNIQUE INDEX idx_payment_idempotency ON payment_attempts(idempotency_key);

-- Idempotency key store (prevents duplicate PSP charges on retry)
CREATE TABLE idempotency_keys (
    idempotency_key  TEXT NOT NULL,
    business_id      UUID NOT NULL,
    request_hash     TEXT NOT NULL,          -- hash of method+path+body
    response_status  INT NOT NULL,
    response_body    JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (idempotency_key, business_id)
);

-- Webhook endpoints registered by businesses
CREATE TABLE webhook_endpoints (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id  UUID NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    url          TEXT NOT NULL,
    secret       TEXT NOT NULL,             -- HMAC signing secret (stored plaintext — scoped per endpoint)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhook_endpoints_business ON webhook_endpoints(business_id);

-- Webhook delivery queue (decoupled from request path)
CREATE TABLE webhook_deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id     UUID NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL,          -- e.g. "invoice.paid"
    payload         JSONB NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending'
                        CHECK (state IN ('pending','delivered','failed','dead')),
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhook_deliveries_pending ON webhook_deliveries(next_attempt_at)
    WHERE state = 'pending';

-- Auto-update updated_at on invoices
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER invoices_updated_at
    BEFORE UPDATE ON invoices
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER payment_attempts_updated_at
    BEFORE UPDATE ON payment_attempts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
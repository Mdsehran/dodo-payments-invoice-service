# DESIGN.md — Dodo Payments Invoice & Payment Service

> **Author:** Mohammed Sehran  
> **Role:** Backend Engineer (Rust) — Assessment Submission  
> **Stack:** Rust · Axum 0.7 · PostgreSQL 16 · sqlx · Tokio · Docker

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Model](#2-data-model)
3. [Invoice State Machine](#3-invoice-state-machine)
4. [Payment Processing — Step by Step](#4-payment-processing--step-by-step)
5. [Concurrency — How Two Simultaneous Payments Are Handled](#5-concurrency--how-two-simultaneous-payments-are-handled)
6. [Idempotency Design](#6-idempotency-design)
7. [PSP Timeout Handling](#7-psp-timeout-handling)
8. [Failure Modes and Recovery](#8-failure-modes-and-recovery)
9. [Webhook Delivery](#9-webhook-delivery)
10. [Authentication and API Key Design](#10-authentication-and-api-key-design)
11. [Error Handling](#11-error-handling)
12. [What I Cut and Why](#12-what-i-cut-and-why)
13. [Production Gaps — Honest Assessment](#13-production-gaps--honest-assessment)

---

## 1. System Overview

The service is a backend billing engine. Merchants use it to create invoices,
collect payments, and receive real-time webhook events about what happened.

Three components run together via `docker-compose`:

```
┌──────────────────────────────────────────────────────────────────┐
│  docker-compose                                                  │
│                                                                  │
│  ┌─────────────────────┐        ┌──────────────┐                │
│  │   invoice_service   │        │  PostgreSQL   │                │
│  │   Rust / Axum :3000 │◀──────▶│     :5432     │                │
│  │                     │  sqlx  └──────────────┘                │
│  │  ┌───────────────┐  │                                        │
│  │  │ webhook_worker│  │  tokio::spawn — background task        │
│  │  │  (bg task)    │  │  polls webhook_deliveries every 5s     │
│  │  └───────────────┘  │                                        │
│  │                     │        ┌──────────────┐                │
│  │  reqwest client     │───────▶│   mock_psp   │                │
│  │  timeout = 10s      │        │  Rust :3001  │                │
│  └─────────────────────┘        └──────────────┘                │
└──────────────────────────────────────────────────────────────────┘
```

The `invoice_service` is the only externally exposed binary.
The `mock_psp` simulates a real payment processor with five deterministic outcomes.
PostgreSQL is the single source of truth for all state.

---

## 2. Data Model

### Design principles

- All monetary values are stored as `BIGINT` (integer cents). No `FLOAT`. No `DECIMAL`.  
  `4900` means $49.00. This is the only correct way to handle money in a billing system.
- The server always recomputes `total_cents` from `invoice_line_items`. Whatever the client
  sends as a total is ignored. A client cannot manipulate its own invoice amount.
- State constraints live in the database, not only in application code.
  The `CHECK` constraint on `invoices.state` means an invalid state can never be written —
  even if application code has a bug.
- Soft-delete pattern for API keys: `revoked_at` timestamp preserves audit trail.

### Schema

```sql
-- Top-level tenant
CREATE TABLE businesses (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Hashed API keys, scoped to one business
CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id UUID NOT NULL REFERENCES businesses(id),
    key_hash    TEXT NOT NULL UNIQUE,   -- SHA-256 of raw key, never plaintext
    key_prefix  TEXT NOT NULL,          -- first segment, for human identification
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at  TIMESTAMPTZ             -- soft-delete, preserves audit trail
);

-- Fast auth lookup on every request — partial index covers only active keys
CREATE INDEX idx_api_keys_hash
    ON api_keys(key_hash)
    WHERE revoked_at IS NULL;

-- Customers, scoped to a business
CREATE TABLE customers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id UUID NOT NULL REFERENCES businesses(id),
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(business_id, email)          -- no duplicate emails per merchant
);

-- Invoices — state is a hard database constraint
CREATE TABLE invoices (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id UUID NOT NULL REFERENCES businesses(id),
    customer_id UUID NOT NULL REFERENCES customers(id),
    state       TEXT NOT NULL DEFAULT 'draft'
                    CHECK (state IN ('draft','open','processing','paid','void','uncollectible')),
    total_cents BIGINT NOT NULL DEFAULT 0 CHECK (total_cents >= 0),
    due_date    DATE NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_invoices_business_state ON invoices(business_id, state);

-- Line items — server computes total from these on every write
CREATE TABLE invoice_line_items (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_id        UUID NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    description       TEXT NOT NULL,
    quantity          INT NOT NULL CHECK (quantity > 0),
    unit_amount_cents BIGINT NOT NULL CHECK (unit_amount_cents > 0)
);

-- One row per payment attempt — idempotency_key is unique
CREATE TABLE payment_attempts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_id       UUID NOT NULL REFERENCES invoices(id),
    idempotency_key  TEXT NOT NULL,
    card_token       TEXT NOT NULL,
    state            TEXT NOT NULL DEFAULT 'pending'
                         CHECK (state IN ('pending','succeeded','failed')),
    psp_ref          TEXT,           -- PSP's charge reference on success
    failure_code     TEXT,           -- PSP's failure code on decline
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_payment_idempotency ON payment_attempts(idempotency_key);

-- Idempotency cache — stores the full response so replays return the same result
CREATE TABLE idempotency_keys (
    idempotency_key  TEXT NOT NULL,
    business_id      UUID NOT NULL,
    request_hash     TEXT NOT NULL,      -- SHA-256 of request body, detects body changes
    response_status  INT NOT NULL,
    response_body    JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (idempotency_key, business_id)
);

-- Webhook endpoints registered per business
CREATE TABLE webhook_endpoints (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id UUID NOT NULL REFERENCES businesses(id),
    url         TEXT NOT NULL,
    secret      TEXT NOT NULL,           -- per-endpoint HMAC signing secret
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Persistent outbox queue for webhook delivery
CREATE TABLE webhook_deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id     UUID NOT NULL REFERENCES webhook_endpoints(id),
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending'
                        CHECK (state IN ('pending','delivered','failed','dead')),
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Worker uses this to find pending deliveries without full table scan
CREATE INDEX idx_webhook_deliveries_pending
    ON webhook_deliveries(next_attempt_at)
    WHERE state = 'pending';
```

### Key index decisions

**`idx_api_keys_hash` is a partial index** (`WHERE revoked_at IS NULL`). Every HTTP request
to a protected route hashes the Bearer token and hits this index. Keeping it narrow —
only active keys — means it stays in memory even at high key volume.

**`idx_invoices_business_state`** is a composite index on `(business_id, state)`.
This powers `GET /invoices?state=open` without a sequential scan.

**`idx_webhook_deliveries_pending`** is a partial index on `next_attempt_at`
filtered to `state = 'pending'`. The worker polls this every 5 seconds.
A full-table index here would be wasteful — most rows are `delivered`.

### Auto-managed timestamps

```sql
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
```

`updated_at` is managed by a trigger, not application code. Application code cannot
forget to update it. This matters for the reconciliation job that detects stale
`PROCESSING` invoices by looking at `updated_at`.

---

## 3. Invoice State Machine

```
                   ┌────────────────────────────────────────────┐
                   │                                            │
        [POST /invoices]                                        │
                   │                                            │
                   ▼                                            │
              ┌─────────┐                                       │
              │  DRAFT  │──[POST /void]──────────────────────▶ VOID
              └─────────┘                                  (terminal)
                   │
          [POST /invoices/:id/open]
                   │
                   ▼
              ┌─────────┐
              │  OPEN   │──[POST /void]──────────────────────▶ VOID
              └─────────┘──[mark-uncollectible]────────────▶ UNCOLLECTIBLE
                   │                                       (terminal)
     [POST /invoices/:id/pay — atomic UPDATE claim]
                   │
                   ▼
           ┌─────────────┐
           │ PROCESSING  │──[PSP timeout, 10s client limit]──▶ PROCESSING
           └─────────────┘                                     (stays, 202 returned)
                   │
         ┌─────────┴──────────┐
         │                    │
     [PSP ok]           [PSP failed /
         │                network error]
         ▼                    ▼
   ┌──────────┐          ┌──────────┐
   │   PAID   │          │   OPEN   │  (re-opened — customer can retry)
   │(terminal)│          └──────────┘
   └──────────┘
```

### Transition rules

| From | To | Trigger | SQL mechanism |
|---|---|---|---|
| `draft` | `open` | `POST /invoices/:id/open` | `UPDATE WHERE state='draft'` |
| `draft` | `void` | `POST /invoices/:id/void` | `UPDATE WHERE state IN ('draft','open')` |
| `open` | `processing` | `POST /invoices/:id/pay` | `UPDATE WHERE state='open' RETURNING id` |
| `open` | `void` | `POST /invoices/:id/void` | `UPDATE WHERE state IN ('draft','open')` |
| `processing` | `paid` | PSP returns `succeeded` | `UPDATE` inside transaction |
| `processing` | `open` | PSP returns `failed` or network error | `UPDATE` inside transaction |
| `paid` | — | (terminal) | Any `UPDATE` returns 0 rows → 422 |
| `void` | — | (terminal) | Any `UPDATE` returns 0 rows → 422 |

Every transition uses a status-conditional `UPDATE ... WHERE state = $expected RETURNING id`.
If 0 rows are returned, the state was wrong — the handler queries current state to return
a precise error message, then returns `422 INVALID_TRANSITION`.

---

## 4. Payment Processing — Step by Step

```
POST /invoices/:id/pay
{
  "card_token": "tok_success"
}
Headers:
  Authorization: Bearer sk_live_...
  Idempotency-Key: idem-abc123
```

**Step 1 — Auth middleware**

The Bearer token is SHA-256 hashed. The hash is looked up in `api_keys`
using the partial index. If not found or `revoked_at IS NOT NULL` → `401`.
The `business_id` is injected into the Axum request extensions.
Every downstream handler gets it automatically — no handler ever trusts
a client-supplied `business_id`.

**Step 2 — Idempotency check**

```sql
SELECT request_hash, response_status, response_body
FROM idempotency_keys
WHERE idempotency_key = $1 AND business_id = $2
```

- Not found → proceed normally.
- Found, `request_hash` matches → return cached response. No PSP call.
- Found, `request_hash` does not match → `409 IDEMPOTENCY_CONFLICT`.
  The client reused a key with different data. This is always a client error.

**Step 3 — Atomic invoice claim**

```sql
UPDATE invoices
SET state = 'processing'
WHERE id = $1
  AND business_id = $2
  AND state = 'open'
RETURNING id
```

If 0 rows returned → invoice is not open. Query current state, return `422`.
If 1 row returned → this request owns the payment attempt. No other concurrent
request can succeed this same statement — PostgreSQL row-level locking guarantees it.

**Step 4 — Record pending attempt**

```sql
INSERT INTO payment_attempts
  (invoice_id, idempotency_key, card_token, state)
VALUES ($1, $2, $3, 'pending')
RETURNING *
```

The attempt is recorded before the PSP call. If the service crashes after this
insert but before the PSP call, the attempt is `pending` and the invoice is
`processing`. The reconciliation job handles this.

**Step 5 — PSP call with client timeout**

```rust
let client = reqwest::Client::builder()
    .timeout(Duration::from_secs(10))
    .build()?;
```

The PSP call happens outside any database transaction. Holding a DB transaction
open across a network call is a lock-escalation antipattern — it keeps a connection
occupied and holds any relevant locks for the entire PSP round-trip.

**Step 6 — Handle PSP response**

| PSP outcome | Payment attempt | Invoice |
|---|---|---|
| `succeeded` | `state = succeeded`, `psp_ref` stored | `state = paid` |
| `failed` (any code) | `state = failed`, `failure_code` stored | `state = open` |
| Timeout (10s) | stays `pending` | stays `processing` |
| Network error | `state = failed`, `failure_code = network_error` | `state = open` |

**Step 7 — Commit final state**

```sql
BEGIN;
  UPDATE payment_attempts SET state = $1, psp_ref = $2, failure_code = $3 WHERE id = $4;
  UPDATE invoices SET state = $1 WHERE id = $2;
COMMIT;
```

Both rows update atomically. If the commit fails, both rows are unchanged —
the invoice is still `processing` and the attempt is still `pending`.
The reconciliation job detects this.

**Step 8 — Enqueue webhook, store idempotency cache**

Webhook is enqueued by inserting into `webhook_deliveries` — non-blocking.
Idempotency response is stored with the final HTTP status code and body.
Future replays with the same key return the cached response immediately.

---

## 5. Concurrency — How Two Simultaneous Payments Are Handled

This is the most important correctness property of the payment system.

### The problem

Two clients call `POST /invoices/:id/pay` at the same millisecond.
Without protection, both could proceed to the PSP, resulting in two charges.

### The solution — status-conditional UPDATE

```sql
UPDATE invoices
SET state = 'processing'
WHERE id = $1 AND state = 'open'
RETURNING id
```

PostgreSQL executes this as a single atomic operation. The row-level lock
is held only for the duration of this one SQL statement. Exactly one
concurrent caller gets `1 row updated`. Every other concurrent caller
gets `0 rows updated` and returns `422 INVALID_TRANSITION` immediately.

### Why not SELECT FOR UPDATE?

```sql
-- This approach has a serious problem
BEGIN;
SELECT id, state FROM invoices WHERE id = $1 FOR UPDATE; -- lock acquired here
-- application checks state
-- calls PSP -- can take up to 10 seconds
-- lock held the entire time
UPDATE invoices SET state = 'processing' WHERE id = $1;
COMMIT; -- lock released here
```

`SELECT FOR UPDATE` locks the row before the PSP call and holds that lock
for the entire duration — up to 10 seconds with our client timeout.
Every other concurrent request queues behind that database lock.
Under load, this creates a bottleneck. The wait time grows with concurrency.

The status-conditional `UPDATE` holds a lock for one SQL statement — microseconds.
Concurrent losers fail in milliseconds, not after waiting in a lock queue.

### Timeline

```
t=0ms   Request A: UPDATE WHERE state='open' → 1 row  ✓  (claims invoice)
t=8ms   Request B: UPDATE WHERE state='open' → 0 rows ✗  (422, fails immediately)

t=0ms   Request A: calls PSP (up to 10s)
t=198ms Request A: PSP returns succeeded
t=198ms Request A: UPDATE invoices SET state='paid' — COMMIT

Result: exactly one charge, Request B rejected in 5ms, no queue
```

---

## 6. Idempotency Design

### What the idempotency key guarantees

A client sends `Idempotency-Key: idem-abc123` with every payment request.
The service guarantees that for any given `(idempotency_key, business_id)` pair:

- The first request is processed normally.
- Any subsequent request with the same key and same body receives the exact same
  response — without triggering a second PSP call.
- A subsequent request with the same key but a different body receives `409 CONFLICT`.

### Request hashing

The request body is hashed with SHA-256 before the idempotency lookup.
This hash is stored alongside the cached response.

```
request_hash = SHA-256(card_token)
```

On replay: `stored_hash == incoming_hash` → cache hit, return stored response.  
On replay: `stored_hash != incoming_hash` → 409 IDEMPOTENCY_CONFLICT.

### Why hash the body instead of storing it

Storing the raw request body has two problems: storage cost scales with body size,
and comparing large JSONB blobs is slower than comparing fixed-length hash strings.
A SHA-256 hash is 64 hex characters — constant size, O(1) comparison.

### Idempotency on the timeout case

When the PSP times out, we cache the idempotency response as `202 Accepted` with
a `poll_url`. If the client retries with the same key, they get the `202` back —
not a new payment attempt. This is important: the invoice is still `PROCESSING`.
A retry that bypassed the idempotency cache would hit the atomic UPDATE,
get 0 rows back (invoice is not `open`), and return `422` — which would confuse
the client into thinking the payment failed when it actually might have succeeded.

---

## 7. PSP Timeout Handling

### The scenario

`tok_timeout` causes the mock PSP to sleep for 30 seconds.
Our `reqwest` client times out after 10 seconds.

### Why we do not mark the payment as failed on timeout

A timeout means the network call did not return — not that the charge failed.
The PSP may have processed the charge and the response was lost in transit.
If we mark the payment as failed and reopen the invoice, the customer
retries, and we call the PSP again — that is potentially a second charge.

### What we do instead

```
PSP call times out at 10s
  → invoice remains in PROCESSING
  → payment_attempt remains pending
  → idempotency key cached as 202
  → API returns 202 Accepted:

{
  "status": "pending",
  "payment_attempt_id": "att_...",
  "invoice_id": "inv_...",
  "message": "Payment is being processed by the acquirer. Poll GET /invoices/{id} to get the final state.",
  "poll_url": "http://localhost:3000/invoices/inv_..."
}
```

### Reconciliation job (documented, not in MVP scope)

A background job runs every 15 minutes and queries:

```sql
SELECT id, payment_attempts.id as att_id, card_token, psp_ref
FROM invoices
JOIN payment_attempts ON payment_attempts.invoice_id = invoices.id
WHERE invoices.state = 'processing'
  AND invoices.updated_at < NOW() - INTERVAL '15 minutes'
```

For each stale invoice, it re-queries the PSP using the payment attempt's
`psp_ref` to determine the actual outcome and updates both rows accordingly.
The mock PSP is deterministic — the same token always produces the same outcome —
so the re-query is safe and will not trigger a second charge.

---

## 8. Failure Modes and Recovery

### Crash after PSP success, before database commit

```
1. Invoice claimed → PROCESSING  (written)
2. Payment attempt inserted       (written)
3. PSP called → succeeded         (PSP charged the card)
4. <<< CRASH >>> before COMMIT
```

On service restart, the invoice is stuck in `PROCESSING` and the attempt is `pending`.
The client retries with the same `Idempotency-Key`:
- Idempotency lookup finds nothing (the response was never cached).
- Atomic UPDATE tries `WHERE state = 'open'` → 0 rows (invoice is `PROCESSING`).
- Returns `422 INVALID_TRANSITION`.

The reconciliation job detects the stale `PROCESSING` invoice after 15 minutes,
re-queries the PSP using the `psp_ref`, gets `succeeded`, and finalises both rows.

**No double charge.** The PSP was charged once. The re-query is a status check,
not a new charge request.

### Full failure coverage table

| Failure scenario | Detection | Outcome |
|---|---|---|
| Concurrent double payment | Atomic UPDATE returns 0 rows | Second request → 422, immediate |
| PSP timeout (slow acquirer) | reqwest 10s client timeout | 202 Accepted, invoice stays PROCESSING |
| PSP network drop | `hyper::Error(Io)` | Rollback → invoice OPEN, no charge |
| PSP returns failure code | `status != succeeded` | Rollback → invoice OPEN, code stored |
| Service crash before DB commit | `updated_at` trigger + reconciliation job | Re-query PSP, finalise state |
| Idempotency replay (same body) | `request_hash` match | Cached response, no PSP call |
| Idempotency replay (different body) | `request_hash` mismatch | 409 IDEMPOTENCY_CONFLICT |
| Void on terminal state | Conditional UPDATE returns 0 rows | 422 INVALID_TRANSITION |
| Invalid API key | SHA-256 hash miss | 401 UNAUTHORIZED |
| Revoked API key | `revoked_at IS NOT NULL` | 401 UNAUTHORIZED |
| Cross-tenant data access | `business_id` on every query | Returns 404 (not found for that business) |
| Client-supplied total manipulation | Server recomputes from line items | Client total silently ignored |

---

## 9. Webhook Delivery

### Architecture — persistent outbox

Webhook delivery is fully decoupled from the API response path.

```
POST /invoices/:id/pay
  → PSP succeeds
  → INSERT INTO webhook_deliveries (state='pending')  ← non-blocking, < 1ms
  → return HTTP 200 to client

Background Tokio task (every 5s):
  → SELECT ... FROM webhook_deliveries WHERE state='pending'
       AND next_attempt_at <= NOW()
       FOR UPDATE SKIP LOCKED          ← safe for multiple worker instances
  → POST to merchant webhook URL
  → on HTTP 2xx: UPDATE state='delivered'
  → on failure: UPDATE attempts++, next_attempt_at = NOW() + delay
```

`FOR UPDATE SKIP LOCKED` is critical — it prevents two worker instances from
picking up and delivering the same event twice. Rows locked by one worker are
skipped by the second, not blocked.

### Signing scheme

```
message   = "{unix_timestamp}.{raw_json_body}"
signature = HMAC-SHA256(endpoint_secret, message)
header    = X-Dodo-Signature: t={timestamp},v1={hex_signature}
```

The timestamp is included in both the signed message and the header.
Receivers can verify by recomputing the signature and rejecting events
where `|now - timestamp| > 300 seconds`. This prevents replay attacks —
a captured valid signature cannot be resent 10 minutes later.

Each endpoint has its own signing secret. A leaked secret only compromises
that one endpoint, not the entire webhook system.

### Retry schedule

| Attempt | Delay after previous failure |
|---|---|
| 1 | Immediate |
| 2 | 10 seconds |
| 3 | 30 seconds |
| 4 | 2 minutes |
| 5 | 10 minutes |

After 5 failed attempts, `state` is set to `dead` and `last_error` is stored.
Dead deliveries are queryable via `GET /webhooks/failed` so merchants can
review and manually replay if needed.

### Events fired

| Event | When |
|---|---|
| `customer.created` | `POST /customers` succeeds |
| `invoice.created` | `POST /invoices` succeeds |
| `invoice.paid` | PSP returns `succeeded` |
| `invoice.payment_failed` | PSP returns `failed` or network/timeout error |

---

## 10. Authentication and API Key Design

### Key generation

```rust
let random_bytes: Vec<u8> = (0..32).map(|_| rand::thread_rng().gen()).collect();
let raw_key = format!("sk_live_{}", hex::encode(random_bytes));
// 32 bytes = 256 bits of entropy
// Transmitted once to the merchant, never stored in plaintext
```

### Key storage

```rust
let hash = hex::encode(Sha256::digest(raw_key.as_bytes()));
// Store only the hash in api_keys.key_hash
// Store the prefix (e.g. "sk_live_nxf") in api_keys.key_prefix for identification
```

The raw key is shown to the merchant once at creation. After that, only the
SHA-256 hash lives in the database. Even a full database dump exposes no usable keys.

### Why SHA-256 and not bcrypt or Argon2

bcrypt and Argon2 are designed to slow down brute-force attacks on low-entropy
secrets like passwords. Our keys are 256-bit random tokens. At that entropy level,
brute force is not a viable attack regardless of hash speed — the search space
is `2^256`. SHA-256 is correct here, and it adds no perceptible latency to auth.

Using bcrypt would add 200–300ms of CPU to every authenticated request.
At 100 requests/second, that is 20–30 CPU cores dedicated to hashing alone.

### Revocation

Setting `revoked_at = NOW()` is enough. The partial index on auth lookups
filters `WHERE revoked_at IS NULL`, so revoked keys fail immediately.
The key row is preserved for audit purposes.

### Tenant isolation

Every query in every handler includes `AND business_id = $auth_business_id`.
There is no code path where a request authenticated as business A can read
or modify data belonging to business B. The auth middleware injects `business_id`
into request extensions — handlers never accept `business_id` from the client.

---

## 11. Error Handling

All errors return a consistent JSON structure:

```json
{
  "error": {
    "code": "INVALID_TRANSITION",
    "message": "Cannot pay invoice in 'paid' state — must be 'open'"
  }
}
```

The `AppError` enum maps to HTTP status codes in a single `IntoResponse` implementation:

| AppError variant | HTTP status |
|---|---|
| `Unauthorized` | 401 |
| `NotFound(msg)` | 404 |
| `Conflict(msg)` | 409 |
| `IdempotencyConflict` | 409 |
| `InvalidTransition(msg)` | 422 |
| `Validation(msg)` | 400 |
| `Database(sqlx::Error)` | 500 (internal, logged) |
| `Internal(msg)` | 500 |

Database errors are logged server-side with full detail but return a generic
`"Internal error"` message to the client. Exposing raw database errors to
clients is a security risk.

---

## 12. What I Cut and Why

**Refunds**  
Refunds require a `refunds` table, a `refunded` invoice state, reverse PSP calls,
and proration logic for partial refunds. The assessment explicitly excludes this.
The `psp_ref` column on `payment_attempts` stores the PSP's charge reference,
which is exactly what a reverse call would need. The extension point is there.

**Subscriptions and recurring billing**  
Entirely separate domain — subscription plans, billing intervals, proration,
trial periods. Not in scope.

**`GET /invoices/:id/open` as an explicit finalize step**  
I implemented this as `POST /invoices/:id/open` rather than making invoices
immediately payable at creation. This matches real-world billing systems
where an invoice is reviewed before being sent to the customer.

**Rate limiting**  
`tower-governor` middleware keyed on `business_id` is the right solution.
Not implemented in this submission but documented in section 13.

**Pagination on list endpoints**  
`GET /invoices` returns all invoices. At scale, this needs cursor-based
or offset pagination. Omitted for MVP scope.

**Audit log**  
A regulatory requirement in most payment jurisdictions — an append-only record
of who did what and when. Not implemented but documented as a gap.

---

## 13. Production Gaps — Honest Assessment

| Gap | Correct production solution |
|---|---|
| No rate limiting | `tower-governor` middleware, keyed on `business_id`, ~100 req/s limit |
| No audit log | Append-only `audit_log` table: `(business_id, actor_key_id, action, entity_id, timestamp)` |
| No pagination | Cursor-based pagination on all list endpoints |
| No reconciliation job | Tokio interval task or external cron querying stale `PROCESSING` invoices |
| Webhook worker in same process | Move to SQS/Redis Streams at production scale for durability |
| Secrets in environment variables | AWS Secrets Manager or HashiCorp Vault |
| No distributed tracing | `tracing-opentelemetry` + Jaeger/Tempo, trace IDs on all log lines |
| No metrics | Prometheus counters for payment success rate, PSP latency, webhook delivery rate |
| UUID v4 primary keys | UUID v7 (time-ordered) or ULID at high insert volume to reduce B-tree fragmentation |

These are not unknown unknowns. They are known trade-offs made to keep the
submission focused on the core correctness properties the assessment evaluates.

---

*Dodo Payments — Backend Engineer (Rust) Assessment · Mohammed Sehran · 2026*

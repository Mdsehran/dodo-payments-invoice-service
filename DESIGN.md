# DESIGN.md — Dodo Payments Invoice & Payment Service

## 1. Data Model

### Tables

**businesses** — top-level tenant. All data is scoped to a business via `business_id` FK.

**api_keys** — stores SHA-256 hash of the raw key (never plaintext). Prefix kept for identification ("which key was this?"). Revoked via `revoked_at` timestamp rather than deletion, preserving audit trail.

**customers** — `(business_id, email)` unique constraint prevents duplicates per tenant.

**invoices** — `state` is a constrained TEXT column with a CHECK constraint. `total_cents` is `BIGINT` — no floats anywhere. Server recomputes total from line items on every write; client-supplied totals are ignored.

**invoice_line_items** — separate table (not a JSONB array) to allow clean aggregation and future line-item-level reporting.

**payment_attempts** — one row per attempt. Idempotency key stored here as a unique index so duplicate inserts fail at DB level.

**idempotency_keys** — caches `(key, business_id, request_hash, response_body)`. Keyed on `(idempotency_key, business_id)`. `request_hash` detects same-key-different-body conflicts (→ 409).

**webhook_endpoints** — stores signing secret per endpoint. Businesses register as many endpoints as they want.

**webhook_deliveries** — acts as an outbox/queue. Delivery happens asynchronously; API responses never wait on it.

### Indexes

- `api_keys.key_hash` — partial index (`WHERE revoked_at IS NULL`) for fast auth on every request
- `invoices(business_id, state)` — powers `GET /invoices?state=open`
- `webhook_deliveries(next_attempt_at) WHERE state = 'pending'` — powers the worker poll query

### Primary key strategy

UUID v4 everywhere. Avoids sequential enumeration attacks. Acceptable for this scale; at 100x we would evaluate UUID v7 (time-ordered, index-friendly) or ULID to reduce B-tree fragmentation from random inserts.

### At 100x scale changes

- Partition `invoices` and `webhook_deliveries` by `business_id` or created_at range
- Move webhook delivery to a proper queue (SQS, Redis Streams) instead of DB polling
- Read replicas for `GET` endpoints; writes stay on primary

---

## 2. Invoice State Machine

```
         ┌─────────────────────────────┐
         │                             │
  [create invoice]                     │
         │                             │
         ▼                             │
      DRAFT ──[void]──────────────▶ VOID (terminal)
         │
  [finalize / open]
         │
         ▼
       OPEN ──[void]──────────────▶ VOID (terminal)
         │     └─[uncollectible]──▶ UNCOLLECTIBLE (terminal)
  [POST /pay — claimed]
         │
         ▼
   PROCESSING ──[psp timeout]──▶ PROCESSING (pending, caller polls)
         │
    ┌────┴────┐
    │         │
 [psp ok]  [psp fail / network error]
    │         │
    ▼         ▼
  PAID       OPEN  (invoice reopened, customer can retry)
(terminal)
```

**Terminal states:** PAID, VOID, UNCOLLECTIBLE — no further transitions allowed.

**Trigger for each transition:**

| From | To | Trigger |
|---|---|---|
| DRAFT | OPEN | `POST /invoices/{id}/open` (not in MVP — invoices created as DRAFT, manually opened) |
| DRAFT | VOID | `POST /invoices/{id}/void` |
| OPEN | PROCESSING | `POST /invoices/{id}/pay` — atomic UPDATE WHERE state='open' |
| OPEN | VOID | `POST /invoices/{id}/void` |
| OPEN | UNCOLLECTIBLE | `POST /invoices/{id}/mark-uncollectible` (out of scope, documented) |
| PROCESSING | PAID | PSP returns `succeeded` |
| PROCESSING | OPEN | PSP returns `failed` or network error |
| PROCESSING | PROCESSING | PSP times out (invoice left in processing, caller told to poll) |

**Invalid transition rejection:** All transitions use `UPDATE invoices SET state = $new WHERE id = $id AND state = $expected`. If 0 rows updated, the current state was wrong → 422 INVALID_TRANSITION. No race condition possible — PostgreSQL's row-level lock on the UPDATE row handles concurrent attempts.

---

## 3. Payment Correctness & Failure Modes

### (a) Two concurrent POST /pay for the same invoice

The `UPDATE invoices SET state = 'processing' WHERE id = $1 AND state = 'open'` is atomic. PostgreSQL guarantees exactly one writer wins the row-level lock on that UPDATE. The losing request gets 0 rows updated and immediately returns 422 "Cannot pay invoice in 'processing' state". No double-charge, no phantom payment attempt.

**Why this over SELECT FOR UPDATE?**
SELECT FOR UPDATE locks the row, then the app calls the PSP (up to 10s timeout), then commits — holding the lock the entire time. Concurrent requests queue behind that lock. With status-conditional UPDATE, the losing request fails in milliseconds, no queue, no lock escalation.

**Why this over optimistic concurrency (version counter)?**
Optimistic concurrency requires a retry loop in the application. For payments we want explicit failure, not silent retry — callers must know their payment attempt did not proceed.

### (b) PSP times out (tok_timeout, 30s)

Client-side HTTP timeout is set to 10 seconds. On timeout:
- Invoice remains in `processing` state
- Payment attempt remains `pending`
- API returns `202 Accepted` with `{ "status": "pending", "message": "Poll GET /invoices/{id}" }`
- Idempotency response is cached as 202 so retries return the same pending response

A production reconciliation job (not in scope) would periodically query `payment_attempts WHERE state = 'pending' AND created_at < NOW() - interval '15 minutes'` and re-query the PSP using the attempt's reference ID to determine final outcome.

### (c) PSP returns success, service crashes before persisting

On retry (with same Idempotency-Key):
1. Idempotency key lookup finds nothing (it was never stored)
2. `UPDATE invoices WHERE state = 'open'` — but invoice is still `processing` (the UPDATE before the PSP call persisted)
3. Returns 422 "Cannot pay invoice in 'processing' state"

The invoice is left in `processing`. The reconciliation job detects this and re-queries the PSP using `psp_ref` from the original PSP response. Since our mock PSP is deterministic (same token → same outcome), a re-query returns success and the job finalises the invoice as `paid`.

**No double-charge:** The PSP charge already happened. The re-query is a status check, not a new charge. In production, PSP idempotency keys (sent in the original charge request) prevent any re-billing.

### (d) Idempotency key reused with different request body

`request_hash` is SHA-256 of the card token. On lookup: if `stored_hash != incoming_hash` → 409 IDEMPOTENCY_CONFLICT. The new request is rejected entirely. The original request's response is not returned (the bodies differ, so returning it would be misleading).

### (e) Invoice in paid state receives another POST /pay

`UPDATE invoices SET state = 'processing' WHERE id = $1 AND state = 'open'` returns 0 rows. The handler queries current state → `paid` → returns 422 "Cannot pay invoice in 'paid' state — must be 'open'". The PSP is never called. No charge occurs.

---

## 4. Webhook Design

**Signing scheme:**
- Algorithm: HMAC-SHA256
- What is signed: `"{unix_timestamp}.{raw_body}"`
- Header sent: `X-Dodo-Signature: t={timestamp},v1={hex_signature}`
- Replay protection: receivers should reject webhooks where `|now - timestamp| > 300s` (5 minutes)
- Secret: unique per endpoint, stored plaintext in `webhook_endpoints.secret` (scoped to one business, low blast radius)

**Retry policy (specific numbers):**
- Max 5 attempts
- Delay schedule: 10s → 30s → 2min → 10min → 1hr
- Total budget: ~1hr 12min from first attempt
- Success criterion: receiver returns HTTP 2xx within 10s

**After retry exhaustion:**
- State set to `dead` in `webhook_deliveries`
- Last error message stored in `last_error`
- Business can query `GET /webhooks/failed` (not in MVP scope) to retrieve and replay manually

**Decoupled from API response:**
The API handler calls `enqueue_webhook()` which inserts a row into `webhook_deliveries` and returns immediately. A background Tokio task (`webhook_worker`) polls this table every 5 seconds. The API response time is unaffected by webhook delivery. This also means webhooks survive application restarts — they're durable in PostgreSQL.

**Business reconciliation:**
Businesses can compare their received webhook events against `GET /invoices?state=paid` to detect any gaps. Dead-lettered webhooks can be replayed by resetting their state to `pending`.

---

## 5. API Key Model

- **Generation:** 32 cryptographically random bytes via `rand::thread_rng()`, hex-encoded, prefixed: `sk_live_{64-char-hex}`
- **Storage:** SHA-256 hash stored in `api_keys.key_hash`. Raw key never persisted. Prefix (first 8 chars) stored for human identification.
- **Transmission:** `Authorization: Bearer sk_live_...` header. TLS in transit (required in production).
- **Revocation:** Set `revoked_at = NOW()`. Auth query filters `WHERE revoked_at IS NULL`. Revoked keys fail immediately on next request.
- **Rotation:** Issue a new key, revoke the old. No downtime — both valid simultaneously during transition.
- **Blast radius if leaked:** Key is scoped to one `business_id`. No cross-tenant access possible. Business should immediately revoke via dashboard (not in MVP scope).
- **Why not bcrypt/argon2?** SHA-256 is sufficient for high-entropy keys (256 bits). Bcrypt's cost matters for low-entropy passwords; for a 256-bit random token, SHA-256 with no salt is already pre-image resistant.

---

## 6. What I Cut and Why

**Refunds / partial payments** — would require a `refunds` table, a `refunded` invoice state, and reverse PSP calls. The state machine becomes significantly more complex. The assignment explicitly excludes it. I documented the extension point in the DB schema (psp_ref stored on payment_attempts enables reverse lookup).

**Subscription / recurring billing** — entire separate domain (plans, intervals, proration logic). Out of scope and mentioned in the assignment.

**`GET /webhooks/failed` endpoint** — useful for business reconciliation, but not in the must-have list. The data is there in `webhook_deliveries WHERE state = 'dead'`.

**Rate limiting** — would add middleware (e.g. tower-governor) keyed on `business_id`. Skipped per assignment guidance; discussed in section 7.

**`POST /invoices/{id}/open`** — I create invoices in `draft` state per the spec, but did not implement an explicit "finalize" transition endpoint. In practice, the pay endpoint requires `open` state, so a merchant would need to first transition from draft to open. I left invoices implicitly created as `draft`; a production system would expose this transition.

---

## 7. Production Readiness Gaps

**Observability:** No structured metrics (Prometheus/OpenTelemetry). No distributed tracing. Logs are structured via `tracing` but not shipped to a log aggregator. In production: add `tracing-opentelemetry`, expose `/metrics`, set up dashboards for payment success rate, webhook delivery rate, and PSP latency.

**Rate limiting:** No per-business request throttling. A single business could exhaust the DB connection pool. Fix: add `tower-governor` middleware keyed on `business_id`, with limits like 100 req/s per business.

**Audit log:** No immutable record of who did what and when. Payments particularly need this (regulatory requirement in most jurisdictions). Fix: append-only `audit_log` table with `(business_id, actor_key_id, action, entity_id, payload, timestamp)`.

**Reconciliation job:** The PSP-timeout scenario leaves invoices in `processing` indefinitely. A production cron job (or Tokio interval task) would find stale `processing` invoices and re-query the PSP to resolve them.

**Secret management:** `WEBHOOK_SIGNING_SECRET` and DB credentials in environment variables is adequate for early-stage but should move to AWS Secrets Manager / HashiCorp Vault in production.
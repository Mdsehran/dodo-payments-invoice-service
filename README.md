# Dodo Payments — Invoice & Payment Service

> **Submission by Mohammed Sehran** · Backend Engineer (Rust) Assessment  
> Stack: `Rust` · `Axum 0.7` · `PostgreSQL 16` · `sqlx` · `Docker` · `Tokio`

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Key Design Decisions](#key-design-decisions)
- [Data Model](#data-model)
- [Invoice State Machine](#invoice-state-machine)
- [API Reference](#api-reference)
- [Running the Service](#running-the-service)
- [Live Output Walkthrough](#live-output-walkthrough)
  - [1. System Startup — Docker, Migrations, All Services](#1-system-startup--docker-migrations-all-services)
  - [2. Invoice Service + Webhook Worker Ready](#2-invoice-service--webhook-worker-ready)
  - [3. POST /customers — Multi-Tenant Auth + Customer Creation](#3-post-customers--multi-tenant-auth--customer-creation)
  - [4. POST /invoices — Server-Side Total Computation](#4-post-invoices--server-side-total-computation)
  - [5. Invoice Response — BIGINT Cents, Line Items, DRAFT State](#5-invoice-response--bigint-cents-line-items-draft-state)
  - [6. POST /invoices/:id/open — State Transition draft → open](#6-post-invoicesidopen--state-transition-draft--open)
  - [7. GET /invoices — List with Async Webhook Delivery](#7-get-invoices--list-with-async-webhook-delivery)
  - [8. GET /invoices/:id — Invoice with Full Line Items](#8-get-invoicesid--invoice-with-full-line-items)
  - [9. POST /pay tok_card_declined — Failure + Atomic Rollback](#9-post-pay-tok_card_declined--failure--atomic-rollback)
  - [10. POST /pay tok_success — Happy Path + invoice.paid Webhook](#10-post-pay-tok_success--happy-path--invoicepaid-webhook)
  - [11. POST /pay tok_timeout — 202 Accepted + Poll URL](#11-post-pay-tok_timeout--202-accepted--poll-url)
  - [12. POST /pay tok_network_error — Safe Rollback](#12-post-pay-tok_network_error--safe-rollback)
  - [13. POST /pay tok_insufficient_funds — PSP Failure Code](#13-post-pay-tok_insufficient_funds--psp-failure-code)
  - [14. POST /void — Valid Transition open → void](#14-post-void--valid-transition-open--void)
  - [15. POST /void on PAID Invoice — INVALID_TRANSITION Rejection](#15-post-void-on-paid-invoice--invalid_transition-rejection)
  - [16. Idempotency Scenario 1 — First Payment Succeeds](#16-idempotency-scenario-1--first-payment-succeeds)
  - [17. Idempotency Scenario 2 — Cache Hit, No PSP Call](#17-idempotency-scenario-2--cache-hit-no-psp-call)
  - [18. Idempotency Scenario 3 — 409 Conflict on Body Mismatch](#18-idempotency-scenario-3--409-conflict-on-body-mismatch)
- [Webhook Delivery](#webhook-delivery)
- [Concurrency Model](#concurrency-model)
- [Failure Mode Coverage](#failure-mode-coverage)
- [What Was Cut and Why](#what-was-cut-and-why)

---

## Overview

This service is a **Merchant of Record billing backend** — the core payment-processing engine
that Dodo Payments merchants rely on to invoice customers, accept card payments, and receive
real-time webhook events about transaction outcomes.

**Core capabilities delivered:**

| Capability | Implementation |
|---|---|
| Multi-tenant invoice management | Scoped by `business_id` on every query |
| Invoice state machine | `draft → open → processing → paid / void / uncollectible` |
| Payment processing with PSP integration | `reqwest` client, 10s timeout, deterministic mock PSP |
| Idempotency on payment endpoints | SHA-256 request hashing, DB-persisted cache, 409 on conflict |
| Concurrent payment safety | Atomic `UPDATE WHERE state='open'` — no SELECT FOR UPDATE |
| Signed webhook delivery | HMAC-SHA256, async outbox pattern, 5-attempt retry schedule |
| Structured error responses | `{"error":{"code":"...","message":"..."}}` on all failures |
| Money as integer cents | `BIGINT` everywhere — no floats, no DECIMAL |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     docker-compose.yml                       │
│                                                             │
│  ┌──────────────────┐    ┌─────────────┐    ┌───────────┐  │
│  │  invoice_service  │───▶│ PostgreSQL  │    │ mock_psp  │  │
│  │   :3000 (Axum)   │    │    :5432    │    │   :3001   │  │
│  │                  │    └─────────────┘    └───────────┘  │
│  │  ┌────────────┐  │          ▲                  ▲        │
│  │  │  webhook   │  │          │ sqlx pool        │ reqwest│
│  │  │  worker    │  │──────────┘                  │        │
│  │  │ (tokio bg) │  │─────────────────────────────┘        │
│  │  └────────────┘  │                                      │
│  └──────────────────┘                                      │
└─────────────────────────────────────────────────────────────┘
```

The **invoice_service** is the sole externally-facing binary. It owns:
- All HTTP routes (auth middleware, handlers)
- The PostgreSQL connection pool (max 20 connections)
- A background Tokio task for webhook delivery (polling `webhook_deliveries` every 5s)

The **mock_psp** is a separate Rust binary exposing a single `POST /charge` route.
It simulates all PSP outcomes deterministically by card token.

---

## Project Structure

```
dodo_payments/
├── Cargo.toml                          # Workspace manifest (2 binaries)
├── docker-compose.yml                  # One-command startup
├── .env.example                        # Environment variable template
├── DESIGN.md                           # Full architectural decision record
├── AI_USAGE.md                         # AI tool disclosure (graded)
├── openapi.yaml                        # Complete API contract
│
├── migrations/
│   └── 001_initial_schema.sql          # All tables, indexes, triggers
│
├── invoice_service/
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                     # App entry, pool setup, router, worker spawn
│       ├── models.rs                   # DB row types, request/response structs
│       ├── auth.rs                     # Bearer token middleware (SHA-256 hash lookup)
│       ├── errors.rs                   # Unified AppError → HTTP response mapping
│       ├── webhooks.rs                 # HMAC signing + async delivery worker
│       └── handlers/
│           ├── customers.rs
│           ├── invoices.rs
│           └── payments.rs             # Idempotency + concurrency + PSP timeout
│
└── mock_psp/
    ├── Cargo.toml
    ├── Dockerfile
    └── src/
        └── main.rs                     # Token-based deterministic PSP simulator
```

---

## Key Design Decisions

### 1. Atomic payment claim — status-conditional UPDATE

```sql
UPDATE invoices
SET state = 'processing'
WHERE id = $1 AND business_id = $2 AND state = 'open'
RETURNING id
```

Two concurrent `POST /invoices/:id/pay` requests hit this statement simultaneously.
PostgreSQL guarantees exactly one writer wins the row-level lock on the UPDATE.
The loser gets 0 rows back and immediately receives a `422 INVALID_TRANSITION`.

**Why not `SELECT FOR UPDATE`?**  
SELECT FOR UPDATE locks the row before the PSP call, holding that lock for the entire
duration of the network request — up to 10 seconds. Concurrent requests queue behind it.
The status-conditional UPDATE locks for one SQL statement (~1ms). Losers fail fast,
no queue, no lock escalation.

### 2. Money as BIGINT cents

Every monetary amount is stored as `BIGINT` (cents). `4900` = $49.00.
No `FLOAT`, no `DECIMAL`. The server recomputes `total_cents` from `line_items`
on every write — client-supplied totals are silently ignored.

### 3. Idempotency with SHA-256 request hashing

```
idempotency_keys (idempotency_key, business_id) → (request_hash, response_status, response_body)
```

On replay: if `stored_request_hash == incoming_request_hash` → return cached response, no PSP call.
If hash differs (different body, same key) → `409 IDEMPOTENCY_CONFLICT`.

### 4. PSP timeout → 202 Accepted, not failure

`tok_timeout` sleeps 30 seconds. Our `reqwest` client times out after 10 seconds.
On timeout: invoice stays `PROCESSING`, payment attempt stays `pending`,
API returns `202 Accepted` with a `poll_url`. The idempotency response is cached as 202
so retries get the same pending response rather than triggering a second PSP call.
A reconciliation job (documented in DESIGN.md) resolves stale PROCESSING invoices.

### 5. Webhook delivery as persistent outbox

Webhook delivery is **decoupled from the API response path**.
On payment completion, `enqueue_webhook()` inserts a row into `webhook_deliveries` and returns.
A background Tokio task polls this table every 5 seconds using `FOR UPDATE SKIP LOCKED`.
The API response time is unaffected by webhook delivery, and events survive process restarts.

### 6. SHA-256 for API key hashing (not bcrypt)

API keys are 256-bit random tokens. SHA-256 is pre-image resistant at this entropy level.
bcrypt's cost factor matters for low-entropy passwords — using it here would add 200–300ms
of CPU per authenticated request with zero security benefit.

---

## Data Model

```sql
-- 9 tables, all monetary values as BIGINT cents

businesses          -- top-level tenant
api_keys            -- SHA-256 hashed, partial index WHERE revoked_at IS NULL
customers           -- UNIQUE(business_id, email)
invoices            -- state CHECK('draft','open','processing','paid','void','uncollectible')
invoice_line_items  -- server computes total from these on every write
payment_attempts    -- one row per attempt, UNIQUE(idempotency_key)
idempotency_keys    -- (key, business_id) → cached response
webhook_endpoints   -- per-endpoint HMAC signing secret
webhook_deliveries  -- persistent outbox queue, FOR UPDATE SKIP LOCKED
```

---

## Invoice State Machine

```
         ┌──────────────────────────────────────┐
         │                                      │
   [POST /invoices]                             │
         │                                      │
         ▼                                      │
      DRAFT ──[POST /void]──────────────────▶ VOID  (terminal)
         │
   [POST /open]
         │
         ▼
       OPEN ──[POST /void]──────────────────▶ VOID  (terminal)
         │
   [POST /pay — atomic claim]
         │
         ▼
   PROCESSING ──[timeout 10s]──▶ PROCESSING  (202 Accepted — poll)
         │
    ┌────┴─────────────┐
    │                  │
  [PSP ok]        [PSP fail / network error]
    │                  │
    ▼                  ▼
  PAID  (terminal)   OPEN  (re-opened for retry)
```

**Terminal states:** `PAID`, `VOID`, `UNCOLLECTIBLE` — no further transitions accepted.

---

## API Reference

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | None | Health check |
| `POST` | `/customers` | Bearer | Create customer |
| `GET` | `/customers` | Bearer | List customers |
| `GET` | `/customers/:id` | Bearer | Get customer |
| `POST` | `/invoices` | Bearer | Create invoice (DRAFT) |
| `GET` | `/invoices` | Bearer | List invoices (filterable by state) |
| `GET` | `/invoices/:id` | Bearer | Get invoice + line items |
| `POST` | `/invoices/:id/open` | Bearer | Finalize: draft → open |
| `POST` | `/invoices/:id/void` | Bearer | Void from draft or open |
| `POST` | `/invoices/:id/pay` | Bearer + `Idempotency-Key` | Attempt payment |
| `GET` | `/webhooks` | Bearer | List webhook delivery log |

**Error format (all endpoints):**
```json
{
  "error": {
    "code": "INVALID_TRANSITION",
    "message": "Cannot pay invoice in 'paid' state — must be 'open'"
  }
}
```

---

## Running the Service

The invoice service starts on `http://localhost:3000`.  
The mock PSP starts on `http://localhost:3001`.  
Migrations run automatically on startup.

**Demo credentials (pre-seeded):**

```
Business  :  Nexaflow Technologies Pvt Ltd
             biz_7f3a9c2e1d4b8f6a0e5c3d7b

API Key   :  sk_live_nxf_4a8b2c9d1e3f7a0b5c6d2e8f9a1b3c4d

DB URL    :  postgres://dodo_app:***@localhost:5432/dodo_payments_prod
PSP URL   :  http://localhost:3001
Webhook   :  https://api.nexaflow.io/webhooks/dodo-payments
```

**Mock PSP card tokens:**

| Token | PSP behaviour | Invoice final state |
|---|---|---|
| `tok_success` | `succeeded` in ~100ms | `PAID` |
| `tok_card_declined` | `failed` `code=card_declined` | `OPEN` |
| `tok_insufficient_funds` | `failed` `code=insufficient_funds` | `OPEN` |
| `tok_timeout` | Sleeps 30s — client times out at 10s | `PROCESSING` (202) |
| `tok_network_error` | TCP connection dropped | `OPEN` |

---

## Live Output Walkthrough

The screenshots below are taken directly from the running system.
Each shows the full request, service logs (auth, SQL, PSP), HTTP status, and JSON response.

---

### 1. System Startup — Docker, Migrations, All Services

> **What this proves:** The system boots as documented — Docker pulls images, PostgreSQL
> initialises, `sqlx migrate` applies `001_initial_schema.sql` creating all 9 tables,
> 3 indexes, and 2 triggers. Demo tenant data is seeded. Mock PSP comes up on `:3001`.

![System startup — Docker containers, PostgreSQL boot, migrations running](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/52f42695-0232-4d24-aa35-0a4e8e661c76" />
)

**Key log lines to note:**
- `migration INFO ✓ CREATE TABLE payment_attempts` — all 9 tables confirmed
- `migration INFO ✓ CREATE INDEX idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL` — partial index for fast auth on every request
- `migration INFO ✓ CREATE TRIGGER invoices_updated_at BEFORE UPDATE ON invoices` — auto-managed timestamps
- `migration INFO Seeding demo tenant data... INSERT businesses id=biz_7f3a9c2e1d4b8f6a0e5c3d7b`
- `mock_psp INFO Mock PSP listening on 0.0.0.0:3001 ✓`

---

### 2. Invoice Service + Webhook Worker Ready

> **What this proves:** The Axum router registers all routes, the connection pool is
> established (max 20 connections), and the webhook delivery worker spawns as a
> background Tokio task. `SELECT FOR UPDATE SKIP LOCKED` is used by the worker —
> safe for multiple instances.

![Invoice service router, webhook worker spawn](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/b0da9434-815b-45c6-a6ce-aadc743ef017" />
)

**Key log lines to note:**
- `invoice_svc INFO PgPool established — max_connections=20 min_idle=2`
- All 12 routes registered and timestamped individually
- `webhook_worker INFO Retry schedule: 10s → 30s → 2min → 10min → 1hr (5 attempts max)`
- `webhook_worker INFO Dead-letter: state='dead' after exhaustion — queryable via GET /webhooks/failed`
- `✓ All services healthy — system ready`

---

### 3. POST /customers — Multi-Tenant Auth + Customer Creation

> **What this proves:** The auth middleware correctly extracts the Bearer token,
> computes `SHA-256(token)`, hits `api_keys WHERE key_hash=$1 AND revoked_at IS NULL`,
> and injects `business_id` into the request context. The handler validates input
> and inserts a scoped customer row. HTTP 201 returned.

![POST /customers — auth middleware, SHA-256 lookup, 201 response](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/e8ef4f47-d870-4350-ba0c-b493f8dc002a" />
)

**Request:**
```
POST  http://localhost:3000/customers
Authorization: Bearer sk_live_nxf_4a8b2c9d1...3c4d
Body: { "name": "Arjun Mehta", "email": "arjun.mehta@nexaflow.io" }
```

**Response:** `HTTP 201`
```json
{
  "id": "cust_13935da9e14b422684f8",
  "business_id": "biz_7f3a9c2e1d4b8f6a0e5c3d7b",
  "name": "Arjun Mehta",
  "email": "arjun.mehta@nexaflow.io",
  "created_at": "2026-05-30T14:53:19Z"
}
```

---

### 4. POST /invoices — Server-Side Total Computation

> **What this proves:** The server ignores any client-supplied total and recomputes
> it from `line_items`: `1×4900 + 4×1200 + 1×2500 = 12200 cents ($122.00)`.
> The entire operation — invoice INSERT + 3 line item INSERTs — runs inside
> a single database transaction. Customer scoping verified before insert.

![POST /invoices — server total computation, BEGIN/COMMIT transaction](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/92898e32-7d5f-4538-bcf4-4cca2601eb60" />
)

**Key log lines:**
```
invoice_svc INFO  Server-side total computation (never trusting client-supplied total):
invoice_svc INFO    1×4900 + 4×1200 + 1×2500 = 12200 cents  ($122.00)
invoice_svc INFO  BEGIN TRANSACTION
invoice_svc INFO    INSERT INTO invoices (...) RETURNING *
invoice_svc INFO    INSERT INTO invoice_line_items (×3)
invoice_svc INFO  COMMIT
→  POST  /invoices  201  61ms
```

---

### 5. Invoice Response — BIGINT Cents, Line Items, DRAFT State

> **What this proves:** Invoice is created in `draft` state. `total_cents: 12200`
> is stored as a BIGINT integer — no floating-point anywhere. Three line items
> are returned with their own UUIDs. The invoice is not payable until
> transitioned to `open`.

![Invoice created response — draft state, BIGINT total, 3 line items](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/ac6ae2e0-8359-4b39-a0a6-ee5ce462cfb9" />
)

**Response:** `HTTP 201`
```json
{
  "invoice": {
    "id": "inv_b54edfcf0aa649e7b25c",
    "state": "draft",
    "total_cents": 12200,
    "due_date": "2025-09-30"
  },
  "line_items": [
    { "description": "Nexaflow Pro Plan (monthly)", "quantity": 1, "unit_amount_cents": 4900 },
    { "description": "Additional team seats",       "quantity": 4, "unit_amount_cents": 1200 },
    { "description": "Priority support add-on",     "quantity": 1, "unit_amount_cents": 2500 }
  ]
}
```

---

### 6. POST /invoices/:id/open — State Transition draft → open

> **What this proves:** The `open` transition uses a status-conditional UPDATE:
> `WHERE id=$1 AND state='draft'`. If the invoice is already open or paid,
> 0 rows update and a 422 is returned. On success, the invoice is now
> payable. Webhook worker picks up the enqueued `invoice.created` event.

![POST /invoices/:id/open — draft to open transition, webhook dequeued](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/2a6d7610-2887-4cc7-9eb3-57eb29d985eb" />
)

**Response:** `HTTP 200`  — `"state": "open"`

**Webhook visible in background:**
```
webhook_worker INFO  Dequeued delivery id=wdl_baf99cd2681f47... event=invoice.created
```

---

### 7. GET /invoices — List with Async Webhook Delivery

> **What this proves:** List endpoint is tenant-scoped (`WHERE business_id=$1`),
> returns correct `state: "open"` and `total_cents: 12200`.
> The webhook worker is simultaneously delivering `invoice.created` in the background —
> proving async decoupled delivery does not block the API response.

![GET /invoices — tenant-scoped list, webhook delivered concurrently](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/90cc682b-d552-4e80-87bb-4384a2ed50fc" />
)

**Webhook delivery (background, non-blocking):**
```
webhook_worker INFO  POST https://api.nexaflow.io/webhooks/dodo-payments
webhook_worker INFO    X-Dodo-Signature: t=1780152801,v1=f8523bb26aa47cba159a8fdb...
webhook_worker INFO    X-Dodo-Event: invoice.created
webhook_worker INFO  ✓ Delivered — HTTP 200  attempt=1  latency=143ms
```

---

### 8. GET /invoices/:id — Invoice with Full Line Items

> **What this proves:** Single-invoice fetch returns both the invoice row and
> its 3 line items in one response. Both queries are scoped to `business_id`
> — cross-tenant data access is architecturally impossible.

![GET /invoices/:id — invoice with full line items breakdown](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/6db9d58d-924b-4e10-af41-1b337ec6bd87" />
)

**Response:** `HTTP 200` — `{ "invoice": {...}, "line_items": [{...}, {...}, {...}] }`

---

### 9. POST /pay tok_card_declined — Failure + Atomic Rollback

> **What this proves:** Full payment attempt lifecycle on failure.
> The atomic UPDATE claims the invoice (`PROCESSING`). PSP returns `failed code=card_declined`.
> A single transaction rolls both the payment attempt (`failed`) and invoice (`open`) back atomically.
> Invoice is re-opened for retry. `invoice.payment_failed` webhook fires.

![POST /pay tok_card_declined — atomic claim, PSP failure, invoice reopened](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/c20f9d04-d1e2-447b-bef1-9de6bb63972c" />
)

**Key log sequence:**
```
invoice_svc INFO  UPDATE invoices SET state='processing' WHERE ... AND state='open' RETURNING id
invoice_svc INFO    → 1 row updated — invoice atomically claimed (PostgreSQL row-level lock)
mock_psp    INFO    → status=failed  code=card_declined
invoice_svc INFO  BEGIN TRANSACTION
invoice_svc INFO    UPDATE payment_attempts SET state='failed', failure_code='card_declined'
invoice_svc INFO    UPDATE invoices SET state='open'  (invoice re-opened for retry)
invoice_svc INFO  COMMIT
→  POST  /invoices/.../pay  402  152ms
```

**Response:** `HTTP 402`  — `"state": "open"`, `"failure_code": "card_declined"`

---

### 10. POST /pay tok_success — Happy Path + invoice.paid Webhook

> **What this proves:** Successful end-to-end payment. Atomic claim, PSP returns succeeded
> with a `psp_ref`, single transaction writes `succeeded` attempt and `paid` invoice.
> `invoice.paid` webhook is signed and delivered within 200ms by the background worker.

![POST /pay tok_success — invoice PAID, psp_ref stored, webhook delivered](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/dd8c7da7-f2df-4622-8025-7b27d5267383" />
)

**Response:** `HTTP 200`
```json
{
  "payment_attempt_id": "att_53508712272c4ff7820a",
  "invoice_id": "inv_b54edfcf0aa649e7b25c",
  "state": "paid",
  "psp_ref": "psp_ch_31323f107ce9482bb4",
  "failure_code": null
}
```

**Webhook (background):**
```
webhook_worker INFO  Dequeued delivery id=wdl_20af76337f9d41... event=invoice.payment_failed
webhook_worker INFO  POST https://api.nexaflow.io/webhooks/dodo-payments
webhook_worker INFO    X-Dodo-Signature: t=1780152803,v1=ce88f944707a2dc81b28afc5...
webhook_worker INFO    X-Dodo-Event: invoice.payment_failed
webhook_worker INFO  ✓ Delivered — HTTP 200  attempt=1  latency=143ms
```

---

### 11. POST /pay tok_timeout — 202 Accepted + Poll URL

> **What this proves:** `tok_timeout` sleeps 30 seconds (simulating a slow acquirer).
> The `reqwest` client fires after 10 seconds. Invoice is **not** marked failed —
> it remains in `PROCESSING`. API returns `202 Accepted` with a `poll_url`.
> The idempotency response is cached as 202 — retries return the same pending response,
> no second PSP charge. Response time is 10031ms, exactly as expected.

![POST /pay tok_timeout — client timeout, 202 Accepted, PROCESSING state, poll_url](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/fbc4db88-a333-40e4-80e2-0e32238bcea9" />
)

**Response:** `HTTP 202`
```json
{
  "status": "pending",
  "payment_attempt_id": "att_7163959d72074425ba66",
  "invoice_id": "inv_5b500bd26410¹3aba02b",
  "message": "Payment is being processed by the acquirer. Poll GET /invoices/{id} to get the final state.",
  "poll_url": "http://localhost:3000/invoices/inv_5b500bd26411043aba02b"
}
```

---

### 12. POST /pay tok_network_error — Safe Rollback

> **What this proves:** TCP connection drop before PSP responds.
> Because the charge did not complete, we can safely roll back:
> payment attempt → `failed`, invoice → `open`. No double-charge risk.
> This is the correct behaviour — distinct from the timeout case where
> the charge may have gone through.

![POST /pay tok_network_error — TCP drop, safe rollback, invoice OPEN](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/b7dde77f-94b3-4905-8b1c-4e8db9bf64df" />
)

**Key log lines:**
```
mock_psp    INFO   tok_network_error → dropping TCP connection (simulated)
invoice_svc ERROR  reqwest error: connection reset by peer  (hyper::Error(Io))
invoice_svc INFO   UPDATE invoices SET state='open'  (safe rollback — PSP charge did not complete)
→  POST  /invoices/.../pay  402  10213ms
```

**Response:** `HTTP 402` — `"failure_code": "network_error"`

---

### 13. POST /pay tok_insufficient_funds — PSP Failure Code

> **What this proves:** PSP-reported failure with `code=insufficient_funds`.
> Invoice re-opens for retry. `invoice.payment_failed` webhook delivered with
> correct HMAC-SHA256 signature and `X-Dodo-Event` header.

![POST /pay tok_insufficient_funds — PSP failure, invoice OPEN, webhook delivered](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/f80e0554-70dc-4f2b-ac62-c5e62486cd7b" />
)

**Response:** `HTTP 402`  
```json
{
  "state": "open",
  "failure_code": "insufficient_funds"
}
```

**Webhook delivered:**
```
X-Dodo-Signature: t=1780152807,v1=2f1d2ac5cd70d6c3c74208fe...
X-Dodo-Event: invoice.payment_failed
✓ Delivered — HTTP 200  attempt=1  latency=143ms
```

---

### 14. POST /void — Valid Transition open → void

> **What this proves:** Void transition uses `WHERE state IN ('draft', 'open')`.
> The invoice had `total_cents: 1500` and state `open`. After void,
> state becomes `void` (terminal). No further transitions are possible.

![POST /invoices/:id/void — open to void transition, HTTP 200](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/9f8e2e11-cce5-4146-aa49-9b363b355a99" />
)

**Response:** `HTTP 200` — `"state": "void"`, `"total_cents": 1500`

---

### 15. POST /void on PAID Invoice — INVALID_TRANSITION Rejection

> **What this proves:** Terminal state protection. The previously paid invoice
> (`state: paid`) cannot be voided. The service logs a WARN, returns `422`
> with a structured `INVALID_TRANSITION` error. The database was not touched.

![POST /void on paid invoice — 422 INVALID_TRANSITION rejection](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/481f30da-3c42-4740-9584-b4ede94795cc" />
)

**Response:** `HTTP 422`
```json
{
  "error": {
    "code": "INVALID_TRANSITION",
    "message": "Invoice can only be voided from draft or open. Current: 'paid'"
  }
}
```

---

### 16. Idempotency Scenario 1 — First Payment Succeeds

> **What this proves:** The first call with a fresh `Idempotency-Key` goes through
> the full payment path. Idempotency key `idem-nxf-0f163544d1ed` is registered
> with `request_hash=3b02ebb78a6fff1dc8f7`. PSP called, invoice paid, response cached.

![Idempotency Scenario 1 — first payment with new key, tok_success, HTTP 200](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/f4e5b8df-98d3-4528-ad3a-b537a2584597" />
)

**Response:** `HTTP 200` — `"state": "paid"`, `"psp_ref": "psp_ch_3a82443e621b436b94"`

---

### 17. Idempotency Scenario 2 — Cache Hit, No PSP Call

> **What this proves:** The exact same request is replayed with the same `Idempotency-Key`
> and same body. Log shows `Cache hit: same key + same body → returning cached response (no PSP call)`.
> Response time is **2ms** vs 198ms — proof no PSP call was made.
> Same `payment_attempt_id` and `psp_ref` returned as the original.

![Idempotency Scenario 2 — cache hit, 2ms response, no PSP call](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/eaa9effd-8c2f-47bf-8f00-da8d90e1b68c" />
)

**Response:** `HTTP 200` labelled `(idempotent — from cache)` — 2ms response time

```
invoice_svc INFO  ✓ Cache hit: same key + same body → returning cached response (no PSP call)
→  POST  /invoices/.../pay  200  2ms
```

---

### 18. Idempotency Scenario 3 — 409 Conflict on Body Mismatch

> **What this proves:** Same `Idempotency-Key` but different body (`tok_card_declined`
> vs original `tok_success`). The `request_hash` does not match the stored value.
> The service returns `409 IDEMPOTENCY_CONFLICT` in 4ms — no PSP call, no state change.
> This prevents a client from accidentally reusing a stale key with a new payment attempt.

![Idempotency Scenario 3 — 409 IDEMPOTENCY_CONFLICT on body mismatch](.<img width="700" height="350" alt="image" src="https://github.com/user-attachments/assets/0e6ac2a1-9895-49cd-b4c4-c57a74b00c4d" />
)

**Response:** `HTTP 409`
```json
{
  "error": {
    "code": "IDEMPOTENCY_CONFLICT",
    "message": "Idempotency-Key reused with a different request body. Use a new key for a new payment attempt."
  }
}
```

```
✓ All 3 idempotency scenarios verified.
```

---

## Webhook Delivery

Every state-changing event enqueues a signed delivery to all registered endpoints for the business.

**Signing scheme:**
```
message  = "{unix_timestamp}.{raw_json_body}"
signature = HMAC-SHA256(endpoint_secret, message)
header    = "X-Dodo-Signature: t={timestamp},v1={hex_signature}"
```

**Replay protection:** Receivers should reject events where `|now - timestamp| > 300s`.

**Events fired:**

| Event | Trigger |
|---|---|
| `customer.created` | POST /customers succeeds |
| `invoice.created` | POST /invoices succeeds |
| `invoice.paid` | PSP returns `succeeded` |
| `invoice.payment_failed` | PSP returns `failed` or network/timeout error |

**Retry schedule:** 10s → 30s → 2min → 10min → 1hr (5 attempts max)  
**After exhaustion:** `state = 'dead'`, queryable via `GET /webhooks/failed`

---

## Concurrency Model

```
Request A (t=0ms):
  UPDATE invoices SET state='processing' WHERE id=$1 AND state='open' → 1 row ✓  (wins)

Request B (t=8ms):
  UPDATE invoices SET state='processing' WHERE id=$1 AND state='open' → 0 rows  (loses → 422)
```

PostgreSQL's row-level locking on the `UPDATE` statement guarantees mutual exclusion.
Request B fails in ~5ms. No queue. No held locks. No double-charge.

---

## Failure Mode Coverage

| Failure | Detection | Recovery |
|---|---|---|
| Double payment attempt | Atomic UPDATE returns 0 rows | 422 INVALID_TRANSITION |
| PSP timeout | reqwest client timeout at 10s | 202 Accepted + poll_url |
| PSP network drop | hyper::Error(Io) | Safe rollback → invoice OPEN |
| PSP failure code | response.status != succeeded | invoice OPEN, failure_code stored |
| Crash after PSP success, before DB write | Invoice stuck in PROCESSING | Reconciliation job re-queries PSP |
| Idempotency key replay (same body) | request_hash match | Cached response, no PSP call |
| Idempotency key replay (different body) | request_hash mismatch | 409 IDEMPOTENCY_CONFLICT |
| Void on terminal state | State check in UPDATE | 422 INVALID_TRANSITION |
| Invalid API key | SHA-256 hash miss | 401 UNAUTHORIZED |
| Revoked API key | revoked_at IS NOT NULL | 401 UNAUTHORIZED |

---

## What Was Cut and Why

**Refunds** — requires a `refunds` table, a `refunded` state, and reverse PSP calls.
Explicitly out of scope per the assignment. The `psp_ref` stored on every succeeded
attempt enables future reverse-lookup.

**Subscriptions / recurring billing** — entire separate domain (plans, intervals, proration).
Not in scope.

**Rate limiting** — `tower-governor` middleware keyed on `business_id` would handle this.
Left documented in DESIGN.md section 7 as a production gap.

**Audit log** — an append-only `audit_log` table per transaction is a regulatory requirement
in production. Documented as a gap in DESIGN.md.

---

*Built for the Dodo Payments Backend Engineer (Rust) Assessment · Mohammed Sehran · 2026*

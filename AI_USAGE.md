# AI Usage Disclosure

This document is required as part of the assessment submission.
It covers exactly how I used AI tools, what I relied on them for,
and where I made the actual engineering decisions myself.

---

## Tools Used

- **Claude (Anthropic)** — used through claude.ai for design review and Rust boilerplate
- **Cursor** — used as my primary editor with inline completions during implementation

---

## What I Used AI For

### Boilerplate and repetitive patterns

Rust has a lot of ceremony — `#[derive(...)]`, sqlx `query_as!` macro signatures,
Axum extractor patterns. I used Cursor completions to move faster through these.
There is nothing interesting in that code and writing it by hand would not have
demonstrated anything meaningful.

### First draft of the DB schema

I described the domain to Claude and asked for a starting schema.
It gave me something reasonable. I rewrote most of it after thinking through
the actual requirements.

### Sanity checking my concurrency reasoning

Before writing the payment handler, I described my approach to Claude
and asked if I was missing any edge cases. It flagged the
"crash after PSP success, before DB write" scenario — which I had
already planned for, but the conversation made me write it down
more clearly in DESIGN.md.

---

## Where I Made the Actual Decisions

These are the three places where I disagreed with AI suggestions
and went with my own approach. This is where the real engineering is.

### 1. Concurrency — status-conditional UPDATE instead of SELECT FOR UPDATE

Claude's first suggestion was `SELECT FOR UPDATE` inside a transaction
that wraps the entire PSP call. I rejected this.

The problem: `SELECT FOR UPDATE` locks the invoice row before the PSP call
and holds that lock until the transaction commits — which could be 10 seconds
with our client timeout. Every other concurrent payment attempt on that invoice
queues behind that lock. That is a bad design for a payment endpoint.

My approach: a single `UPDATE WHERE state = 'open' RETURNING id`.
This is atomic. The lock window is one SQL statement, not one PSP call.
The losing concurrent request gets 0 rows back and fails immediately with 422.
No queue. No 10-second locks.

I knew this pattern from understanding how PostgreSQL row-level locking works.
Claude suggested the naive approach. I used the better one.

### 2. PSP timeout — 202 Accepted instead of marking the payment failed

Claude's initial suggestion on the timeout case was to catch the timeout error,
mark the payment attempt as `failed`, and roll the invoice back to `open`.

I disagreed with this. A timeout means we do not know if the PSP processed
the charge or not. The network timed out — the charge may have gone through.
Rolling the invoice back to `open` and returning a failure response could result
in a duplicate charge if the customer retries immediately.

The correct behaviour is: leave the invoice in `PROCESSING`, return `202 Accepted`
with a poll URL, cache the idempotency response as 202 so retries get the same
pending response rather than triggering a new PSP call, and let a reconciliation job
resolve the outcome after re-querying the PSP.

This is a real payments problem. Getting it wrong costs money.

### 3. SHA-256 for API key hashing instead of bcrypt

Claude suggested bcrypt for storing API key hashes. I changed it to SHA-256.

bcrypt exists to slow down brute-force attacks on low-entropy secrets like
human-chosen passwords. Our API keys are 32 random bytes — 256 bits of entropy.
There is no brute-force attack possible at that entropy level regardless of
hashing algorithm. Using bcrypt here would add 200–300ms of CPU on every
single authenticated request with no security benefit whatsoever.

SHA-256 is the right tool. The reasoning matters more than following a default.

---

## What AI Got Wrong That I Fixed

**Money types** — the initial schema draft from Claude used `DECIMAL(10, 2)`
for monetary amounts and `f64` in the corresponding Rust structs.
I caught this during review and changed everything to `BIGINT` (cents) with `i64`.

Floating-point money is a known correctness bug. `0.1 + 0.2` does not equal `0.3`
in IEEE 754. That is not acceptable in a billing system.

**Webhook delivery on the request path** — Claude's first webhook implementation
delivered webhooks synchronously inside the API handler, meaning the HTTP response
would wait for the webhook POST to complete (or fail).

I moved this to a persistent outbox pattern — insert a row into `webhook_deliveries`,
return the API response immediately, let a background Tokio task handle delivery.
This means the API latency is unaffected by webhook endpoint reliability,
and deliveries survive process restarts since they are in PostgreSQL, not memory.

---

## Summary

I used AI to move faster on things that are mechanical.
I did not use it to figure out how the system should actually work.
The concurrency model, the timeout handling, the idempotency design,
the webhook outbox pattern.


# AI Usage Disclosure

## Tools Used

- **Claude (Anthropic)** — Used to generate the initial Rust scaffolding (handler signatures, sqlx query patterns, Axum middleware boilerplate), review the DB schema draft, and stress-test the failure-mode reasoning before writing DESIGN.md.
- **GitHub Copilot** — Used for inline autocomplete on repetitive sqlx query blocks and error-mapping patterns.

## Three Decisions I Made Myself (Against or Independent of AI Suggestions)

**1. Status-conditional UPDATE over SELECT FOR UPDATE for concurrency**

Claude initially suggested `SELECT ... FOR UPDATE` inside a transaction that wraps the PSP call. I rejected this because the PSP call (up to 10s for tok_timeout with our client timeout) would hold the row lock for the entire duration, serialising all concurrent payment attempts behind a queue. I chose a status-conditional `UPDATE WHERE state = 'open'` instead — it's a single atomic statement, the lock window is milliseconds, and losing concurrent requests fail fast with a clear error. This is a better fit for a payments API where concurrent attempts on the same invoice are uncommon but should not degrade under load.

**2. Flat webhook retry with DB as queue instead of in-memory queue**

Claude suggested an in-memory `tokio::mpsc` channel for webhook delivery. I rejected this because in-memory queues don't survive process restarts, and webhook delivery guarantees are a correctness property, not a performance optimisation. Using `webhook_deliveries` as a persistent outbox means the worker can restart at any point without losing events. The polling overhead (every 5s, SKIP LOCKED) is negligible at this scale.

**3. SHA-256 over bcrypt for API key hashing**

Claude suggested bcrypt for API key storage "for security." I overrode this because bcrypt's cost factor matters for low-entropy secrets like passwords; our API keys are 256-bit random tokens, where SHA-256 is already computationally pre-image resistant without stretching. Using bcrypt would add 100–300ms of CPU to every authenticated request with no security benefit. The tradeoff only makes sense for human-memorable secrets.

## One Thing AI Got Wrong

Claude generated the sqlx `query_as!` macro calls with `DECIMAL(10,2)` column types for monetary amounts in an early schema draft, and the corresponding Rust struct fields as `f64`. I caught this during schema review and corrected all monetary columns to `BIGINT` (cents) with `i64` Rust types. Floating-point money is a classic correctness bug — `0.1 + 0.2 != 0.3` in IEEE 754. The assignment explicitly checks for this.
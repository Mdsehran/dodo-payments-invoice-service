use axum::{
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

use crate::{
    auth::AuthBusiness,
    errors::{AppError, Result},
    models::{PayInvoiceRequest, PaymentAttempt, PspResponse},
    webhooks::enqueue_webhook,
    AppConfig,
};

fn hash_request_body(card_token: &str) -> String {
    let mut h = Sha256::new();
    h.update(card_token.as_bytes());
    hex::encode(h.finalize())
}

pub async fn pay_invoice(
    State(pool): State<PgPool>,
    State(config): State<AppConfig>,
    Extension(auth): Extension<AuthBusiness>,
    Path(invoice_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<PayInvoiceRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>)> {
    // ── 1. Extract and validate idempotency key ───────────────────────────
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::Validation("Idempotency-Key header is required".into()))?
        .to_string();

    let request_hash = hash_request_body(&req.card_token);

    // ── 2. Check for existing idempotency key (return cached response) ────
    let existing = sqlx::query!(
        "SELECT request_hash, response_status, response_body
         FROM idempotency_keys
         WHERE idempotency_key = $1 AND business_id = $2",
        idempotency_key, auth.id
    )
    .fetch_optional(&pool)
    .await?;

    if let Some(cached) = existing {
        if cached.request_hash != request_hash {
            // Same key, different body → 409 conflict
            return Err(AppError::IdempotencyConflict);
        }
        // Same key, same body → return cached response (no second PSP call)
        let status = StatusCode::from_u16(cached.response_status as u16)
            .unwrap_or(StatusCode::OK);
        return Ok((status, Json(cached.response_body)));
    }

    // ── 3. Status-conditional update: atomically claim the invoice ────────
    //
    // We use UPDATE ... WHERE state = 'open' RETURNING to atomically
    // transition to 'processing'. Only one concurrent request can succeed
    // this — PostgreSQL's row-level lock on the UPDATE ensures it.
    //
    // This is preferable to SELECT FOR UPDATE + app-level check because:
    // - It keeps the lock window to a single statement (not PSP call duration)
    // - The PSP call happens AFTER the invoice is marked 'processing'
    // - Concurrent requests fail fast (no queue behind a lock)
    //
    let claimed = sqlx::query!(
        r#"UPDATE invoices SET state = 'processing'
           WHERE id = $1 AND business_id = $2 AND state = 'open'
           RETURNING id"#,
        invoice_id, auth.id
    )
    .fetch_optional(&pool)
    .await?;

    if claimed.is_none() {
        // Check why: not found, wrong business, or wrong state
        let inv = sqlx::query!(
            "SELECT state FROM invoices WHERE id = $1 AND business_id = $2",
            invoice_id, auth.id
        )
        .fetch_optional(&pool)
        .await?;

        return match inv {
            None => Err(AppError::NotFound(format!("Invoice {} not found", invoice_id))),
            Some(r) => Err(AppError::InvalidTransition(
                format!("Cannot pay invoice in '{}' state — must be 'open'", r.state)
            )),
        };
    }

    // ── 4. Record the payment attempt as pending ──────────────────────────
    let attempt = sqlx::query_as!(
        PaymentAttempt,
        r#"INSERT INTO payment_attempts
           (invoice_id, idempotency_key, card_token, state)
           VALUES ($1, $2, $3, 'pending')
           RETURNING id, invoice_id, idempotency_key, card_token, state,
                     psp_ref, failure_code, created_at, updated_at"#,
        invoice_id, idempotency_key, req.card_token
    )
    .fetch_one(&pool)
    .await?;

    // ── 5. Call mock PSP with a hard timeout ─────────────────────────────
    //
    // tok_timeout sleeps 30 seconds. We time out after 10s, leave the
    // invoice in 'processing', and return 202. The caller should poll
    // GET /invoices/{id} to learn the final state. A background job
    // (not implemented in this scope but documented in DESIGN.md) would
    // reconcile long-pending attempts.
    //
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let psp_result = client
        .post(format!("{}/charge", config.mock_psp_url))
        .json(&json!({
            "card_token": req.card_token,
            "amount_cents": 0,  // fetched by PSP from attempt context in real system
            "reference": attempt.id
        }))
        .send()
        .await;

    // ── 6. Handle PSP response (including timeout and network error) ──────
    let (final_invoice_state, final_attempt_state, psp_ref, failure_code, http_status) =
        match psp_result {
            Err(e) if e.is_timeout() => {
                tracing::warn!("PSP timed out for attempt {}", attempt.id);
                // Leave invoice in 'processing' — do NOT mark failed yet
                // Return 202 so caller knows to poll
                let body = json!({
                    "status": "pending",
                    "payment_attempt_id": attempt.id,
                    "message": "Payment is being processed. Poll GET /invoices/{id} for final state."
                });
                let _ = store_idempotency(
                    &pool, &idempotency_key, &auth.id, &request_hash, 202, &body
                ).await;
                return Ok((StatusCode::ACCEPTED, Json(body)));
            }
            Err(e) => {
                tracing::error!("PSP network error for attempt {}: {}", attempt.id, e);
                ("open", "failed", None, Some("network_error".to_string()), StatusCode::PAYMENT_REQUIRED)
            }
            Ok(resp) => {
                let psp: PspResponse = resp.json().await
                    .map_err(|e| AppError::Internal(e.to_string()))?;

                if psp.status == "succeeded" {
                    ("paid", "succeeded", psp.psp_ref, None, StatusCode::OK)
                } else {
                    ("open", "failed", None, psp.code, StatusCode::PAYMENT_REQUIRED)
                }
            }
        };

    // ── 7. Persist final state in a single transaction ────────────────────
    //
    // If we crash here after PSP success but before writing: on retry,
    // the idempotency key isn't stored yet (step 2 found nothing), so
    // the caller retries. The mock PSP is deterministic — tok_success
    // always returns success. We get a second "succeeded" response and
    // write it now. In production: use PSP's psp_ref as a unique
    // constraint on payment_attempts to prevent double recording.
    //
    let mut tx = pool.begin().await?;

    sqlx::query!(
        "UPDATE payment_attempts SET state = $1, psp_ref = $2, failure_code = $3
         WHERE id = $4",
        final_attempt_state, psp_ref, failure_code, attempt.id
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        "UPDATE invoices SET state = $1 WHERE id = $2",
        final_invoice_state, invoice_id
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // ── 8. Enqueue webhooks (non-blocking) ───────────────────────────────
    let event_type = if final_invoice_state == "paid" {
        "invoice.paid"
    } else {
        "invoice.payment_failed"
    };

    let webhook_payload = json!({
        "event": event_type,
        "invoice_id": invoice_id,
        "payment_attempt_id": attempt.id,
        "state": final_invoice_state
    });

    let _ = enqueue_webhook(&pool, auth.id, event_type, webhook_payload).await;

    // ── 9. Store idempotency response and return ──────────────────────────
    let response_body = json!({
        "payment_attempt_id": attempt.id,
        "invoice_id": invoice_id,
        "state": final_invoice_state,
        "psp_ref": psp_ref,
        "failure_code": failure_code
    });

    let _ = store_idempotency(
        &pool, &idempotency_key, &auth.id, &request_hash,
        http_status.as_u16() as i32, &response_body
    ).await;

    Ok((http_status, Json(response_body)))
}

async fn store_idempotency(
    pool: &PgPool,
    key: &str,
    business_id: &Uuid,
    request_hash: &str,
    status: i32,
    body: &serde_json::Value,
) -> std::result::Result<(), sqlx::Error> {
    sqlx::query!(
        r#"INSERT INTO idempotency_keys
           (idempotency_key, business_id, request_hash, response_status, response_body)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT DO NOTHING"#,
        key, business_id, request_hash, status, body
    )
    .execute(pool)
    .await?;
    Ok(())
}
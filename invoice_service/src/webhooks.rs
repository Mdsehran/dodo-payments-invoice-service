use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// Sign a webhook payload: HMAC-SHA256 over "timestamp.body"
/// Receiver verifies by recomputing the signature and comparing.
/// Timestamp prevents replay attacks (reject if > 5 min old).
pub fn sign_payload(secret: &str, timestamp: i64, body: &str) -> String {
    let message = format!("{}.{}", timestamp, body);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC key error");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Enqueue a webhook delivery for all endpoints of a business
pub async fn enqueue_webhook(
    pool: &PgPool,
    business_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), sqlx::Error> {
    let endpoints = sqlx::query!(
        "SELECT id FROM webhook_endpoints WHERE business_id = $1",
        business_id
    )
    .fetch_all(pool)
    .await?;

    for ep in endpoints {
        sqlx::query!(
            r#"INSERT INTO webhook_deliveries (endpoint_id, event_type, payload, state)
               VALUES ($1, $2, $3, 'pending')"#,
            ep.id, event_type, payload
        )
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Background worker: polls webhook_deliveries and delivers with retry
/// Retry schedule: 10s, 30s, 2min, 10min, 1hr (5 attempts max)
/// After exhaustion → state = 'dead' (business can query for reconciliation)
pub async fn webhook_worker(pool: PgPool, _signing_secret: String) {
    let retry_delays: &[i64] = &[10, 30, 120, 600, 3600]; // seconds
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let pending = sqlx::query!(
            r#"SELECT wd.id, wd.endpoint_id, wd.event_type, wd.payload,
                      wd.attempts, we.url, we.secret
               FROM webhook_deliveries wd
               JOIN webhook_endpoints we ON we.id = wd.endpoint_id
               WHERE wd.state = 'pending' AND wd.next_attempt_at <= NOW()
               LIMIT 10
               FOR UPDATE SKIP LOCKED"#
        )
        .fetch_all(&pool)
        .await;

        let deliveries = match pending {
            Ok(d) => d,
            Err(e) => { tracing::error!("webhook poll error: {e}"); continue; }
        };

        for d in deliveries {
            let timestamp = chrono::Utc::now().timestamp();
            let body = serde_json::json!({
                "event": d.event_type,
                "data": d.payload,
                "timestamp": timestamp
            })
            .to_string();

            let signature = sign_payload(&d.secret, timestamp, &body);
            let attempt_num = d.attempts as usize;

            let result = client
                .post(&d.url)
                .header("Content-Type", "application/json")
                .header("X-Dodo-Signature", format!("t={},v1={}", timestamp, signature))
                .header("X-Dodo-Event", &d.event_type)
                .body(body)
                .send()
                .await;

            let success = matches!(&result, Ok(r) if r.status().is_success());

            if success {
                let _ = sqlx::query!(
                    "UPDATE webhook_deliveries SET state = 'delivered', attempts = $1 WHERE id = $2",
                    attempt_num as i32 + 1, d.id
                )
                .execute(&pool)
                .await;
            } else {
                let error_msg = match result {
                    Err(e) => e.to_string(),
                    Ok(r) => format!("HTTP {}", r.status()),
                };

                let next_attempt = attempt_num + 1;
                if next_attempt >= retry_delays.len() {
                    // Exhausted retries → dead letter
                    let _ = sqlx::query!(
                        "UPDATE webhook_deliveries SET state = 'dead', attempts = $1, last_error = $2 WHERE id = $3",
                        next_attempt as i32, error_msg, d.id
                    )
                    .execute(&pool)
                    .await;
                    tracing::warn!("Webhook {} permanently failed after {} attempts", d.id, next_attempt);
                } else {
                    let delay = retry_delays[next_attempt];
                    let _ = sqlx::query!(
                        r#"UPDATE webhook_deliveries
                           SET attempts = $1, last_error = $2,
                               next_attempt_at = NOW() + ($3 || ' seconds')::interval
                           WHERE id = $4"#,
                        next_attempt as i32, error_msg, delay.to_string(), d.id
                    )
                    .execute(&pool)
                    .await;
                }
            }
        }
    }
}
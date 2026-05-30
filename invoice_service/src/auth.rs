use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::AppError;

/// Authenticated business extracted from API key
#[derive(Debug, Clone)]
pub struct AuthBusiness {
    pub id:   Uuid,
    pub name: String,
}

/// Generate a new raw API key (call once, show to user, never store raw)
pub fn generate_api_key() -> String {
    use rand::Rng;
    let random_bytes: Vec<u8> = (0..32).map(|_| rand::thread_rng().gen()).collect();
    let encoded = hex::encode(&random_bytes);
    format!("sk_live_{}", encoded)
}

/// Hash a raw API key for storage
pub fn hash_api_key(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

/// Axum middleware: extract Bearer token, validate against DB, inject AuthBusiness into extensions
pub async fn auth_middleware(
    State(pool): State<PgPool>,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let raw_key = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(AppError::Unauthorized)?
        .to_string();

    let key_hash = hash_api_key(&raw_key);

    let row = sqlx::query!(
        r#"
        SELECT ak.business_id, b.name
        FROM api_keys ak
        JOIN businesses b ON b.id = ak.business_id
        WHERE ak.key_hash = $1 AND ak.revoked_at IS NULL
        "#,
        key_hash
    )
    .fetch_optional(&pool)
    .await?
    .ok_or(AppError::Unauthorized)?;

    req.extensions_mut().insert(AuthBusiness {
        id:   row.business_id,
        name: row.name,
    });

    Ok(next.run(req).await)
}
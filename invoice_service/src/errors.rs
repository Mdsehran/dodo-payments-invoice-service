use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("unauthorized")]
    Unauthorized,

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("idempotency key conflict")]
    IdempotencyConflict,

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            AppError::Unauthorized => (
                StatusCode::UNAUTHORIZED, "UNAUTHORIZED", self.to_string(),
            ),
            AppError::NotFound(m) => (
                StatusCode::NOT_FOUND, "NOT_FOUND", m.clone(),
            ),
            AppError::Conflict(m) => (
                StatusCode::CONFLICT, "CONFLICT", m.clone(),
            ),
            AppError::InvalidTransition(m) => (
                StatusCode::UNPROCESSABLE_ENTITY, "INVALID_TRANSITION", m.clone(),
            ),
            AppError::Validation(m) => (
                StatusCode::BAD_REQUEST, "VALIDATION_ERROR", m.clone(),
            ),
            AppError::IdempotencyConflict => (
                StatusCode::CONFLICT, "IDEMPOTENCY_CONFLICT",
                "Idempotency key reused with different request body".to_string(),
            ),
            AppError::Database(e) => {
                tracing::error!("db error: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR", "Internal error".to_string())
            },
            AppError::Internal(m) => (
                StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", m.clone(),
            ),
        };

        (status, Json(json!({
            "error": {
                "code": code,
                "message": message
            }
        }))).into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
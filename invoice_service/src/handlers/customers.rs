use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::AuthBusiness,
    errors::{AppError, Result},
    models::{CreateCustomerRequest, Customer},
};

pub async fn create_customer(
    State(pool): State<PgPool>,
    Extension(auth): Extension<AuthBusiness>,
    Json(req): Json<CreateCustomerRequest>,
) -> Result<(StatusCode, Json<Customer>)> {
    if req.name.trim().is_empty() {
        return Err(AppError::Validation("name is required".into()));
    }
    if !req.email.contains('@') {
        return Err(AppError::Validation("invalid email".into()));
    }

    let customer = sqlx::query_as!(
        Customer,
        r#"
        INSERT INTO customers (business_id, name, email)
        VALUES ($1, $2, $3)
        RETURNING id, business_id, name, email, created_at
        "#,
        auth.id,
        req.name.trim(),
        req.email.trim().to_lowercase()
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db) = e {
            if db.constraint() == Some("customers_business_id_email_key") {
                return AppError::Conflict("Customer with this email already exists".into());
            }
        }
        AppError::Database(e)
    })?;

    Ok((StatusCode::CREATED, Json(customer)))
}

pub async fn get_customer(
    State(pool): State<PgPool>,
    Extension(auth): Extension<AuthBusiness>,
    Path(id): Path<Uuid>,
) -> Result<Json<Customer>> {
    let customer = sqlx::query_as!(
        Customer,
        "SELECT id, business_id, name, email, created_at FROM customers
         WHERE id = $1 AND business_id = $2",
        id, auth.id
    )
    .fetch_optional(&pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Customer {} not found", id)))?;

    Ok(Json(customer))
}

pub async fn list_customers(
    State(pool): State<PgPool>,
    Extension(auth): Extension<AuthBusiness>,
) -> Result<Json<Vec<Customer>>> {
    let customers = sqlx::query_as!(
        Customer,
        "SELECT id, business_id, name, email, created_at FROM customers
         WHERE business_id = $1 ORDER BY created_at DESC",
        auth.id
    )
    .fetch_all(&pool)
    .await?;

    Ok(Json(customers))
}
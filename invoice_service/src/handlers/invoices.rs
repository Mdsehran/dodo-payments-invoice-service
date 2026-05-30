use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    auth::AuthBusiness,
    errors::{AppError, Result},
    models::{CreateInvoiceRequest, Invoice, InvoiceLineItem, InvoiceResponse},
};

#[derive(Deserialize)]
pub struct ListInvoicesQuery {
    pub state: Option<String>,
}

pub async fn create_invoice(
    State(pool): State<PgPool>,
    Extension(auth): Extension<AuthBusiness>,
    Json(req): Json<CreateInvoiceRequest>,
) -> Result<(StatusCode, Json<InvoiceResponse>)> {
    if req.line_items.is_empty() {
        return Err(AppError::Validation("at least one line item is required".into()));
    }
    for item in &req.line_items {
        if item.quantity <= 0 {
            return Err(AppError::Validation("quantity must be positive".into()));
        }
        if item.unit_amount_cents <= 0 {
            return Err(AppError::Validation("unit_amount_cents must be positive".into()));
        }
    }

    // Server-side total computation — never trust client-supplied total
    let total_cents: i64 = req.line_items.iter()
        .map(|i| i.quantity as i64 * i.unit_amount_cents)
        .sum();

    // Verify customer belongs to this business
    let customer_exists = sqlx::query_scalar!(
        "SELECT EXISTS(SELECT 1 FROM customers WHERE id = $1 AND business_id = $2)",
        req.customer_id, auth.id
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(false);

    if !customer_exists {
        return Err(AppError::NotFound("Customer not found".into()));
    }

    let mut tx = pool.begin().await?;

    let invoice = sqlx::query_as!(
        Invoice,
        r#"
        INSERT INTO invoices (business_id, customer_id, state, total_cents, due_date)
        VALUES ($1, $2, 'draft', $3, $4)
        RETURNING id, business_id, customer_id, state, total_cents, due_date, created_at, updated_at
        "#,
        auth.id, req.customer_id, total_cents, req.due_date
    )
    .fetch_one(&mut *tx)
    .await?;

    let mut line_items = Vec::new();
    for item in &req.line_items {
        let li = sqlx::query_as!(
            InvoiceLineItem,
            r#"
            INSERT INTO invoice_line_items (invoice_id, description, quantity, unit_amount_cents)
            VALUES ($1, $2, $3, $4)
            RETURNING id, invoice_id, description, quantity, unit_amount_cents
            "#,
            invoice.id, item.description, item.quantity, item.unit_amount_cents
        )
        .fetch_one(&mut *tx)
        .await?;
        line_items.push(li);
    }

    tx.commit().await?;

    Ok((StatusCode::CREATED, Json(InvoiceResponse { invoice, line_items })))
}

pub async fn get_invoice(
    State(pool): State<PgPool>,
    Extension(auth): Extension<AuthBusiness>,
    Path(id): Path<Uuid>,
) -> Result<Json<InvoiceResponse>> {
    let invoice = sqlx::query_as!(
        Invoice,
        r#"SELECT id, business_id, customer_id, state, total_cents, due_date, created_at, updated_at
           FROM invoices WHERE id = $1 AND business_id = $2"#,
        id, auth.id
    )
    .fetch_optional(&pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Invoice {} not found", id)))?;

    let line_items = sqlx::query_as!(
        InvoiceLineItem,
        "SELECT id, invoice_id, description, quantity, unit_amount_cents
         FROM invoice_line_items WHERE invoice_id = $1",
        id
    )
    .fetch_all(&pool)
    .await?;

    Ok(Json(InvoiceResponse { invoice, line_items }))
}

pub async fn list_invoices(
    State(pool): State<PgPool>,
    Extension(auth): Extension<AuthBusiness>,
    Query(q): Query<ListInvoicesQuery>,
) -> Result<Json<Vec<Invoice>>> {
    let invoices = if let Some(state) = q.state {
        sqlx::query_as!(
            Invoice,
            r#"SELECT id, business_id, customer_id, state, total_cents, due_date, created_at, updated_at
               FROM invoices WHERE business_id = $1 AND state = $2 ORDER BY created_at DESC"#,
            auth.id, state
        )
        .fetch_all(&pool)
        .await?
    } else {
        sqlx::query_as!(
            Invoice,
            r#"SELECT id, business_id, customer_id, state, total_cents, due_date, created_at, updated_at
               FROM invoices WHERE business_id = $1 ORDER BY created_at DESC"#,
            auth.id
        )
        .fetch_all(&pool)
        .await?
    };

    Ok(Json(invoices))
}

pub async fn void_invoice(
    State(pool): State<PgPool>,
    Extension(auth): Extension<AuthBusiness>,
    Path(id): Path<Uuid>,
) -> Result<Json<Invoice>> {
    let invoice = sqlx::query_as!(
        Invoice,
        r#"UPDATE invoices SET state = 'void'
           WHERE id = $1 AND business_id = $2 AND state IN ('draft', 'open')
           RETURNING id, business_id, customer_id, state, total_cents, due_date, created_at, updated_at"#,
        id, auth.id
    )
    .fetch_optional(&pool)
    .await?
    .ok_or_else(|| AppError::InvalidTransition(
        "Invoice can only be voided from draft or open state".into()
    ))?;

    Ok(Json(invoice))
}
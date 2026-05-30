use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── Invoice state machine ─────────────────────────────────────────────────
// Valid transitions:
//   draft       → open, void
//   open        → processing, void, uncollectible
//   processing  → open (psp fail), paid (psp success), void
//   paid        → (terminal)
//   void        → (terminal)
//   uncollectible → (terminal)

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text")]
pub enum InvoiceState {
    #[serde(rename = "draft")]         Draft,
    #[serde(rename = "open")]          Open,
    #[serde(rename = "processing")]    Processing,
    #[serde(rename = "paid")]          Paid,
    #[serde(rename = "void")]          Void,
    #[serde(rename = "uncollectible")] Uncollectible,
}

impl InvoiceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            InvoiceState::Draft         => "draft",
            InvoiceState::Open          => "open",
            InvoiceState::Processing    => "processing",
            InvoiceState::Paid          => "paid",
            InvoiceState::Void          => "void",
            InvoiceState::Uncollectible => "uncollectible",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, InvoiceState::Paid | InvoiceState::Void | InvoiceState::Uncollectible)
    }
}

impl std::fmt::Display for InvoiceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ── DB row types ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Business {
    pub id:         Uuid,
    pub name:       String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Customer {
    pub id:          Uuid,
    pub business_id: Uuid,
    pub name:        String,
    pub email:       String,
    pub created_at:  DateTime<Utc>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Invoice {
    pub id:           Uuid,
    pub business_id:  Uuid,
    pub customer_id:  Uuid,
    pub state:        String,
    pub total_cents:  i64,
    pub due_date:     NaiveDate,
    pub created_at:   DateTime<Utc>,
    pub updated_at:   DateTime<Utc>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct InvoiceLineItem {
    pub id:                Uuid,
    pub invoice_id:        Uuid,
    pub description:       String,
    pub quantity:          i32,
    pub unit_amount_cents: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct PaymentAttempt {
    pub id:              Uuid,
    pub invoice_id:      Uuid,
    pub idempotency_key: String,
    pub card_token:      String,
    pub state:           String,
    pub psp_ref:         Option<String>,
    pub failure_code:    Option<String>,
    pub created_at:      DateTime<Utc>,
    pub updated_at:      DateTime<Utc>,
}

// ── Request/response shapes ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateCustomerRequest {
    pub name:  String,
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateInvoiceRequest {
    pub customer_id: Uuid,
    pub due_date:    NaiveDate,
    pub line_items:  Vec<LineItemInput>,
}

#[derive(Debug, Deserialize)]
pub struct LineItemInput {
    pub description:       String,
    pub quantity:          i32,
    pub unit_amount_cents: i64,
}

#[derive(Debug, Deserialize)]
pub struct PayInvoiceRequest {
    pub card_token: String,
}

#[derive(Debug, Serialize)]
pub struct InvoiceResponse {
    pub invoice:    Invoice,
    pub line_items: Vec<InvoiceLineItem>,
}

// ── PSP response shape (from mock PSP) ───────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PspResponse {
    pub status:  String,
    pub psp_ref: Option<String>,
    pub code:    Option<String>,
}
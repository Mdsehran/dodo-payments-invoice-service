mod auth;
mod errors;
mod handlers;
mod models;
mod webhooks;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;

pub mod handlers {
    pub mod customers;
    pub mod invoices;
    pub mod payments;
}

#[derive(Clone)]
pub struct AppConfig {
    pub mock_psp_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "invoice_service=debug,tower_http=info".into()),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")?;
    let mock_psp_url = std::env::var("MOCK_PSP_URL")
        .unwrap_or_else(|_| "http://localhost:3001".into());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".into())
        .parse()?;
    let webhook_secret = std::env::var("WEBHOOK_SIGNING_SECRET")
        .unwrap_or_else(|_| "dev_secret".into());

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;

    // Run migrations
    sqlx::migrate!("../migrations").run(&pool).await?;

    tracing::info!("Migrations applied");

    let config = AppConfig { mock_psp_url };

    // Spawn webhook delivery worker
    let worker_pool = pool.clone();
    let worker_secret = webhook_secret.clone();
    tokio::spawn(async move {
        webhooks::webhook_worker(worker_pool, worker_secret).await;
    });

    let protected = Router::new()
        .route("/customers",        get(handlers::customers::list_customers)
                                   .post(handlers::customers::create_customer))
        .route("/customers/:id",    get(handlers::customers::get_customer))
        .route("/invoices",         get(handlers::invoices::list_invoices)
                                   .post(handlers::invoices::create_invoice))
        .route("/invoices/:id",     get(handlers::invoices::get_invoice))
        .route("/invoices/:id/void", post(handlers::invoices::void_invoice))
        .route("/invoices/:id/pay", post(handlers::payments::pay_invoice))
        .layer(middleware::from_fn_with_state(pool.clone(), auth::auth_middleware))
        .with_state(pool.clone())
        .with_state(config);

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .merge(protected)
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Invoice service listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
use axum::{routing::post, Router, Json};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[derive(Deserialize)]
struct ChargeRequest {
    card_token: String,
    #[allow(dead_code)]
    amount_cents: Option<i64>,
    #[allow(dead_code)]
    reference: Option<Uuid>,
}

#[derive(Serialize)]
struct ChargeResponse {
    status:  &'static str,
    psp_ref: Option<String>,
    code:    Option<&'static str>,
}

async fn charge(Json(req): Json<ChargeRequest>) -> Json<ChargeResponse> {
    match req.card_token.as_str() {
        "tok_success" => {
            sleep(Duration::from_millis(100)).await;
            Json(ChargeResponse {
                status: "succeeded",
                psp_ref: Some(Uuid::new_v4().to_string()),
                code: None,
            })
        }
        "tok_insufficient_funds" => {
            sleep(Duration::from_millis(100)).await;
            Json(ChargeResponse {
                status: "failed",
                psp_ref: None,
                code: Some("insufficient_funds"),
            })
        }
        "tok_card_declined" => {
            sleep(Duration::from_millis(100)).await;
            Json(ChargeResponse {
                status: "failed",
                psp_ref: None,
                code: Some("card_declined"),
            })
        }
        "tok_timeout" => {
            // Sleep 30s — invoice service must handle this with a client-side timeout
            sleep(Duration::from_secs(30)).await;
            Json(ChargeResponse {
                status: "succeeded",
                psp_ref: Some(Uuid::new_v4().to_string()),
                code: None,
            })
        }
        "tok_network_error" => {
            // Axum will drop the connection — simulates a 500 / TCP drop
            panic!("simulated network error")
        }
        _ => {
            sleep(Duration::from_millis(100)).await;
            Json(ChargeResponse {
                status: "failed",
                psp_ref: None,
                code: Some("unknown_token"),
            })
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3001".into())
        .parse()?;

    let app = Router::new().route("/charge", post(charge));
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    println!("Mock PSP listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
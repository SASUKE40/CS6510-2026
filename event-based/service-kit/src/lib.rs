//! Chassis shared by every deployable: JSON errors, blocking database
//! dispatch, SQLite connection handling, the Kafka event bus, CLI parsing,
//! health checks, and graceful shutdown. It holds no schema, no SQL beyond
//! connection pragmas, and no business rules; each service owns those.
pub mod bus;
mod error;
pub mod sqlite;

pub use bus::{Bus, Delivery, Outgoing, Topic};
pub use error::{Error, Kind, Result};

use axum::{
    Json, Router,
    extract::rejection::{JsonRejection, QueryRejection},
    http::StatusCode,
    routing::get,
    serve::{Listener, ListenerExt},
};
use chrono::{SecondsFormat, Utc};
use serde_json::json;
use std::collections::HashMap;

pub type ApiResult<T> = Result<T>;

pub fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The contract expresses money as decimal currency; services store cents.
pub fn money(cents: i64) -> f64 {
    cents as f64 / 100.0
}

/// Database calls are synchronous; keep them off the async I/O workers.
pub async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(Error::internal)?
}

pub fn body<T>(input: std::result::Result<Json<T>, JsonRejection>) -> Result<T> {
    input
        .map(|Json(v)| v)
        .map_err(|e| Error::bad(e.body_text()))
}

pub fn query<T>(input: std::result::Result<axum::extract::Query<T>, QueryRejection>) -> Result<T> {
    input
        .map(|axum::extract::Query(v)| v)
        .map_err(|e| Error::bad(e.body_text()))
}

/// Parses the contract's `tx-<n>` transaction id.
pub fn parse_transaction_id(id: &str) -> Result<i64> {
    id.strip_prefix("tx-")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .ok_or_else(|| Error::missing("No such transaction"))
}

pub fn not_found() -> Error {
    Error::missing("No such route")
}

/// Adds `/health` and the contract's JSON 404/405 responses to a service.
pub fn finish<S: Clone + Send + Sync + 'static>(
    name: &'static str,
    router: Router<S>,
) -> Router<S> {
    router
        .route(
            "/health",
            get(move || async move { Json(json!({"service": name, "status": "UP"})) }),
        )
        .fallback(|| async { not_found() })
        .method_not_allowed_fallback(|| async {
            (
                StatusCode::METHOD_NOT_ALLOWED,
                Json(json!({"error": "METHOD_NOT_ALLOWED", "message": "Method not allowed"})),
            )
        })
}

/// `--key=value` command-line options with `--help`.
pub struct Args(HashMap<String, String>);
impl Args {
    pub fn parse(
        usage: &str,
        keys: &[&str],
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let mut values = HashMap::new();
        for arg in std::env::args().skip(1) {
            if arg == "--help" {
                println!("{usage}");
                std::process::exit(0);
            }
            let (key, value) = arg
                .split_once('=')
                .ok_or_else(|| format!("Expected --key=value; usage: {usage}"))?;
            if !keys.contains(&key) {
                return Err(format!("Unknown option {key}; usage: {usage}").into());
            }
            values.insert(key.to_owned(), value.to_owned());
        }
        Ok(Self(values))
    }
    pub fn string(&self, key: &str, default: &str) -> String {
        self.0
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_owned())
    }
    pub fn number<T: std::str::FromStr>(
        &self,
        key: &str,
        default: T,
    ) -> std::result::Result<T, Box<dyn std::error::Error>> {
        self.0.get(key).map_or(Ok(default), |v| {
            v.parse()
                .map_err(|_| format!("{key} expects a number, got {v}").into())
        })
    }
}

/// Serves `router` on loopback until SIGINT/SIGTERM, then drains requests.
pub async fn serve(name: &str, port: u16, router: Router) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await?
        .tap_io(|tcp| {
            let _ = tcp.set_nodelay(true);
        });
    println!("{name} listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
}

pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

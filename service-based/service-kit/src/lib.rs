//! HTTP chassis shared by every deployable: JSON errors, blocking database
//! dispatch, CLI parsing, health checks, and graceful shutdown. Contains no
//! business rules and no SQL.
use axum::{
    Json, Router,
    extract::rejection::{JsonRejection, QueryRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    serve::{Listener, ListenerExt},
};
use checkout_data::{Error, Kind};
use serde_json::json;
use std::collections::HashMap;

/// Wraps the data-access error so this crate can render it as HTTP.
pub struct ApiError(pub Error);
pub type ApiResult<T> = std::result::Result<T, ApiError>;
impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0.kind {
            Kind::Invalid => StatusCode::BAD_REQUEST,
            Kind::NotFound => StatusCode::NOT_FOUND,
            Kind::Conflict => StatusCode::CONFLICT,
            Kind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(json!({"error": self.0.code, "message": self.0.message})),
        )
            .into_response()
    }
}

/// Database calls are synchronous; keep them off the async I/O workers.
pub async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> checkout_data::Result<T> + Send + 'static,
) -> ApiResult<T> {
    Ok(tokio::task::spawn_blocking(f)
        .await
        .map_err(Error::internal)??)
}

pub fn body<T>(input: Result<Json<T>, JsonRejection>) -> ApiResult<T> {
    input
        .map(|Json(v)| v)
        .map_err(|e| Error::bad(e.body_text()).into())
}

pub fn query<T>(input: Result<axum::extract::Query<T>, QueryRejection>) -> ApiResult<T> {
    input
        .map(|axum::extract::Query(v)| v)
        .map_err(|e| Error::bad(e.body_text()).into())
}

pub fn not_found() -> ApiError {
    Error::missing("No such route").into()
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
    pub fn parse(usage: &str, keys: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
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
    ) -> Result<T, Box<dyn std::error::Error>> {
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

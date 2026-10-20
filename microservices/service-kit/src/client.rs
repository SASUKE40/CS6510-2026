//! JSON-over-HTTP client for calls between services. Transport failures and
//! timeouts become `SERVICE_UNAVAILABLE`; error replies keep the remote code.
use crate::error::{Error, Kind, Result};
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde_json::Value;
use std::time::Duration;

const MAX_BODY: usize = 64 << 20;

#[derive(Clone)]
pub struct HttpClient {
    client: Client<HttpConnector, Body>,
    timeout: Duration,
}

pub struct Reply {
    pub status: StatusCode,
    pub body: Value,
}

impl Reply {
    /// The body of a 2xx reply, or the remote error.
    pub fn ok(self) -> Result<Value> {
        if self.status.is_success() {
            Ok(self.body)
        } else {
            Err(self.error())
        }
    }

    pub fn error(&self) -> Error {
        let kind = match self.status.as_u16() {
            400 => Kind::Invalid,
            404 => Kind::NotFound,
            409 => Kind::Conflict,
            502..=504 => Kind::Unavailable,
            _ => Kind::Internal,
        };
        let text = |key: &str| self.body[key].as_str().unwrap_or_default().to_owned();
        Error::new(kind, text("error"), text("message"))
    }
}

impl HttpClient {
    pub fn new(timeout: Duration) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .build(connector);
        Self { client, timeout }
    }

    pub async fn get(&self, url: &str) -> Result<Reply> {
        self.send(Method::GET, url, Body::empty()).await
    }

    pub async fn post(&self, url: &str, body: &Value) -> Result<Reply> {
        self.send(Method::POST, url, Body::from(body.to_string()))
            .await
    }

    async fn send(&self, method: Method, url: &str, body: Body) -> Result<Reply> {
        let request = Request::builder()
            .method(method)
            .uri(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .map_err(Error::internal)?;
        let call = async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|e| e.to_string())?;
            let status = response.status();
            let bytes = to_bytes(Body::new(response.into_body()), MAX_BODY)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(Reply {
                status,
                body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            })
        };
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(error)) => Err(Error::unavailable(format!("{url}: {error}"))),
            Err(_) => Err(Error::unavailable(format!("{url}: timed out"))),
        }
    }
}

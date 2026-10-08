//! API gateway: the one URL the unchanged load client knows. It routes each
//! request by path to the domain service that owns it and relays the response
//! unchanged. It holds no business logic and no database connection; services
//! never call each other through it.
use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, Uri, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub const NAME: &str = "checkout-gateway";

/// Base URLs (scheme, host, port) of each domain service.
#[derive(Clone, Debug)]
pub struct Upstreams {
    pub inventory: String,
    pub basket: String,
    pub payment: String,
    pub analytics: String,
}

impl Upstreams {
    /// Path-based routing table for the API contract.
    pub fn route(&self, path: &str) -> Option<(&'static str, &str)> {
        let segments: Vec<_> = path.trim_start_matches('/').split('/').collect();
        match segments.as_slice() {
            ["items"] | ["inventory", ..] => Some(("inventory", &self.inventory)),
            ["transactions", _, "complete"] => Some(("payment", &self.payment)),
            ["transactions"] | ["transactions", _] | ["transactions", _, "items"] => {
                Some(("basket", &self.basket))
            }
            ["analytics", ..] => Some(("analytics", &self.analytics)),
            _ => None,
        }
    }
}

struct Gateway {
    upstreams: Upstreams,
    client: Client<HttpConnector, Body>,
}

pub fn router(upstreams: Upstreams) -> Router {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .build(connector);
    // Anything that is not the gateway's own page is forwarded; the owning
    // service answers 404/405 for its own paths.
    service_kit::finish(
        NAME,
        Router::new()
            .route("/docs", get(docs))
            .route("/docs/", get(docs))
            .route("/openapi.yaml", get(openapi)),
    )
    .fallback(forward)
    .with_state(Arc::new(Gateway { upstreams, client }))
}

async fn forward(State(gateway): State<Arc<Gateway>>, mut request: Request) -> Response {
    let Some((service, base)) = gateway.upstreams.route(request.uri().path()) else {
        return service_kit::not_found().into_response();
    };
    let path = request.uri().path_and_query().map_or("/", |p| p.as_str());
    let Ok(uri) = format!("{base}{path}").parse::<Uri>() else {
        return service_kit::not_found().into_response();
    };
    *request.uri_mut() = uri;
    match gateway.client.request(request).await {
        Ok(response) => response.map(Body::new),
        Err(error) => {
            eprintln!("{service} service unavailable: {error}");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "SERVICE_UNAVAILABLE",
                    "message": format!("The {service} service is unavailable")})),
            )
                .into_response()
        }
    }
}

async fn docs() -> Html<&'static str> {
    Html(include_str!("docs.html"))
}

async fn openapi() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/yaml; charset=utf-8")],
        include_str!("../../../spec/self-checkout-openapi.yaml"),
    )
}
